module leverage_efficiency_codes

## %%
using CSV
using DataFrames
using DataFramesMeta
using Dates
using Chain
using Impute: Interpolate, impute, locf
using YAML
using Optim

datafolder = "../../data/1-source"

function Fed() 
    f1= CSV.File(datafolder*"/FED_1927-12-30_2020-05-14.csv", header=4, footerskip=1 ) |> DataFrame
    fedDateConversion(x) = Date(string(x), DateFormat("YYYYmmdd"))
    dailyToYearRateConv(x) = 100*((1+x/100)^252.75-1)
    @chain f1 (
        transform!(:Column1 => (x -> fedDateConversion.(x)) => :date);
        transform!(:RF => (x-> dailyToYearRateConv.(x)) => :level);
        filter!(:date => <(Date(1954,6,30)), _);
        select!([:date, :level])
    )
    f1=vcat(f1,FEDM())
    f1.return = (1.0 .+ (f1.level ./ 100)) .^ (1/360)
    f1
end

function FEDM() 
    f2=CSV.File(datafolder*"/FED_1954-07-01_2020-03-01-FRED.csv" ) |> DataFrame
    fedMDateConversion(x) = Date(string(x), DateFormat("YYYY-mm-dd"))
    @chain f2 (
        transform!(:DATE => (x -> fedMDateConversion.(x)) => :date);
        transform!(:FEDFUNDS  => :level);
        filter!(:date => >(Date(1954,7,1)), _);
        select!([:date, :level])
    )
    f2
end

ret(x) = [1., (x[2:end]./x[1:end-1])...]
returnOnLevel!(df) = @chain df  ( transform!(:level => ret => :return);)
standardColumn!(df) = @chain df (select!([:date, :level, :return]);)

function BTC() 
    f1=CSV.File(datafolder*"/BPI_2010-07-18_2018-04-06_Coindesk.csv", footerskip=2 ) |> DataFrame
    CoindeskDateConversion(x) = Date(split(string(x))[1], DateFormat("YYYY-mm-dd"))
    @chain f1 (
        transform!(:Date => (x -> CoindeskDateConversion.(x)) => :date);
        filter!(:date => <(Date(2014, 9, 17)), _);
        transform!(Symbol("Close Price")  => :level);
        returnOnLevel!();
        standardColumn!();
    )
    f2=CSV.File(datafolder*"/BTC-USD_2014-09-17_2020-05-01_YF.csv" ) |> DataFrame
    BTCDateConversion(x) = Date(string(x), DateFormat("YYYY-mm-dd"))

    @chain f2 (
        transform!(:Date => (x -> BTCDateConversion.(x)) => :date);
        filter!(:date => >(Date(2014, 9, 16)), _);
        transform!(:Close  => :level);
        returnOnLevel!();
        standardColumn!();
    )
    vcat(f1,f2)
end


function SP500() 
    f1=CSV.File(datafolder*"/SP500_1927-12-31_2020-05-14.csv") |> DataFrame
        SPDateConversion(x) = Date(string(x), DateFormat("YYYY-mm-dd"))

    @chain f1 (
        transform!(:Date => (x -> SPDateConversion.(x)) => :date);
        transform!(:Close  => :level);
        returnOnLevel!();
        standardColumn!();
    )
    f1
end

function Madoff() 
    f1=CSV.File(datafolder*"/MAD_1990-01-01_2005-05-01_DU.csv",header=0) |> DataFrame
    function MadoffDateConversion(x) 
        d = Date(string(x), DateFormat("dd/mm/yy"))
        d += year(d) < 20 ? Year(2000) : Year(1900)
        d
    end


    @chain f1 (
        transform!(:Column1 => (x -> MadoffDateConversion.(x)) => :date);
        transform!(:Column2  => (x -> x./100.0.+1.0) => :level);
        returnOnLevel!();
        standardColumn!();    )
    f1
end

function interpolateOnDate(df)
   everyDay=DataFrame(date=range(df[1, :date], df[end, :date], step=Day(1)))
   @chain everyDay (
        transform!(:date => (x-> Date.(x)) => :date);
        leftjoin(df, on=:date);
        sort(:date);
        locf();
   )
end


params = YAML.load_file("../../model_parameters.yaml")
fed = dropmissing(interpolateOnDate(Fed()))
btc = BTC(), params["BTC"]
sp500 = interpolateOnDate(SP500()), params["SP500"]
mad = Madoff(), params["MAD"]


## %%%%%%%%%%%%%%%%%%%%%%%%%%%

function optimumLeverage(a1, d2)
    d1, p1 = a1

    df=@chain d1 (
        innerjoin(d2, on=:date,  makeunique=true);
        transform(  :level      => identity     => :risky, 
                    :level_1    => identity     => :riskless,
                    :return     => identity     => :riskyRet,
                    :return_1   => identity     => :dailyRet );
    )
    #get leverage limits that would avoid ruin (lowesr number that gives positive result, highest neg result)
    leverageRatio = df.dailyRet ./ (df.dailyRet .- df.riskyRet)
    maxLeverage = minimum(filter(>(0), leverageRatio ))
    minLeverage = (maximum(filter(<(0), leverageRatio )))

    @show minLeverage, maxLeverage

    model1(leverage) = 1.0 .+ leverage .* (df.riskyRet .- 1.0) .+ (1.0 .- leverage) .* (df.dailyRet .- 1.0)
    wealth(m) = l -> -log(cumprod(m(l))[end])  #max wealth is to min -log
    res = optimize(wealth(model1), minLeverage, maxLeverage)
    @show  "model1 : ", Optim.minimum(res), Optim.minimizer(res)

    

end

xx=optimumLeverage(btc, fed)

## %%%%%%%%%%%%%%%%%%%%%%%%%%%


end # module
