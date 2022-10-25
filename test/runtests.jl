using Hermes
using Dates
using CSV
using ProfileView

test_hermes(from, to) = begin
    iter = Hermes.ReplaySingleFeed{Hermes.IncrementalBookL2}(
        Hermes.FTX, 
        Hermes.BTCPERP, 
        from, 
        to
    )

    s::Float64 = 0.0

    for data in iter
        s += data.amount
    end

    println(s)
end

test_hermes(Date(2022, 10, 14), Date(2022, 10, 16))

