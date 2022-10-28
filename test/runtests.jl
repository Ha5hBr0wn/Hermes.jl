using Hermes: ReplaySingleFeed, IncrementalBookL2, Trade, FTX, BTCPERP
using Dates
using CSV
using ProfileView
using MergedIterators: MergedIterator, SingleIterator
using BenchmarkTools

test_hermes(from, to) = begin
    iter = ReplaySingleFeed{IncrementalBookL2}(
        FTX, 
        BTCPERP, 
        from, 
        to
    )

    iter2 = ReplaySingleFeed{Trade}(
        FTX, 
        BTCPERP, 
        from,
        to
    )

    merged_iter = MergedIterator(iter, iter2)

    s::Float64 = 0.0

    for data in merged_iter
        s += data.amount
    end

    # for data in iter
    #     s += data.amount
    # end

    # for data in iter2
    #     s += data.amount
    # end

    println(s)
end

@time test_hermes(Date(2022, 10, 14), Date(2022, 10, 16))

