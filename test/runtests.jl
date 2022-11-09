using Hermes: ReplaySingleFeed, IncrementalBookL2, Trade, FTX, BTCPERP, TardisDataType
using Dates
using MergedIterators: MergedIterators, @merge_and_process, IteratorProcess

mutable struct SumAmounts <: IteratorProcess
    s::Float64
end

(s::SumAmounts)(x::TardisDataType) = begin
    s.s += x.amount
end

test_hermes(from, to) = begin
    iter1 = ReplaySingleFeed{IncrementalBookL2}(
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
    
    sum_process = SumAmounts(0.0)

    @merge_and_process sum_process iter1 iter2

    println(sum_process.s)

end

@time test_hermes(Date(2022, 10, 14), Date(2022, 10, 16))

