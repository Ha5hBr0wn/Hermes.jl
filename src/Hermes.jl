module Hermes

########################### using statements ############################
using HTTP
using CSV
using Dates
using Kibisis
using MergedIterators
using Serialization
import Base: *


########################### internal constants ############################
const tardis_api_key = ENV["TARDIS_API_KEY"]
const base_api_endpoint = "https://api.tardis.dev/v1"

########################## recommended external constants ##############################
const cache_root = ENV["HERMES_CACHE_ROOT"]
const cache_size_gb = parse(Float64, ENV["HERMES_CACHE_SIZE_GB"])


######################### tardis sides #####################
@enum Side::UInt8 bid ask unknown

@inline Base.convert(::Type{T}, s::Side) where T <: Number = begin
    if s === bid
        T(1)
    elseif s === ask
        T(-1)
    else
        T(0)
    end
end

@inline Base.convert(::Type{Side}, s::AbstractString) = begin
    if s == "bid"
        bid
    elseif s == "ask"
        ask
    else
        unknown
    end
end

(*)(a1::T, a2::Side) where T <: Number = a1 * convert(T, a2)
(*)(a1::Side, a2::T) where T <: Number = convert(T, a1) * a2


######################### tardis exchanges #########################
@enum Exchange::UInt8 FTX Deribit

@inline Base.convert(::Type{Exchange}, s::AbstractString) = begin
    if s == "ftx"
        FTX
    elseif s == "deribit"
        Deribit
    else
        error("unsupported exchange $s")
    end
end


######################## tardis symbols #########################
@enum Contract::UInt8 BTCPERP ETHPERP

@inline Base.convert(::Type{Contract}, s::AbstractString) = begin
    if s == "BTC-PERP"
        BTCPERP
    elseif s == "ETH-PERP"
        ETHPERP
    else
        error("unsupported contract $s")
    end
end


######################### tardis csv data types ######################
struct IncrementalBookL2
    exchange::Exchange
    contract::Contract
    timestamp::Int64
    local_timestamp::Int64
    is_snapshsot::Bool
    side::Side
    price::Float64
    amount::Float64
end

struct Trade
    exchange::Exchange
    contract::Contract
    timestamp::Int64
    local_timestamp::Int64
    id::String
    side::Side
    price::Float64
    amount::Float64
end

struct Liquidation
    exchange::Exchange
    contract::Contract
    timestamp::Int64
    local_timestamp::Int64
    id::String
    side::Side
    price::Float64
    amount::Float64
end

const TardisDataType = Union{IncrementalBookL2, Trade, Liquidation}

@inline Base.convert(::Type{IncrementalBookL2}, row::CSV.Row) = begin
    IncrementalBookL2(
        row.exchange::String, 
        row.symbol::String, 
        row.timestamp::Int64, 
        row.local_timestamp::Int64, 
        row.is_snapshot::Bool, 
        row.side::String, 
        row.price::Float64, 
        row.amount::Float64
    )
end

@inline type_list(::Type{IncrementalBookL2}) = [String, String, Int64, Int64, Bool, String, Float64, Float64]

@inline Base.convert(::Type{Trade}, row::CSV.Row) = begin
    Trade(
        row.exchange::String, 
        row.symbol::String, 
        row.timestamp::Int64, 
        row.local_timestamp::Int64, 
        row.id::String, 
        row.side::String, 
        row.price::Float64, 
        row.amount::Float64
    )
end

@inline type_list(::Type{Trade}) = [String, String, Int64, Int64, String, String, Float64, Float64]

@inline Base.convert(::Type{Liquidation}, row::CSV.Row) = begin
    Liquidation(
        row.exchange::String, 
        row.symbol::String, 
        row.timestamp::Int64, 
        row.local_timestamp::Int64, 
        row.id::String, 
        row.side::String, 
        row.price::Float64, 
        row.amount::Float64
    )
end

@inline type_list(::Type{Liquidation}) = [String, String, Int64, Int64, String, String, Float64, Float64]


########################### types for loading data ############################
@enum ResourceLocation::UInt8 cached remote

struct TardisCacheItem
    file_name::String
end

struct TardisCache
    dir::String
    size_gb::Int16
    lru::Kibisis.LRUSet{TardisCacheItem}
end

struct TardisLoader{T <: TardisDataType}
    cache::TardisCache
    exchange::Exchange
    year::Int16
    month::Int8
    day::Int8
    contract::Contract

    TardisLoader{T}(exchange, year, month, day, contract) where T <: TardisDataType = begin
        cache_file_name = cache_root * "/cache.jls"
        if !isfile(cache_file_name)
            lru = Kibisis.LRUSet{TardisCacheItem}(cache_size_gb)
            Serialization.serialize(cache_file_name, lru)
        end
        
        new(
            TardisCache(cache_root, cache_size_gb, Serialization.deserialize(cache_file_name)), 
            exchange, 
            year, 
            month,
            day, 
            contract
        )
    end

    TardisLoader{T}(loader::TardisLoader{T}, year, month, day) where T <: TardisDataType = begin
        new(
            loader.cache, 
            loader.exchange, 
            year, 
            month, 
            day, 
            loader.contract
        )
    end
end


##################### cache utilities #######################
Kibisis.item_size(x::TardisCacheItem, ::Vector{UInt8}) = begin
    (x.file_name |> filesize) / 1e9
end

Kibisis.on_pop(item::TardisCacheItem, ::Vector{UInt8}) = begin
    item.file_name |> rm
end

Kibisis.on_new_push(item::TardisCacheItem, body::Vector{UInt8}) = begin
    open(item.file_name, "w") do io
        write(io, body)
    end
end

flush(cache::TardisCache) = begin
    cache_file_name = cache.dir * "/cache.jls"
    Serialization.serialize(cache_file_name, cache.lru)
end

flush(loader::TardisLoader) = begin
    cache_file_name = loader.cache.dir * "/cache.jls"
    Serialization.serialize(cache_file_name, loader.cache.lru)
end


##################### resource path creation ##################
create_resource_path(loader::TardisLoader{T}, ::Val{remote}) where T <: TardisDataType = begin
    path_components = [
        base_api_endpoint,
        create_resource_path(Val(loader.exchange)),
        create_resource_path(T), 
        create_resource_path(loader.year, loader.month, loader.day, Val(remote)), 
        create_resource_path(Val(loader.contract)) * ".csv.gz"
    ]

    join(path_components, '/')
end

create_resource_path(loader::TardisLoader{T}, ::Val{cached}) where T <: TardisDataType = begin
    path_components = [
        create_resource_path(Val(loader.exchange)), 
        create_resource_path(Val(loader.contract)), 
        create_resource_path(T), 
        create_resource_path(loader.year, loader.month, loader.day, Val(cached)) * ".csv.gz"
    ]

    loader.cache.dir * "/" * join(path_components, '-')
end

create_resource_path(::Type{IncrementalBookL2}) = "incremental_book_L2"
create_resource_path(::Type{Trade}) = "trades"
create_resource_path(::Type{Liquidation}) = "liquidations"

create_resource_path(date_component::Integer) = date_component < 10 ? "0" * string(date_component) : string(date_component)
create_resource_path(year::Integer, month::Integer, day::Integer, sep::String) = begin
    path_components = [
        create_resource_path(year), 
        create_resource_path(month), 
        create_resource_path(day)
    ]
    join(path_components, sep)
end
create_resource_path(year::Integer, month::Integer, day::Integer, ::Val{remote}) = create_resource_path(year, month, day, "/")
create_resource_path(year::Integer, month::Integer, day::Integer, ::Val{cached}) = create_resource_path(year, month, day, "-")

create_resource_path(::Val{BTCPERP}) = "BTC-PERP"
create_resource_path(::Val{ETHPERP}) = "ETH-PERP"
create_resource_path(::Val{FTX}) = "ftx"


####################### fetching resource and updating cache #######################
determine_resource_location(loader::TardisLoader) = begin
    possible_cached_path = create_resource_path(loader, Val(cached))
    isfile(possible_cached_path) ? cached : remote
end

fetch_resource(loader::TardisLoader) = begin
    resource_location = determine_resource_location(loader)
    resource = fetch_resource(loader, Val(resource_location))
    cache_item = TardisCacheItem(create_resource_path(loader, Val(cached)))
    Kibisis.pushpop!(loader.cache.lru, cache_item, resource)
    flush(loader) # Warning if we crash during pushpop! we could end up with a corrupt cache
    resource
end

fetch_resource(loader::TardisLoader, location_value_type::Val{remote}) = begin
    resource_path = create_resource_path(loader, location_value_type)
    resp = HTTP.request(
        :GET, 
        resource_path, 
        headers=["Authorization" => "Bearer $tardis_api_key"]
    )
    resp.status == 200 || error("invalid status: $resp.status")
    resp.body
end

fetch_resource(loader::TardisLoader, location_value_type::Val{cached}) = begin
    resource_path = create_resource_path(loader, location_value_type)
    open(resource_path, "r") do io
        unsafe_wrap(Vector{UInt8}, read(io, String))
    end
end


####################### higher level replay ##################
struct ReplaySingleFeed{T <: TardisDataType}
    exchange::Exchange
    contract::Contract
    from::Date
    to::Date
end

mutable struct ReplaySingleFeedState{T <: TardisDataType}
    const current_loader::TardisLoader{T}
    const current_date::Date
    const current_file::CSV.File
    current_line::Int64
end

loader_date(loader::TardisLoader) = Date(loader.year, loader.month, loader.day)
add_one_to_from_date(x::ReplaySingleFeed{T}) where T = ReplaySingleFeed{T}(x.exchange, x.contract, x.from + Day(1), x.to)

Base.iterate(iter::ReplaySingleFeed{T}) where T <: TardisDataType = begin
    iter.to - iter.from >= Day(1) || return nothing
    loader = TardisLoader{T}(
        iter.exchange, 
        Dates.year(iter.from), 
        Dates.month(iter.from), 
        Dates.day(iter.from), 
        iter.contract
    )

    current_loader = loader
    current_date = loader_date(loader)
    current_file = CSV.File(fetch_resource(loader); types=type_list(T))
    current_line = 1

    if current_line > length(current_file)
        iterate(add_one_to_from_date(iter))
    else
        convert(T, current_file[current_line]), ReplaySingleFeedState(current_loader, current_date, current_file, current_line + 1)
    end
end

Base.iterate(iter::ReplaySingleFeed{T}, state::ReplaySingleFeedState{T}) where T <: TardisDataType = begin
    if state.current_line > length(state.current_file) && iter.to - state.current_date == Day(1)
        return nothing
    end

    if state.current_line > length(state.current_file)
        next_date = state.current_date + Day(1)
        next_loader = TardisLoader{T}(
            state.current_loader,
            Dates.year(next_date), 
            Dates.month(next_date), 
            Dates.day(next_date)
        )
        next_file = CSV.File(fetch_resource(next_loader); types=type_list(T))
        next_line = 1
        iterate(iter, ReplaySingleFeedState(next_loader, next_date, next_file, next_line))
    else
        convert(T, state.current_file[state.current_line]), (state.current_line += 1; state)
    end
end

##################### helpers for merging iterators ########################
MergedIterators.SingleIterator(iter::ReplaySingleFeed{T}) where T = begin
    MergedIterators.SingleIterator{
        ReplaySingleFeed{T}, 
        T, 
        ReplaySingleFeedState{T}
    }(iter)
end

struct LocalTimestampOrdering <: Base.Order.Ordering end

MergedIterators.MergedIterator(iters::Vararg{ReplaySingleFeed}) = begin
    MergedIterators.MergedIterator(
        LocalTimestampOrdering(), map(MergedIterators.SingleIterator, iters)...
    )
end

MergedIterators.@custom_lt Base.Order.lt(::LocalTimestampOrdering, a::TardisDataType, b::TardisDataType) = begin
    a.local_timestamp < b.local_timestamp
end

end