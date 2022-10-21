module Hermes

########################### using statements ############################
using HTTP
using JSON
using CSV
using Accessors
using Dates
using Kibisis
import Base: *


########################### internal constants ############################
const tardis_api_key = ENV["TARDIS_API_KEY"]
const base_api_endpoint = "https://api.tardis.dev/v1"

########################## recommended external constants ##############################
const cache_root = ENV["HERMES_CACHE_ROOT"]
const tardis_lru_cache_size_gb = parse(Int64, ENV["HERMES_CACHE_SIZE_GB"])


######################### tardis sides #####################
@enum Side::UInt8 bid ask unknown

Base.convert(::Type{T}, s::Side) where T <: Number = begin
    if s === bid
        T(1)
    elseif s === ask
        T(-1)
    else
        T(0)
    end
end

(*)(a1::T, a2::Side) where T <: Number = a1 * convert(T, a2)
(*)(a1::Side, a2::T) where T <: Number = convert(T, a1) * a2


######################### tardis exchanges #########################
@enum Exchange::UInt8 FTX Coinbase Gemini Kraken BitMEX Deribit


######################## tardis symbols #########################
@enum Contract::UInt8 BTCPERP ETHPERP


######################### tardis csv data types ######################
struct IncrementalBookL2
    exchange::Exchange
    contract::Contract
    timestamp::Int64
    local_timestamp::Int64
    is_snapshsot::Bool
    side::Side
    price::Float64
    amount::Int32
end

struct Trade
    exchange::Exchange
    contract::Contract
    timestamp::Int64
    local_timestamp::Int64
    id::String
    side::Side
    price::Float64
    amount::Int32
end

struct Liquidation
    exchange::Exchange
    contract::Contract
    timestamp::Int64
    local_timestamp::Int64
    id::String
    side::Side
    price::Float64
    amount::Int32
end

const TardisDataType = Union{IncrementalBookL2, Trade, Liquidation}


########################### types for loading data ############################
@enum ResourceLocation::UInt8 cached remote

struct TardisCache
    dir::String
    size_gb::Int16
    lru_cache::Union{Nothing, FKJS}
    # Want to write a LRU implementation to use here. The first time the loader is used to 
    # fetch a resource, its cache should be instantiated from a file on disk. After the last time
    # the loader is used to fetch a resource the cache will be flushed back to the file
end

struct TardisLoader
    cache::TardisCache
    exchange::Exchange
    data_type::Type{T} where T <: TardisDataType
    year::Int16
    month::Int16
    day::Int16
    contract::Contract
end


##################### resource path creation ##################
create_resource_path(loader::TardisLoader, ::Val{remote}) = begin
    path_components = [
        base_api_endpoint,
        create_resource_path(Val(loader.exchange)),
        create_resource_path(loader.data_type), 
        create_resource_path(loader.year, loader.month, loader.day, Val(remote)), 
        create_resource_path(Val(loader.contract)) * ".csv.gz"
    ]

    join(path_components, '/')
end

create_resource_path(loader::TardisLoader, ::Val{cached}) = begin
    path_components = [
        create_resource_path(Val(loader.exchange)), 
        create_resource_path(Val(loader.contract)), 
        create_resource_path(loader.data_type), 
        create_resource_path(loader.year, loader.month, loader.day, Val(cached)) * ".csv.gz"
    ]

    loader.cache.dir * "/" * join(path_components, '-')
end

create_resource_path(::Type{IncrementalBookL2}) = "incremental_book_L2"
create_resource_path(::Type{Trade}) = "trades"
create_resource_path(::Type{Liquidation}) = "liquidations"

create_resource_path(date_component::Integer) = date_component < 10 ? "0" * string(x) : string(x)
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
    update_cache(loader, resource, Val(resource_location))
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

update_cache(loader::TardisLoader, resource::Vector{UInt8}, ::Val{remote}) = begin
    cache_path = create_resource_path(loader, Val(cached))
    open(cache_path, 'w') do io
        write(io, resource)
    end
    update_cache(loader, resource, Val(cached))
end

update_cache(loader::TardisLoader, _::Vector{UInt8}, ::Val{cached}) = begin
    
end



##################### making request to tardis and saving data #####################
request_data(params::TardisDownloadQueryParameters) = begin
    resp = HTTP.request(
        :GET, 
        base_api_endpoint * '/' * create_path(params), 
        headers=["Authorization" => "Bearer $tardis_api_key"]
    )
    resp.status == 200 || error("invalid status: $resp.status")
    resp
end



download(downloader::TardisCSVDownloader) = begin
    file_name = create_file_name(downloader)
    resp = request_data(downloader.params)
    mkpath(dirname(file_name))
    open(file_name, "w") do io
        write(io, resp.body)
    end
end


####################### higher level replay ##################
struct ReplaySingleFeed
    exchange::Exchange
    data_type::Type{T} where T <: TardisDataType
    contract::Contract
    from::Date
    to::Date
end

struct ReplaySingleFeedState
    current_date::Date
    current_file::CSV.File
    current_line::Int64
end

Base.iterate(iter::ReplaySingleFeed) = begin
    iter.to - iter.from >= Day(1) || return nothing
    params = TardisDownloadQueryParameters(
        iter.exchange, 
        iter.data_type, 
        Dates.year(iter.from), 
        Dates.month(iter.from), 
        Dates.day(iter.from), 
        iter.contract
    )

    current_date = iter.from
    current_file = CSV.File(request_data(params).body)
    current_line = 1

    if current_line > length(current_file)
        iterate(@set iter.from += Day(1))
    else
        current_file[current_line], ReplaySingleFeedState(current_date, current_file, current_line + 1)
    end
end

Base.iterate(iter::ReplaySingleFeed, state::ReplaySingleFeedState) = begin
    if iter.to - state.current_date == Day(1) && state.current_line > length(state.current_file)
        return nothing
    end

    if state.current_line > length(state.current_file)
        next_date = state.current_date + Day(1)
        params = TardisDownloadQueryParameters(
            iter.exchange, 
            iter.data_type, 
            Dates.year(next_date), 
            Dates.month(next_date), 
            Dates.day(next_date), 
            iter.contract
        )
        next_file = CSV.File(request_data(params).body)
        next_line = 1
        iterate(iter, ReplaySingleFeedState(next_date, next_file, next_line))
    else
        state.current_file[state.current_line], @set state.current_line += 1
    end
end


end


