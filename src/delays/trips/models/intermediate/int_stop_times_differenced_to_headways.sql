with time_partitions as (

    select *
    from unnest(
        generate_timestamp_array(
            timestamp '2026-03-01 00:00:00',
            timestamp '2026-03-07 23:55:00',
            interval 5 minute
        )
    ) as partition_time

),

station_route_combinations as (

    select distinct
        stop_id,
        route_id
    from {{ ref('int_stop_times_augmented') }}

),

time_station_route_grid as (

    select * from time_partitions
    cross join station_route_combinations

),

realized_stop_times as (

    select
        trip_id,
        route_id,
        stop_id,
        timestamp_add(
            sunday_timestamp,
            interval (
                cast(split(arrival_time, ':')[0] as int64) * 3600 +
                cast(split(arrival_time, ':')[1] as int64) * 60 +
                cast(split(arrival_time, ':')[2] as int64)
            ) second
        ) as realized_arrival_time
    from {{ ref('int_stop_times_augmented') }}
    cross join unnest(
        [timestamp '2026-03-01 00:00:00', timestamp '2026-03-08 00:00:00']
    ) as sunday_timestamp
    where
        day_of_week = 'Sunday'
    
    union all

    select
        trip_id,
        route_id,
        stop_id,
        timestamp_add(
            weekday_timestamp,
            interval (
                cast(split(arrival_time, ':')[0] as int64) * 3600 +
                cast(split(arrival_time, ':')[1] as int64) * 60 +
                cast(split(arrival_time, ':')[2] as int64)
            ) second
        ) as realized_arrival_time
    from {{ ref('int_stop_times_augmented') }}
    cross join unnest(
        generate_timestamp_array(
            timestamp '2026-03-02 00:00:00',
            timestamp '2026-03-06 00:00:00',
            interval 1 day
        )
    ) as weekday_timestamp
    where
        day_of_week = 'Weekday'
    
    union all

    select
        trip_id,
        route_id,
        stop_id,
        timestamp_add(
            timestamp '2026-03-07 00:00:00',
            interval (
                cast(split(arrival_time, ':')[0] as int64) * 3600 +
                cast(split(arrival_time, ':')[1] as int64) * 60 +
                cast(split(arrival_time, ':')[2] as int64)
            ) second
        ) as realized_arrival_time
    from {{ ref ('int_stop_times_augmented') }}
    where
        day_of_week = 'Saturday'
),

next_trains as (

    select
        time_station_route_grid.partition_time,
        time_station_route_grid.stop_id,
        time_station_route_grid.route_id,
        timestamp_diff(realized_stop_times.realized_arrival_time, time_station_route_grid.partition_time, second) as time_till_arrival,
        row_number() over (
            partition by time_station_route_grid.partition_time, time_station_route_grid.stop_id, time_station_route_grid.route_id
            order by time_till_arrival
        ) as train_order
    from time_station_route_grid
    left join realized_stop_times on
        time_station_route_grid.partition_time <= realized_stop_times.realized_arrival_time and
        timestamp_add(time_station_route_grid.partition_time, interval 5 hour) > realized_stop_times.realized_arrival_time and
        time_station_route_grid.stop_id = realized_stop_times.stop_id and
        time_station_route_grid.route_id = realized_stop_times.route_id
    qualify
        train_order < 5

),

consecutive_headways as (

    select
        partition_time,
        stop_id,
        route_id,
        train_order,
        time_till_arrival - lag(time_till_arrival, 1) over (
            partition by partition_time, stop_id, route_id order by time_till_arrival
        ) as time_till_next_train
    from next_trains

),

consecutive_headways_pivoted as (

    select *
    from consecutive_headways pivot (
        max(time_till_next_train) as headway for train_order in (2, 3, 4)
    )

),

avg_headways as (

    select
        partition_time,
        stop_id,
        route_id,
        headway_2 / 2 + headway_3 / 3 + headway_4 / 6 as avg_headway,
        headway_2 / 4 + headway_3 / 6 + headway_4 / 12 as avg_first_headway
    from consecutive_headways_pivoted

)

select * from avg_headways
