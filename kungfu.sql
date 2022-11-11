-- assuming the telemetry.csv file in this repo has been imported
-- build a common table expression [cte] that returns it's records, 
--   classifies each row's speed based on thresholds [configurable],
--   and determines the last_timestamp for each row.
-- we put placeholders in for lat/lng fields that are present
--  in normal telemetry but were skipped for this example
with cte_telemetry as (
  select
    *,
    null as lat,
    null as lng,
    case
      when speed >= 150 then "TOOFAST"
      when speed >= 110 then "FAST"
      when speed == 0 then "STOPPED"
      when speed <= 40 then "SLOW"
      else "OK"
    end as state,
    lag(timestamp) over (
      partition by vehicleid
      order by
        timestamp
    ) as last_timestamp
  from
    telemetry
  order by
    vehicleid,
    timestamp
),
-- then, determine each rows previous state provided the previous row was within 3 minutes [configurable]
cte_telemetry_in_context as (
  select
    *,
    if (
      (
        unix_timestamp(timestamp) - unix_timestamp(last_timestamp)
      ) < 180,
      -- 3 minutes
      lag(state) over (
        partition by vehicleid
        order by
          timestamp asc
      ),
      null
    ) as prev_state
  from
    cte_telemetry
),
-- then, determine if a state change has occurred 
cte_telemetry_state_changed as (
  select
    VehicleID,
    lat,
    lng,
    Timestamp,
    Speed,
    state,
    prev_state,
    if(
      (prev_state is null)
      or (state != prev_state),
      1,
      0
    ) as state_changed
  from
    cte_telemetry_in_context
),
-- and assign a state change identifier
cte_telemetry_with_state_change_id as (
  select
    *,
   sum(state_changed) over (
      partition by vehicleid
      order by
        timestamp asc
    ) as state_change_id
  from
    cte_telemetry_state_changed
),
-- use the vehicle id and state change identifier to work out some metrics and identify the row numbers
--  also create a state_group_id so that this can be reused to visualize all the lat/lngs for a particular vehicle's state at a specific time
cte_telemetry_with_row_num as (
  select
    vehicleid || '_' || unix_timestamp(timestamp) || '_' || state_change_id as state_group_id,
    *,
    row_number() over (
      partition by vehicleid,
      state_change_id
      order by
        timestamp desc
    ) as rn,
    round(avg(speed) over (
      partition by vehicleid,
      state_change_id
      order by
        vehicleid,
        timestamp
    ), 2) as avg_speed_in_state,
    max(speed) over (
      partition by vehicleid,
      state_change_id
      order by
        vehicleid,
        timestamp
    ) as max_speed_in_state,
    min(timestamp) over (
      partition by vehicleid,
      state_change_id
      order by
        vehicleid,
        timestamp
    ) as state_timestamp_start
  from
    cte_telemetry_with_state_change_id
  order by
    vehicleid,
    timestamp
),
-- calculate the how many seconds the vehicle was in a state and only focus on the row before a state change
cte_telemetry_state_durations as (
  select
    state_group_id,
    state_timestamp_start as state_start,
    timestamp as state_end,
    vehicleid as vehicle_id,
    state,
    avg_speed_in_state,
    max_speed_in_state,
    unix_timestamp(timestamp) - unix_timestamp(state_timestamp_start) as state_duration_in_seconds,
    lat as end_lat,
    lng as end_lng
 from
    cte_telemetry_with_row_num
  where
    rn = 1
)
-- and finally, use all of that to run normal queries on
-- such as show telemetry durations for particular states, between timespans, by vehicle number, or speed
select
  *
from
  cte_telemetry_state_durations
where
  state in ("FAST", "TOOFAST")
order by state_start;

-- ideas for future possibilities:
-- use the state_group_id to plot on a map
-- identify convergence of vehicles, sequence of reinforcements, time-to-scene, first-on-scene
-- if telemetry was real-time, understand proxmity of deployed resources and posture of reserve resources
-- supplement data with other data sources such as vehicle / crew capability / speciality [K-9 for example], or commslogs, or instrument metrics [lights state, sirens state, etc]
-- adjust thresholds to be tuned to the specifics of road the vehicle is on based on lat/lng [e.g. residential vs. school zone vs. rural road, etc]
-- consider traffic density as a function of time of day and impact of vehicle presence on traffic patterns?