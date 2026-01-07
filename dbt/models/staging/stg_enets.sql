with src as (
    select * from raw_events_dedup
),
typed as (
    select
        event_id,
        try_cast(event_ts as timestamp) as event_ts,
        user_id,
        event_type,
        product_id,
        price_gbp,
        session_id
    from src
)
select * from typed
where event_ts is not null
