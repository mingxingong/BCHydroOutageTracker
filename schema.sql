-- Enable PostGIS (Supabase: Database > Extensions > postgis, or run this)
create extension if not exists postgis;

-- Raw outage events, one row per BC Hydro outage id, updated as it evolves
create table outage_events (
    id              bigint primary key,        -- BC Hydro's outage id
    gis_id          bigint,
    municipality    text,
    area            text,
    cause           text,
    region_name     text,
    customers_out   int,
    date_off        timestamptz not null,
    date_on         timestamptz,                -- null while still active
    duration_min    int,                        -- computed once resolved
    status          text not null default 'active',  -- 'active' | 'resolved'
    latitude        double precision,
    longitude       double precision,
    polygon         geometry(Polygon, 4326),    -- affected area boundary
    first_seen_at   timestamptz not null default now(),
    last_seen_at    timestamptz not null default now()
);

create index outage_events_polygon_idx on outage_events using gist (polygon);
create index outage_events_status_idx on outage_events (status);
create index outage_events_municipality_idx on outage_events (municipality);

-- Your C&I target property list
create table properties (
    id              bigserial primary key,
    name            text not null,              -- e.g. "7525 Cambie Street"
    company         text,                       -- e.g. "Wesgroup Properties"
    address         text,
    latitude        double precision not null,
    longitude       double precision not null,
    geom            geometry(Point, 4326) generated always as (
                        st_setsrid(st_makepoint(longitude, latitude), 4326)
                    ) stored,
    property_type   text,                       -- warehouse, hotel, new-construction, etc
    created_at      timestamptz not null default now()
);

create index properties_geom_idx on properties using gist (geom);

-- Matches: which properties sat inside which outage polygons
create table outage_property_matches (
    id              bigserial primary key,
    outage_id       bigint not null references outage_events(id),
    property_id     bigint not null references properties(id),
    matched_at      timestamptz not null default now(),
    outreach_flagged_at timestamptz,             -- set once it's 1-2wks post-outage
    outreach_sent   boolean not null default false,
    unique (outage_id, property_id)
);

-- Spatial match function: finds properties inside outage polygons and
-- inserts new matches (skips ones already recorded). Called by the poller.
create or replace function match_properties_in_outages()
returns table (outage_id bigint, property_id bigint) as $$
begin
    return query
    insert into outage_property_matches (outage_id, property_id)
    select oe.id, p.id
    from outage_events oe
    join properties p on st_contains(oe.polygon, p.geom)
    where oe.polygon is not null
    on conflict (outage_id, property_id) do nothing
    returning outage_property_matches.outage_id, outage_property_matches.property_id;
end;
$$ language plpgsql;

-- Grid overlay for true polygon-based hotspot detection.
-- ~1km cells across the BC Hydro Lower Mainland / Fraser Valley service area.
-- (0.012 deg lon / 0.009 deg lat ≈ 1km at this latitude)
create table hotspot_grid (
    cell_id serial primary key,
    geom    geometry(Polygon, 4326) not null
);

insert into hotspot_grid (geom)
select st_makeenvelope(
    -123.3 + (x * 0.012), 49.00 + (y * 0.009),
    -123.3 + ((x + 1) * 0.012), 49.00 + ((y + 1) * 0.009),
    4326
)
from generate_series(0, 80) as x, generate_series(0, 40) as y;

create index hotspot_grid_geom_idx on hotspot_grid using gist (geom);

-- Superimposes every resolved outage polygon onto the grid and scores each
-- cell by how many outages intersected it. This is the real overlay-based
-- hotspot detection, independent of municipality names or property lists.
create view outage_hotspot_grid as
select
    g.cell_id,
    st_x(st_centroid(g.geom)) as lon,
    st_y(st_centroid(g.geom)) as lat,
    count(oe.id) as outage_count,
    count(oe.id) filter (where oe.duration_min < 240) as short_outage_count,
    round(
        100.0 * count(oe.id) filter (where oe.duration_min < 240) / nullif(count(oe.id), 0), 1
    ) as pct_under_4h,
    round(
        count(oe.id) * (0.5 + 0.5 * (count(oe.id) filter (where oe.duration_min < 240)::numeric / nullif(count(oe.id), 0))), 1
    ) as hotspot_score
from hotspot_grid g
join outage_events oe
    on st_intersects(g.geom, oe.polygon)
    and oe.status = 'resolved'
group by g.cell_id, g.geom
having count(oe.id) > 0
order by hotspot_score desc;

-- Hotspot view: outage frequency by municipality, with NO dependency on the
-- properties table. This is for finding NEW prospecting zones, not scoring
-- known accounts. Feeds directly into Apollo's organization_locations filter.
-- (Coarser than outage_hotspot_grid above, but useful as a quick city-level
-- rollup for Apollo searches, which require a location name rather than a
-- lat/lon or polygon.)
create view outage_hotspots as
select
    municipality,
    count(*) as outage_count,
    count(*) filter (where duration_min < 240) as short_outage_count,
    round(
        100.0 * count(*) filter (where duration_min < 240) / nullif(count(*), 0), 1
    ) as pct_under_4h,
    max(date_off) as most_recent_outage,
    -- simple weighted score: frequency matters, but recurring SHORT outages
    -- matter more for a backup-battery pitch than one long storm event
    round(
        count(*) * (0.5 + 0.5 * (count(*) filter (where duration_min < 240)::numeric / nullif(count(*), 0))), 1
    ) as hotspot_score
from outage_events
where status = 'resolved'
group by municipality
order by hotspot_score desc;

-- Convenience view: resilience score per property
-- (outage frequency + share of outages under 4h = good backup-battery fit)
create view property_outage_stats as
select
    p.id as property_id,
    p.name,
    p.company,
    count(oem.outage_id) as outage_count,
    count(oem.outage_id) filter (where oe.duration_min < 240) as short_outage_count,
    round(
        100.0 * count(oem.outage_id) filter (where oe.duration_min < 240)
        / nullif(count(oem.outage_id), 0), 1
    ) as pct_under_4h,
    max(oe.date_off) as most_recent_outage
from properties p
join outage_property_matches oem on oem.property_id = p.id
join outage_events oe on oe.id = oem.outage_id
where oe.status = 'resolved'
group by p.id, p.name, p.company;
