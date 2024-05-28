-- CURATED SCHEMA OPERATIONS

use role sysadmin;
use warehouse transport_wh;
use database swiss_transport;
use schema curated;

-- tables creation, based on tables from raw schema

create or replace transient table curated_transport (
    id number(38,0),
    date date,
    trip_id varchar,
    operator_abbr varchar,
    line_id varchar,
    circuit_id varchar,
    vehicle_id varchar,
    additional_trip boolean,
    was_cancelled boolean,
    stop_id number(38,0),
    arrival_time timestamp,
    arrival_prognose timestamp,
    arrival_prognose_status varchar,
    departure_time timestamp,
    departure_prognose timestamp,
    departure_prognose_status varchar,
    transit boolean
);

create or replace transient table curated_accessibility (
    sloid varchar,
    t_autonomy boolean,
    t_ramp  boolean,
    t_advance_registration  boolean,
    t_no_access  boolean,
    t_no_info  boolean,
    o_autonomy boolean,
    o_ramp boolean,
    o_advance_registration boolean,
    o_no_access boolean,
    o_no_info boolean,
    t_spare_transport boolean,
    o_spare_transport boolean,
    platform varchar,
    parent_sloid varchar,
    height number(5,1),
    inclination number (3,1),
    info_type varchar,
    tactile_systems varchar,
    access_type varchar,
    wheelchair_area_length number(10,1),
    wheelchair_area_width number(10,1),
    toilet_designation varchar,
    toilet_wheelchair_access varchar
);

create or replace transient table curated_vehicles (
    vehicle_id varchar,
    vehicle_type_DE varchar,
    vehicle_type varchar,
    transport_type_id varchar,
    international boolean,
    public_use boolean,
    Ref_NeTEx_TransportSubMode varchar,
    Ref_NeTEx_ProductCategoryRef varchar
);

create or replace transient table curated_transport_types (
    transport_type_id varchar,
    transport_type_DE varchar,
    transport_type varchar,
    international boolean,
    ref_NeTEx varchar
);

create or replace transient table curated_line_data (
    slnid varchar,
    swiss_line_number varchar,
    status varchar,
    line_type varchar,
    payment_type varchar,
    line_id varchar,
    sboid varchar, -- kod operatora cyfrowy
    departure_station varchar,
    final_destination varchar,
    midstation_count number(38,0),
    description varchar
);

create or replace transient table curated_operators (
    sboid varchar,
    said varchar,
    abbreviation varchar,
    company_name varchar,
    description varchar,
    status varchar,
    business_type_id varchar
);

create or replace transient table curated_business_types (
    business_type_id varchar,
    business_type_DE varchar,
    business_type_FR varchar,
    business_type_IT varchar
);

create or replace transient table curated_municipality_data (
    municipality_id number(38,0),
    municipality varchar
);


create or replace transient table curated_stop_data (
-- dodac sloid
    stop_id number(38,0),
    stop_short_num number(38,0),
    stop_control_num number(38,0),
    stop_abbr varchar,
    stop_name varchar,
    city varchar,
    municipality_id number(38,0),
    canton_id varchar,
    coord_e float,
    coord_n float,
    altitude varchar,
    sloid varchar
);

create or replace transient table curated_occupancy (
    stop_id number(38,0),
    avg_daily_traffic number(38,0),
    avg_wday_traffic number(38,0),
    avg_holiday_traffic number(38,0),
    daily_odds_2018 number(38,0),
    wday_odds_2018 number(38,0),
    holiday_odds_2018 number(38,0)
);

create or replace transient table curated_parking (
    stop_id number(38,0),
    min_duration number(38,0),
    max_duration number(38,0),
    max_day_price number(38,0),
    price number(38,0),
    monthly_price number(38,0),
    yearly_price number(38,0),
    days_open number(38,0),
    capacity_standard number(38,0),
    capacity_disabled number(38,0),
    capacity_reservable number(38,0),
    capacity_charging number(38,0),
    capacity_bike number(38,0)
);