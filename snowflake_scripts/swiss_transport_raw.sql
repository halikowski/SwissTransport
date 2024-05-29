-- RAW SCHEMA OPERATIONS

use role sysadmin;
use warehouse transport_wh;
use database swiss_transport;
use schema raw;

-- raw transient tables creation for every ingested file

create or replace transient table raw_transport(
    id number(38,0),
    date date,
    trip_id varchar,
    operator_id varchar,
    operator_short_name varchar,
    operator_name varchar,
    vehicle_type varchar,
    line_id varchar,
    circuit_id varchar,
    vehicle_id varchar,
    additional_trip boolean,
    was_cancelled boolean,
    stop_id number(38,0),
    stop_name varchar,
    arrival_time timestamp,
    arrival_prognose timestamp,
    arrival_prognose_status varchar,
    departure_time timestamp,
    departure_prognose timestamp,
    departure_prognose_status varchar,
    transit boolean
);

create or replace transient table raw_transport_subtypes (
    vehicle_id varchar,
    vehicle_type_DE varchar,
    vehicle_type varchar,
    transport_type_id varchar,
    international boolean,
    public_use boolean,
    Ref_NeTEx_TransportSubMode varchar,
    Ref_NeTEx_ProductCategoryRef varchar
);

create or replace transient table raw_transport_types (
    transport_type_id varchar,
    transport_type_DE varchar,
    transport_type varchar,
    CH boolean,
    international boolean,
    ref_NeTEx varchar
);

create or replace transient table raw_line_data (
    slnid varchar, -- digital line code
    swissLineNumber varchar,
    status varchar,
    line_type varchar,
    payment_type varchar,
    line_id varchar,
    sboid varchar, -- digital operator code
    description varchar,
    comment varchar
);

create or replace transient table raw_operators (
    sboid varchar,
    said varchar,
    organisation_number number(38,0),
    status varchar,
    description_DE varchar,
    description varchar,
    abbreviation_DE varchar,
    abbreviation varchar,
    business_type_id varchar,
    business_type_DE varchar,
    business_type_IT varchar,
    business_type_FR varchar,
    company_id varchar,
    company_name varchar
);

create or replace transient table raw_stop_data (
    stop_id number(38,0),
    land_num number(38,0),
    stop_short_num number(38,0),
    stop_control_num number(38,0),
    stop_name varchar,
    vehicle_type varchar,
    organisation_id varchar,
    stop_city varchar,
    municipality_id number(38,0),
    municipality varchar,
    canton_id varchar,
    coord_E varchar,
    coord_N varchar,
    altitude varchar
);

create or replace transient table raw_occupancy_data (
    stop_abbr varchar,
    stop_id number(38,0),
    stop_name varchar,
    canton_id varchar,
    station_owner_id varchar,
    year number(4,0),
    avg_daily_traffic varchar,
    avg_wday_traffic varchar,
    avg_holiday_traffic varchar
);

create or replace transient table raw_accessibility_1 (
    -- t is for trains(incl metro tram etc), o is for other means of transport (bus etc.)
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
    platform varchar
);

create or replace transient table raw_accessibility_2 (
    sloid varchar,
    parent_sloid varchar,
    height number(5,1),
    inclination number (3,1),
    info_type varchar,
    tactile_systems varchar,
    access_type varchar,
    wheelchair_area_length number(10,1),
    wheelchair_area_widht number(10,1)
);

create or replace transient table raw_toilets (
    sloid varchar,
    parent_sloid varchar,
    designation varchar,
    wheelchair_accessibility varchar
);

create or replace transient table raw_parking_data (
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
    capacity_charging number(38,0)
);

create or replace transient table raw_bike_parking_data (
    stop_id number(38,0),
    capacity number(38,0)
);

-- staging tables for json data
create or replace transient table raw_json_parking (
    json_data variant
);

create or replace transient table raw_json_bike_parking (
    json_data variant
);
