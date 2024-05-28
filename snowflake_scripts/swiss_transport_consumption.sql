-- CONSUMPTION SCHEMA OPERATIONS

use role sysadmin;
use warehouse transport_wh;
use database swiss_transport;
use schema consumption;

-- tables creation for final stage

create or replace table transport_fact (
    id number(38,0) autoincrement primary key,
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

create or replace table accessibility_dim (
    -- t is for trains(incl metro tram etc), o is for other means of transport (bus etc.)
    sloid varchar primary key,
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

create or replace table vehicles_dim (
    vehicle_id varchar primary key,
    vehicle_type_DE varchar,
    vehicle_type varchar,
    transport_type_id varchar,
    international boolean,
    public_use boolean,
    Ref_NeTEx_TransportSubMode varchar,
    Ref_NeTEx_ProductCategoryRef varchar,
    vehicle_trips_count number(38,0)
);

create or replace table transport_types_dim (
    transport_type_id varchar primary key,
    transport_type_DE varchar,
    transport_type varchar,
    international boolean,
    ref_NeTEx varchar
);

create or replace table lines_dim (
    slnid varchar,
    swiss_line_number varchar,
    status varchar,
    line_type varchar,
    payment_type varchar,
    line_id varchar primary key,
    sboid varchar,
    departure_station varchar,
    final_destination varchar,
    midstation_count number(38,0),
    description varchar
);

create or replace table operators_dim (
    sboid varchar unique,
    said varchar,
    abbreviation varchar primary key,
    company_name varchar,
    description varchar,
    status varchar,
    business_type_id varchar,
    trips_count number(38,0)
);

create or replace table business_types_dim (
    business_type_id varchar primary key,
    business_type_DE varchar,
    business_type_FR varchar,
    business_type_IT varchar
);

create or replace table municipality_dim (
    municipality_id number(38,0) primary key,
    municipality varchar
);

create or replace table stops_dim (
    stop_id number(38,0) primary key,
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
    sloid varchar unique
);

create or replace table occupancy_dim (
    stop_id number(38,0) primary key,
    avg_daily_traffic number(38,0),
    avg_wday_traffic number(38,0),
    avg_holiday_traffic number(38,0),
    daily_odds_2018 number(38,0),
    wday_odds_2018 number(38,0),
    holiday_odds_2018 number(38,0)
);

create or replace table parking_dim (
    stop_id number(38,0) primary key,
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

-- foreign keys allocation

alter table transport_fact add constraint 
    fk_vehicle FOREIGN KEY (vehicle_id) REFERENCES vehicles_dim (vehicle_id);

alter table transport_fact add constraint
    fk_operator FOREIGN KEY (operator_abbr) REFERENCES operators_dim (abbreviation);

alter table transport_fact add constraint
    fk_line FOREIGN KEY (line_id) REFERENCES lines_dim (line_id);

alter table transport_fact add constraint
    fk_stop FOREIGN KEY (stop_id) REFERENCES stops_dim (stop_id);

alter table vehicles_dim add constraint
    fk_transport_type FOREIGN KEY (transport_type_id) REFERENCES transport_types_dim                     (transport_type_id);

alter table operators_dim add constraint
    fk_business FOREIGN KEY (business_type_id) REFERENCES business_types_dim (business_type_id);

alter table stops_dim add constraint
    fk_accessibility FOREIGN KEY (sloid) REFERENCES accessibility_dim (sloid);

alter table stops_dim add constraint
    fk_municipality FOREIGN KEY (municipality_id) REFERENCES municipality_dim (municipality_id);

alter table stops_dim add constraint
    fk_occupancy FOREIGN KEY (stop_id) REFERENCES occupancy_dim (stop_id);

alter table stops_dim add constraint
    fk_parking FOREIGN KEY (stop_id) REFERENCES parking_dim (stop_id);
