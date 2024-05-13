from snowflake.snowpark import Session
from snowflake.snowpark.functions import when, col, split, lit, substring_index, regexp_count
import sys
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')


def get_snowpark_session() -> Session:
    connection_parameters = {
       "ACCOUNT":"iigqpyy-qq30975",
        "USER":"user_01",
        "PASSWORD":"Snowp4rk",
        "ROLE":"SYSADMIN",
        "DATABASE":"SWISS_TRANSPORT",
        "SCHEMA":"RAW",
        "WAREHOUSE":"TRANSPORT_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def main():
    session = get_snowpark_session()

    # SnowSQL commands for file ingestion:

    # PUT file://C:/Users/Mateusz/Downloads/transport_files/2024-04-12_istdaten.csv @my_stg/daily auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/actual_date_line_versions_2024-04-17.csv @my_stg/weekly/line_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/actual_date_business_organisation_versions_2024-04-17.csv @my_stg/weekly/org_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/actual-date-stop_point-2024-04-04.csv @my_stg/weekly/stop_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/actual-date-platform-2024-04-19.csv @my_stg/weekly/platform_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/actual-date-toilet-2024-04-04.csv @my_stg/weekly/toilet_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/bav_list_current_timetable.csv @my_stg/weekly/bav_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/full-toilet-2024-04-04.csv @my_stg/weekly/full_toilet_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/bfr_haltestellendaten.csv @my_stg/weekly/bfr_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/transportmodes280923.csv @my_stg/monthly/tmode_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/transportsubmodes.csv @my_stg/monthly/tsub_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/bike_parking.json @my_stg/monthly/bike_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/parking-facilities.json @my_stg/monthly/parking_files auto_compress=true;
    # PUT file://C:/Users/Mateusz/Downloads/transport_files/t01x_sbb-cff-ffs_frequentia_2022.csv @my_stg/yearly/occupancy_files auto_compress=true;

    # loading raw_transport
    try:
        session.sql("\
            copy into raw_transport from(\
                select\
                    transport_seq.nextval,\
                    to_date($1, 'DD.MM.YYYY') as date,\
                    $2::text as trip_id,\
                    $3::text as operator_id,\
                    $4::text as operator_short_name,\
                    $5::text as operator_name,\
                    $6::text as vehicle_type,\
                    $8::text as line_id,\
                    $9::text as circuit_id,\
                    $10::text as vehicle_id,\
                    $11::boolean as additional_trip,\
                    $12::boolean as was_cancelled,\
                    $13::number(38,0) as stop_id,\
                    $14::text as stop_name,\
                    to_timestamp_ntz($15, 'DD.MM.YYYY HH24:MI') as arrival_time,\
                    to_timestamp_ntz($16, 'DD.MM.YYYY HH24:MI:SS') as arrival_prognose,\
                    $17::text as arrival_prognose_status,\
                    to_timestamp_ntz($18, 'DD.MM.YYYY HH24:MI')  as departure_time,\
                    to_timestamp_ntz($19, 'DD.MM.YYYY HH24:MI:SS') as departure_prognose,\
                    $20::text as departure_prognose_status,\
                    $21::boolean as transit\
                    from \
                    @my_stg/daily\
                    (file_format => 'swiss_transport.common.my_csv_format')\
                    )\
                on_error = 'continue'\
                "
        ).collect()
        logging.info('Successfully copied data into raw_transport table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_transport table')

# loading raw_line_data
    try:
        session.sql("\
            copy into raw_line_data from (\
            select\
                $1::text as slnid,\
                $4::text as swissLineNumber,\
                $5::text as status,\
                $6::text as line_type,\
                $7::text as payment_type,\
                $8::text as line_id,\
                $9::text as sboid,\
                $18::text as description,\
                $19::text as comment\
            from\
            @my_stg/weekly/line_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error='continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_line_data table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_line_data table')

# loading raw_operators
    try:
        session.sql("\
            copy into raw_operators from (\
            select\
                $1::text as sboid,\
                $2::text as said,\
                $5::number(38,0) as organisation_number,\
                $6::text as status,\
                $7::text as description_DE,\
                $10::text as description,\
                $11::text as abbreviation_DE,\
                $14::text as abbreviation,\
                $15::text as business_type_id,\
                $16::text as business_type_DE,\
                $17::text as business_type_IT,\
                $18::text as business_type_FR,\
                $20::text as company_id,\
                $21::text as company_name\
            from \
            @my_stg/weekly/org_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error='continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_operators table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_operators table')

# loading raw_Stop_data
    try:
        session.sql("\
            copy into raw_stop_data from (\
            select\
                $1::number(38,0) as stop_id,\
                $2::number(38,0) as land_num,\
                $3::number(38,0) as stop_short_num,\
                $4::number(38,0) as stop_control_num,\
                $5::text as stop_name,\
                $15::text as vehicle_type,\
                $17::text as organisation_id,\
                $20::text as stop_city,\
                $21::number(38,0) as municipality_id,\
                $22::text as municipality,\
                $23::text as canton_id,\
                $24::text as coord_E,\
                $25::text as coord_N,\
                $26::text as altitude\
            from \
            @my_stg/weekly/bav_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error='continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_stop_data table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_stop_data table')

# loading raw_accessibility_1
    try:
        session.sql("\
            copy into raw_accessibility_1 from (\
            select\
                $15::text as sloid,\
                $3::boolean as t_autonomy,\
                $4::boolean as t_ramp,\
                $5::boolean as t_advance_registration,\
                $6::boolean as t_no_access,\
                $7::boolean as t_no_info,\
                $8::boolean as o_autonomy,\
                $9::boolean as o_ramp,\
                $10::boolean as o_advance_registration,\
                $11::boolean as o_no_access,\
                $12::boolean as o_no_info,\
                $13::boolean as t_spare_transport,\
                $14::boolean as o_spare_transport,\
                $16::text as platform\
            from \
            @my_stg/weekly/bfr_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error ='continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_accessibility_1 table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_accessibility_1 table')


# loading raw_accessibility_2
    try:
        session.sql("\
        copy into raw_accessibility_2 from (\
            select\
                $1::text as sloid,\
                $2::text as parent_sloid,\
                $10::number(5,1) as height,\
                $12::number(3,1) as inclination,\
                $14::varchar as info_type,\
                $18::text as tactile_systems,\
                $19::text as access_type,\
                $20::number(10,1) as wheelchair_area_length,\
                $21::number(10,1) as wheelchair_area_width\
            from \
            @my_stg/weekly/platform_files \
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error = 'continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_accessibility_2 table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_accessibility_2 table')

# Loading raw_toilets
    try:
        session.sql("\
        copy into raw_toilets from (\
            select\
                $1::text as sloid,\
                $2::text as parent_sloid,\
                $4::text as designation,\
                $6::text as wheelchair_accessibility\
            from \
            @my_stg/weekly/full_toilet_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error = 'continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_toilets table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_toilets table')

#loading json parking data
    try:
        session.sql("""
            COPY INTO raw_json_parking
            FROM @my_stg/monthly/parking_files
            FILE_FORMAT = 'swiss_transport.common.my_json_format'
            ON_ERROR = 'continue'
        """).collect()
        logging.info('Successfully copied data into raw_json_parking table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_json_parking table')


# inserting data from json_raw_parking to raw_parking_data
    try:
        session.sql("""
        insert into raw_parking_data (
            select
            f.value:properties:uic::number(38,0) as stop_id,
            f.value:properties:pricingModel:minimumDuration::number(38,0) as min_duration,
            f.value:properties:pricingModel:maximumDuration::number(38,0) as max_duration,
            f.value:properties:pricingModel:maximumDayPrice::number(38,0) as max_day_price,
            f.value:properties:pricingModel:priceSegments[0]:price::number(38,0) as price,
            f.value:properties:pricingModel:monthlyTicketPrice::number(38,0) as monthly_price,
            f.value:properties:pricingModel:yearlyTicketPrice::number(38,0) as yearly_price,
            ARRAY_SIZE(f.value:properties:operationTime:daysOfWeek)::number(38,0) as days_open,
            f.value:properties:capacities[0]:total::number(38,0) as capacity_standard,
            f.value:properties:capacities[1]:total::number(38,0) as capacity_disabled,
            f.value:properties:capacities[2]:total::number(38,0) as capacity_reservable,
            f.value:properties:capacities[3]:total::number(38,0) as capacity_charging,
    
        from raw_json_parking,
        lateral flatten(input => json_data:features) f
        )""").collect()
        logging.info('Successfully copied data into raw_parking_data table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_parking_data table')

# loading json bike parking file
    try:
        session.sql("""
        copy into raw_json_bike_parking
        from @my_stg/monthly/bike_files
        file_format = 'swiss_transport.common.my_json_format'
        on_error = 'continue'
        """).collect()
        logging.info('Successfully copied data into raw_json_bike_parking table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_json_bike_parking table')

# loading data from json to bike
    try:
        session.sql("""
        insert into raw_bike_parking_data (
        select
            f.value:properties:stopPlaceUic::number(38,0) as stop_id,
            f.value:properties:capacity::number(38,0) as capacity
        from raw_json_bike_parking,
        lateral flatten(input => json_data:features) f
        )""").collect()
        logging.info('Successfully copied data into raw_bike_parking_data table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_bike_parking_data table')

# loading raw_transport_subtypes
    try:
        session.sql("\
            copy into raw_transport_subtypes from(\
                select\
                    $1::text as vehicle_id,\
                    $2::text as vehicle_type_DE,\
                    $5::text as vehicle_type,\
                    $6::text as transport_type_id,\
                    $8::boolean as international,\
                    $9::boolean as public_use,\
                    $12::text as Ref_NeTEx_TransportSubMode,\
                    $13::text as Ref_NeTEx_ProductCategoryRef\
                from \
                @my_stg/monthly/tsub_files\
                (file_format => 'swiss_transport.common.my_csv_format')\
                )\
            on_error='continue'\
            "
        ).collect()
        logging.info('Successfully copied data into raw_transport_subtypes table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_transport_subtypes table')


# loading raw_transport_types
    try:
        session.sql("\
            copy into raw_transport_types from (\
            select\
                $1::text as transport_type_id,\
                $2::text as transport_Type_DE,\
                $5::text as transport_type,\
                $6::boolean as CH,\
                $7::boolean as international,\
                $10::text as ref_NeTEx\
            from \
            @my_stg/monthly/tmode_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error='continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_transport_types table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_transport_types table')


# loading raw_occupancy_data
    try:
        session.sql("\
            copy into raw_occupancy_data from (\
            select\
                $1::text as stop_abbr,\
                $2::number(38,0) as stop_id,\
                $3::text as stop_name,\
                $4::text as canton_id,\
                $5::text as station_owner_id,\
                $6::number(4,0) as year,\
                $7::text as avg_daily_traffic,\
                $8::text as avg_wday_traffic,\
                $9::text as avg_holiday_traffic\
            from \
            @my_stg/yearly/occupancy_files\
            (file_format => 'swiss_transport.common.my_csv_format')\
        )\
        on_error = 'continue'\
        "
        ).collect()
        logging.info('Successfully copied data into raw_occupancy_data table')
    except Exception as e:
        logging.error('Error occured during copying data into raw_occupancy_data table')


if __name__ == '__main__':
    main()