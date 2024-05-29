from snowflake.snowpark.functions import when, col, split, lit, substring_index, regexp_count, avg, round, concat
from snowflake.snowpark import Session
import sys
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')


# Functions for each table in curated schema
def update_curated_transport(session: Session) -> None:
    """
    Updates curated_transport table with minor transformations on raw table
    """
    transport_df = session.sql("""
                               select
                                   id,
                                   date,
                                   trip_id,
                                   operator_short_name as operator_abbr,
                                   line_id,
                                   circuit_id,
                                   vehicle_id,
                                   additional_trip,
                                   was_cancelled,
                                   stop_id,
                                   arrival_time,
                                   arrival_prognose,
                                   arrival_prognose_status,
                                   departure_time,
                                   departure_prognose,
                                   departure_prognose_status,
                                   transit
                               from raw_transport
                                   """)
    try:
        transport_df.write.save_as_table('curated.curated_transport', mode='truncate')
        logging.info('Successfully updated table curated_transport')
    except Exception as e:
        logging.error('Error occured during updating table curated_transport:', e)


def update_curated_accessibility(session: Session) -> None:
    """
    Updates curated_accessibility table.
    Joins on tables raw_accessibility_1, raw_accessibility_2 and raw_toilets are used.
    """
    access1_df = session.table('raw_accessibility_1')
    access2_df = session.table('raw_accessibility_2')
    toilets_df = session.sql("""
                            select 
                                distinct parent_sloid,
                                designation,
                                wheelchair_accessibility
                            from raw_toilets
    """)
    access_merged_df = access1_df.join(access2_df,
                                       access1_df['sloid'] == access2_df['sloid'],
                                       join_type='outer').drop(access2_df['sloid'])
    access_merged_df = access_merged_df.withColumnRenamed(access_merged_df.columns[0], 'sloid')
    access_full_df = access_merged_df.join(toilets_df,
                                           access_merged_df['sloid'] == toilets_df['parent_sloid'],
                                           join_type='left').drop(toilets_df['parent_sloid'])
    access_full_df = access_full_df.withColumnRenamed(access_full_df.columns[0], 'sloid')
    access_full_df = access_full_df.select(
        col('sloid'),
        col('t_autonomy'),
        col('t_ramp'),
        col('t_advance_registration'),
        col('t_no_access'),
        col('t_no_info'),
        col('o_autonomy'),
        col('o_ramp'),
        col('o_advance_registration'),
        col('o_no_access'),
        col('o_no_info'),
        col('t_spare_transport'),
        col('o_spare_transport'),
        col('platform'),
        access_merged_df['parent_sloid'].alias('parent_sloid'),
        col('height'),
        col('inclination'),
        col('info_type'),
        col('tactile_systems'),
        col('access_type'),
        col('wheelchair_area_length'),
        col('wheelchair_area_widht').alias('wheelchair_area_width'),
        col('designation').alias('toilet_designation'),
        col('wheelchair_accessibility').alias('toilet_wheelchair_access')
    )

    try:
        access_full_df.write.save_as_table('curated.curated_accessibility', mode='truncate')
        logging.info('Successfully updated table curated_accessibility')
    except Exception as e:
        logging.error('Error occured during updating table curated_accessibility:', e)


def update_curated_vehicles(session: Session) -> None:
    """
    Updates curated_vehicles table.
    """
    trans_subtypes_df = session.table('raw_transport_subtypes')
    trans_subtypes_df = trans_subtypes_df.withColumn('international',
                                                     when(col('international').is_null(), 'false').otherwise(
                                                         col('international')))
    trans_subtypes_df = trans_subtypes_df.withColumn('public_use',
                                                     when(col('public_use').is_null(), 'false').otherwise(
                                                         col('public_use')))
    trans_subtypes_df = trans_subtypes_df.withColumn('ref_netex_transportsubmode',
                                                     when(col('ref_netex_transportsubmode').like('%OR%'),
                                                          split(col('ref_netex_transportsubmode'), lit(' '))[
                                                              0]).otherwise(col('ref_netex_transportsubmode')))
    trans_subtypes_df = trans_subtypes_df.select(
        col('vehicle_id'),
        col('vehicle_type_DE'),
        col('vehicle_type'),
        col('transport_type_id'),
        col('international'),
        col('public_use'),
        col('ref_netex_transportsubmode'),
        col('ref_netex_productcategoryref')
    )
    try:
        trans_subtypes_df.write.save_as_table('curated.curated_vehicles', mode='truncate')
        logging.info('Successfully updated table curated_vehicles')
    except Exception as e:
        logging.error('Error occured during updating table curated_vehicles:', e)


def update_curated_transport_types(session: Session) -> None:
    """
    Updates curated_transport_types table.
    """
    trans_types_df = session.table('raw_transport_types')
    trans_types_df = trans_types_df.withColumn('international',
                                               when(col('international').is_null(), 'false').otherwise(
                                                   col('international')))
    trans_types_df = trans_types_df.drop(trans_types_df['CH'])
    trans_types_df = trans_types_df.select(
        col('transport_type_id'),
        col('transport_type_DE'),
        col('transport_type'),
        col('international'),
        col('ref_netex')
    )
    try:
        trans_types_df.write.save_as_table('curated.curated_transport_types', mode='truncate')
        logging.info('Successfully updated table curated_transport_types')
    except Exception as e:
        logging.error('Error occured during updating table curated_transport_types:', e)


def update_curated_line_data(session: Session) -> None:
    """
    Updates curated_line_data table.
    """
    line_data_df = session.table('raw_line_data')
    # In case 'description' column contains multiple stops, retrieve 1st stop as departure_station
    line_data_df = line_data_df.withColumn('departure_station',
                                           when(col('description').like('% - %'),
                                                split(col('description'), lit(' -'))[0]).otherwise('None'))
    # In case 'description' column contains multiple stops, retrieve last stop as final_destination
    line_data_df = line_data_df.withColumn('final_destination',
                                           when(col('description').like('% - %'),
                                                substring_index(col('description'), lit(' -'), -1)).otherwise('None'))
    # In case 'description' column contains multiple stops, count mid stops as midstation_count
    line_data_df = line_data_df.withColumn('midstation_count',
                                           when(col('description').like('% - %'),
                                                regexp_count(col('description'), ' -') - 1).otherwise(0))
    line_data_df.drop_duplicates('line_id')
    line_data_df = line_data_df.select(
        col('slnid'),
        col('swisslinenumber').alias('swiss_line_number'),
        col('status'),
        col('line_type'),
        col('payment_type'),
        col('line_id'),
        col('sboid'),
        col('departure_station'),
        col('final_destination'),
        col('midstation_count'),
        col('description')
    )
    try:
        line_data_df.write.save_as_table('curated.curated_line_data', mode='truncate')
        logging.info('Successfully updated table curated_line_data')
    except Exception as e:
        logging.error('Error occured during updating table curated_line_data:', e)


def update_curated_business(session: Session) -> None:
    """
    Updates curated_business table.
    """
    business_types_df = session.sql("""
        select 
            distinct business_type_id,
            business_type_de,
            business_type_fr,
            business_type_it
        from raw_operators
        """)
    business_types_df = business_types_df.filter(~(col('business_type_id').contains(lit(','))))
    business_types_df = business_types_df.select(
        col('business_type_id'),
        col('business_type_DE'),
        col('business_type_FR'),
        col('business_type_IT')
    )
    try:
        business_types_df.write.save_as_table('curated.curated_business_types', mode='truncate')
        logging.info('Successfully updated table curated_business_types')
    except Exception as e:
        logging.error('Error occured during updating table curated_business_types:', e)


def update_curated_operators(session: Session) -> None:
    """
    Updated curated_operators table.
    """
    operators_df = session.sql("""
        select
            distinct sboid,
            said,
            abbreviation,
            company_name,
            description,
            status,
            business_type_id
        from raw_operators
    """)
    # In case 'business_type_id' contains multiple ids, get the 1st one as main and preserve it
    operators_df = operators_df.withColumn('business_type_id',
                                           when(col('business_type_id').like('%,%'),
                                                split(col('business_type_id'), lit(','))[0]).otherwise(
                                               col('business_type_id')))
    operators_df = operators_df.withColumn('company_name',
                                           when(col('company_name').is_null(), col('description')).otherwise(
                                               col('company_name')))
    operators_df = operators_df.select(
        col('sboid'),
        col('said'),
        col('abbreviation'),
        col('company_name'),
        col('description'),
        col('status'),
        col('business_type_id')
    )
    try:
        operators_df.write.save_as_table('curated.curated_operators', mode='truncate')
        logging.info('Successfully updated table curated_operators')
    except Exception as e:
        logging.error('Error occured during updating table curated_operators:', e)


def update_curated_stop_municipality(session: Session) -> None:
    """
    Updates curated_municipality_data and curated_stop_data tables.
    """
    municipality_df = session.sql("""
        select
            distinct municipality_id,
            municipality
        from raw_stop_data
    """)
    try:
        municipality_df.write.save_as_table('curated.curated_municipality_data', mode='truncate')
        logging.info('Successfully updated table curated_municipality_data')
    except Exception as e:
        logging.error('Error occured during updating table curated_municipality_data:', e)

    stop_data_df = session.sql("""
                               select
                                   distinct sd.stop_id as stop_id,
                                   stop_short_num,
                                   stop_control_num,
                                   sd.stop_name as stop_name,
                                   od.stop_abbr as stop_abbr,
                                   stop_city as city,
                                   municipality_id,
                                   sd.canton_id as canton_id,
                                   replace(coord_e,' ','')::FLOAT as coord_e,
                                   replace(coord_n,' ','')::FLOAT as coord_n,
                                   altitude
                               from raw_stop_data sd
                               left join raw_occupancy_data od
                               on sd.stop_id = od.stop_id
                               """)
    # Create column 'sloid' by concatenating  modificated 'stop_id' str with prefix. It allows further joins
    stop_data_df = stop_data_df.withColumn('sloid', concat(lit('ch:1:sloid:'), stop_data_df['stop_id'] - 8500000))

    try:
        stop_data_df.write.save_as_table('curated.curated_stop_data', mode='truncate')
        logging.info('Successfully updated table curated_stop_data')
    except Exception as e:
        logging.error('Error occured during updating table curated_stop_data:', e)


def update_curated_occupancy(session: Session) -> None:
    """
    Updates curated_occupancy table.
    """
    occupancy_df = session.sql("""
                           select
                               od1.stop_id,
                               to_number(replace(od1.avg_daily_traffic,' ',''),38,0) as avg_daily_traffic,
                               to_number(replace(od1.avg_wday_traffic,' ',''),38,0) as avg_wday_traffic,
                               to_number(replace(od1.avg_holiday_traffic,' ',''),38,0) as avg_holiday_traffic,
                               to_number(replace(od1.avg_daily_traffic,' ',''),38,0) - to_number(replace(od2.avg_daily_traffic,' ',''),38,0) as daily_odds_2018,
                               to_number(replace(od1.avg_wday_traffic,' ',''),38,0) - to_number(replace(od2.avg_wday_traffic,' ',''),38,0) as wday_odds_2018,
                               to_number(replace(od1.avg_holiday_traffic,' ',''),38,0) - to_number(replace(od2.avg_holiday_traffic,' ',''),38,0) as holiday_odds_2018
                           from raw_occupancy_data od1
                           inner join raw_occupancy_data od2
                           on od1.stop_id = od2.stop_id
                           where od1.year = '2022'
                               and od2.year = '2018'
                               """)
    try:
        occupancy_df.write.save_as_table('curated.curated_occupancy', mode='truncate')
        logging.info('Successfully updated table curated_occupancy')
    except Exception as e:
        logging.error('Error occured during updating table curated_occupancy:', e)


def update_curated_parkings(session: Session) -> None:
    """
    Updates curated_parking table.
    """
    bike_parking_df = session.sql("""
        select  
            stop_id,
            capacity
        from raw_bike_parking_data
    """)
    # Multiple capacity records are given for many stops. Calculating the average
    bike_parking_df = bike_parking_df.groupBy('stop_id').agg(avg('capacity').alias('avg_capacity'))
    bike_parking_df = bike_parking_df.select(col('stop_id'), round(col('avg_capacity'), 0).alias('capacity_bike'))

    parking_df = session.sql("""
        select 
            distinct stop_id,
            min_duration,
            max_duration,
            max_day_price,
            price,
            monthly_price,
            yearly_price,
            days_open,
            capacity_standard,
            capacity_disabled,
            capacity_reservable,
            capacity_charging
        from raw_parking_data
    """)

    # Join parking data and bike parking data together
    parking_df = parking_df.join(bike_parking_df, parking_df['stop_id'] == bike_parking_df['stop_id'],
                                 join_type='inner').drop(bike_parking_df['stop_id'])
    parking_df = parking_df.withColumnRenamed(parking_df.columns[0], 'stop_id')

    try:
        parking_df.write.save_as_table('curated.curated_parking', mode='truncate')
        logging.info('Successfully updated table curated_parking')
    except Exception as e:
        logging.error('Error occured during updating table curated_parking:', e)