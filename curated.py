from snowflake.snowpark import Session
from snowflake.snowpark.functions import when, col, split, lit, substring_index, regexp_count, avg, round, concat
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
        "SCHEMA":"raw",
        "WAREHOUSE":"TRANSPORT_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()

def main():
    session = get_snowpark_session()

    # TRANSFORMATIONS:

    # transport

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
    transport_df.write.save_as_table('curated.curated_transport', mode='append')

    # merging raw accessibility tables into 1

    access1_df = session.sql('select * from RAW_ACCESSIBILITY_1')
    access2_df = session.sql('select * from RAW_ACCESSIBILITY_2')
    toilets_df = session.sql('select distinct parent_sloid, designation, wheelchair_accessibility from raw_toilets')
    access_merged_df = access1_df.join(access2_df, access1_df['sloid']==access2_df['sloid'], join_type='outer').drop(access2_df['sloid'])
    access_merged_df = access_merged_df.withColumnRenamed(access_merged_df.columns[0], 'sloid')
    access_full_df = access_merged_df.join(toilets_df, access_merged_df['sloid']==toilets_df['parent_sloid'], join_type='left').drop(toilets_df['parent_sloid'])
    access_full_df = access_full_df.withColumnRenamed(access_full_df.columns[0],'sloid')
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
        access_merged_df['parent_sloid'],
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
    # access_full_df.show(50)
    access_full_df.write.save_as_table('curated.curated_accessibility', mode ='append')

    # transforming transport_subtypes table

    trans_subtypes_df = session.sql('select * from raw_transport_subtypes')
    trans_subtypes_df = trans_subtypes_df.withColumn('international',when(col('international').is_null(),'false').otherwise(col('international')))
    trans_subtypes_df = trans_subtypes_df.withColumn('public_use',when(col('public_use').is_null(),'false').otherwise(col('public_use')))
    trans_subtypes_df = trans_subtypes_df.withColumn('ref_netex_transportsubmode',when(col('ref_netex_transportsubmode').like('%OR%'),split(col('ref_netex_transportsubmode'),lit(' '))[0]).otherwise(col('ref_netex_transportsubmode')))
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
    trans_subtypes_df.write.save_as_table('curated.curated_vehicles', mode='append')

    # transforming transport_types table

    trans_types_df = session.sql('select * from raw_transport_types')
    trans_types_df = trans_types_df.withColumn('international',when(col('international').is_null(),'false').otherwise(col('international')))
    trans_types_df = trans_types_df.drop(trans_types_df['CH'])
    trans_types_df = trans_types_df.select(
        col('transport_type_id'),
        col('transport_type_DE'),
        col('transport_type'),
        col('international'),
        col('ref_netex')
    )
    trans_types_df.write.save_as_table('curated.curated_transport_types', mode='append')

    # transforming line_data table

    line_data_df = session.sql('select * from raw_line_data')
    line_data_df = line_data_df.withColumn('departure_station', when(col('description').like('% - %'),split(col('description'),lit(' -'))[0]).otherwise('None'))
    line_data_df = line_data_df.withColumn('final_destination', when(col('description').like('% - %'), substring_index(col('description'),lit(' -'), -1)).otherwise('None'))
    line_data_df = line_data_df.withColumn('midstation_count', when(col('description').like('% - %'), regexp_count(col('description'),' -') - 1).otherwise(0))
    line_data_df.drop_duplicates(subset = ['line_id'])
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

    line_data_df.write.save_as_table('curated.curated_line_data', mode='append')

    # transforming business table (from operators)

    business_types_df = session.sql('select distinct business_type_id, business_type_de, business_type_fr, business_type_it from raw_operators')
    business_types_df = business_types_df.filter(~(col('business_type_id').contains(lit(','))))
    business_types_df = business_types_df.select(
        col('business_type_id'),
        col('business_type_DE'),
        col('business_type_FR'),
        col('business_type_IT')
    )
    business_types_df.write.save_as_table('curated.curated_business_types', mode='append')

    # transforming operators table
    operators_df = session.sql('select distinct sboid, said, abbreviation, company_name, description, status, business_type_id from raw_operators')
    operators_df = operators_df.withColumn('business_type_id', when(col('business_type_id').like('%,%'), split(col('business_type_id'), lit(','))[0]).otherwise(col('business_type_id')))
    operators_df = operators_df.withColumn('company_name', when(col('company_name').is_null(), col('description')).otherwise(col('company_name')))
    operators_df = operators_df.select(
        col('sboid'),
        col('said'),
        col('abbreviation'),
        col('company_name'),
        col('description'),
        col('status'),
        col('business_type_id')
    )
    operators_df.write.save_as_table('curated.curated_operators', mode='append')

    # transforming stop table

    municipality_df = session.sql('select distinct municipality_id, municipality from raw_stop_data')
    municipality_df.write.save_as_table('curated.curated_municipality_data', mode='append')

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
                                coord_e,
                                coord_n,
                                altitude
                            from raw_stop_data sd
                            inner join raw_occupancy_data od
                            on sd.stop_id = od.stop_id
                            """)
    stop_data_df = stop_data_df.withColumn('sloid', concat(lit('ch:1:sloid:'),stop_data_df['stop_id'] - 8500000))
    stop_data_df.write.save_as_table('curated.curated_stop_data', mode='append')

#occupancy # co z rokiem gdy zmieni sie plik??
    occupancy_df = session.sql("""
                        select
                            od1.stop_id,
                            to_number(replace(od1.avg_daily_traffic,' ',''),38,0) as avg_daily_traffic,
                            to_number(replace(od1.avg_wday_traffic,' ',''),38,0) as avg_wday_traffic,
                            to_number(replace(od1.avg_holiday_traffic,' ',''),38,0) as avg_holiday_traffic,
                            to_number(replace(od1.avg_daily_traffic,' ',''),38,0) - to_number(replace(od2.avg_daily_traffic,' ',''),38,0) as daily_2018_odds,
                            to_number(replace(od1.avg_wday_traffic,' ',''),38,0) - to_number(replace(od2.avg_wday_traffic,' ',''),38,0) as wday_2018_odds,
                            to_number(replace(od1.avg_holiday_traffic,' ',''),38,0) - to_number(replace(od2.avg_holiday_traffic,' ',''),38,0) as holiday_2018_odds
                        from raw_occupancy_data od1
                        inner join raw_occupancy_data od2
                        on od1.stop_id = od2.stop_id
                        where od1.year = '2022'
                            and od2.year = '2018'
                            """)
    occupancy_df.write.save_as_table('curated.curated_occupancy', mode='append')

#parking data
    bike_parking_df = session.sql('select stop_id, capacity from raw_bike_parking_data')
    bike_parking_df = bike_parking_df.groupBy('stop_id').agg(avg('capacity').alias('avg_capacity'))
    bike_parking_df = bike_parking_df.select(col('stop_id'), round(col('avg_capacity'),0).alias('capacity_bike'))
    # bike_parking_df.show(5)

    parking_df = session.sql('select distinct stop_id, min_duration, max_duration, max_day_price, price, monthly_price,\
                            yearly_price, days_open, capacity_standard, capacity_disabled, capacity_reservable, \
                            capacity_charging \
                            from raw_parking_data')
    parking_df = parking_df.join(bike_parking_df, parking_df['stop_id'] == bike_parking_df['stop_id'],
                                 join_type='inner').drop(bike_parking_df['stop_id'])
    parking_df = parking_df.withColumnRenamed(parking_df.columns[0], 'stop_id')
    parking_df.show(5)
    parking_df.write.save_as_table('curated.curated_parking', mode='append')

if __name__ == '__main__':
    main()