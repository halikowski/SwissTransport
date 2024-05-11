from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from googletrans import Translator
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
        "SCHEMA":"consumption",
        "WAREHOUSE":"TRANSPORT_WH"
    }
    # creating snowflake session object
    return Session.builder.configs(connection_parameters).create()


def main():
    session = get_snowpark_session()

# transport_fact
    transport_fact_df = session.sql("""
                                select
                                    transport_fact_seq.nextval as id,
                                    date,
                                    trip_id,
                                    operator_abbr,
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
                                from curated.curated_transport
                                    """)
    try:
        transport_fact_df.write.save_as_table('consumption.transport_fact', mode='truncate')
        logging.info('Successfully updated transport_fact table')
    except Exception as e:
        logging.error('Error occured during updating the table transport_fact:', e)

# operators_dim
    curated_operators_df = session.table('curated.curated_operators')
    trips_count_df = session.sql("""select operator_abbr, count(*) as trips_count
                                                            FROM transport_fact
                                                            GROUP BY operator_abbr""")
    operators_dim_df = curated_operators_df.join(trips_count_df,
                                                 curated_operators_df['abbreviation']==trips_count_df['operator_abbr'],
                                                 'left_outer')
    operators_dim_df = operators_dim_df.withColumn('trips_count', col('trips_count'))
    operators_dim_df = operators_dim_df.drop('operator_abbr')
    try:
        operators_dim_df.write.save_as_table('consumption.operators_dim', mode='truncate')
        logging.info('Successfully updated operators_dim tabe')
    except Exception as e:
        logging.error('Error occured during updating table operators_dim:', e)

# accessibility_dim
    curated_accessibility_df = session.table('curated.curated_accessibility')
    accessibility_dim_df = curated_accessibility_df.na.drop(subset=['sloid'])
    try:
        accessibility_dim_df.write.save_as_table('consumption.accessibility_dim', mode='truncate')
        logging.info('Successfully updated accessibility_dim tabe')
    except Exception as e:
        logging.error('Error occured during updating table accessibility_dim',e )

# # business_types_dim
# #     @udf(packages=['googletrans'])
# #     def translate_de_to_en(text: str) -> str:
# #         translator = Translator()
# #         return translator.translate(text, src_lang="de", dest_lang="en").text

    curated_btypes_df = session.table('curated.curated_business_types')
    btypes_dim_df = curated_btypes_df.na.drop(subset=['business_type_id'])
    try:
        btypes_dim_df.write.save_as_table('consumption.business_types_dim', mode='truncate')
        logging.info('Successfully updated business_types_dim table')
    except Exception as e:
        logging.error('Error occured during updating table business_types_dim:', e)

# # lines_dim
    curated_lines_df = session.table('curated.curated_line_data')
    lines_dim = curated_lines_df.na.drop(subset=['line_id'])
    try:
        lines_dim.write.save_as_table('consumption.lines_dim', mode='truncate')
        logging.info('Successfully updated lines_dim table')
    except Exception as e:
        logging.error('Error occured during updating table lines_dim:', e)

# # municipality_dim
    curated_municipality_df = session.table('curated.curated_municipality_data')
    municipality_dim = curated_municipality_df.na.drop(subset=['municipality_id'])
    try:
        municipality_dim.write.save_as_table('consumption.municipality_dim', mode='truncate')
        logging.info('Successfully updated municipality_dim table')
    except Exception as e:
        logging.error('Error occured during updating table municipality_dim:', e)

# # occupancy_dim
    curated_occupancy_df = session.table('curated.curated_occupancy')
    occupancy_dim_df = curated_occupancy_df.na.drop(subset=['stop_id'])
    try:
        occupancy_dim_df.write.save_as_table('consumption.occupancy_dim', mode='truncate')
        logging.info('Successfully updated occupancy_dim table')
    except Exception as e:
        logging.error('Error occured during updating table occupancy_dim:', e)
#
# # parking_dim
    curated_parking_df = session.table('curated.curated_parking')
    parking_dim_df = curated_parking_df.na.drop(subset=['stop_id'])
    try:
        parking_dim_df.write.save_as_table('consumption.parking_dim', mode='truncate')
        logging.info('Successfully updated parking_dim table')
    except Exception as e:
        logging.error('Error occured during updating table parking_dim:', e)

# # stops_dim
    curated_stops_df = session.table('curated.curated_stop_data')
    stops_dim = curated_stops_df.na.drop(subset=['stop_id'])
    try:
        stops_dim.write.save_as_table('consumption.stops_dim', mode='truncate')
        logging.info('Successfully update stops_dim table')
    except Exception as e:
        logging.error('Error occured during updating table stops_dim:', e)

# # transport_types_dim
    curated_transport_types_df = session.table('curated.curated_transport_types')
    transport_types_dim_df = curated_transport_types_df.na.drop(subset=['transport_type_id'])
    try:
        transport_types_dim_df.write.save_as_table('consumption.transport_types_dim', mode='truncate')
        logging.info('Successfully updated transport_types_dim table')
    except Exception as e:
        logging.error('Error occured during updating table transport_types_dim:', e)

# # vehicles_dim
    curated_vehicles_df = session.table('curated.curated_vehicles').na.drop(subset=['vehicle_id'])
    trips_per_vehicle_df = session.sql("""select vehicle_id, count(*) as vehicle_trips_count
                                                                FROM transport_fact
                                                                GROUP BY vehicle_id""")
    vehicles_dim_df = curated_vehicles_df.join(trips_per_vehicle_df,
                                            curated_vehicles_df['vehicle_id']==trips_per_vehicle_df['vehicle_id'],
                                               'left_outer').drop(trips_per_vehicle_df['vehicle_id'])
    vehicles_dim_df = vehicles_dim_df.withColumnRenamed(vehicles_dim_df[0], 'vehicle_id')
    vehicles_dim_df = vehicles_dim_df.withColumn('vehicle_trips_count', col('vehicle_trips_count'))
    try:
        vehicles_dim_df.write.save_as_table('consumption.vehicles_dim', mode='truncate')
        logging.info('Successfully updated vehicles_dim table')
    except Exception as e:
        logging.error('Error occured during updating table vehicles_dim:', e)

if __name__ == '__main__':
    main()