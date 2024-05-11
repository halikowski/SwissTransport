from snowflake.snowpark import Session, DataFrame
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
    transport_fact_df.write.save_as_table('consumption.transport_fact', mode='append')

# operators_dim
    curated_operators_df = session.sql('select * from curated.curated_operators')
    trips_count_df = session.sql("""select operator_abbr, count(*) as trips_count 
                                                            FROM transport_fact 
                                                            GROUP BY operator_abbr""")
    operators_dim_df = curated_operators_df.join(trips_count_df,
                                                 curated_operators_df['abbreviation']==trips_count_df['operator_abbr'],
                                                 'left_outer')
    operators_dim_df = operators_dim_df.withColumn('trips_count', col('trips_count'))
    operators_dim_df = operators_dim_df.drop('operator_abbr')
    operators_dim_df.write.save_as_table('consumption.operators_dim', mode='append')







if __name__ == '__main__':
    main()