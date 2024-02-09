import sys
from lib import DataManipulation, DataReader, Utils, logger
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__ == '__main__':

    job_run_env = Utils.check_input_environment(sys.argv)

    print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)

    #spark.sparkContext.setLogLevel('INFO')

    logger = Log4j(spark)

    logger.info("Created Spark Session")

    orders_df = DataReader.read_orders(spark,job_run_env)

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customers(spark,job_run_env)

    joined_df = DataManipulation.join_orders_customers(orders_filtered,customers_df)

    aggregated_results = DataManipulation.count_orders_state(joined_df)

    aggregated_results.show(50)

    #print(aggregated_results.collect())

    logger.info("this is the end of main")
