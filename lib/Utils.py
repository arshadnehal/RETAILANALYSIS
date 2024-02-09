import sys
from pyspark.sql import SparkSession
from lib.ConfigReader import get_pyspark_config

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf=get_pyspark_config(env)) \
            .enableHiveSupport() \
            .getOrCreate()

def check_input_environment(env):
    valid_env = ['LOCAL', 'TEST', 'PROD']
    
    env_ext = env[1].upper() if len(env) == 2 else input("Please specify the environment: ").upper()
    
    if env_ext in valid_env:
        return env_ext
    else:
        print('Invalid environment, exiting...')
        sys.exit(-1)
