import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "Creates spark session"
    spark_session =  get_spark_session('LOCAL')
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    "gives expected result for aggregation"
    result_schema = 'state string, count int'
    return spark.read.format('csv').schema(result_schema).option('delimiter','  ').load('data/processed.csv')

