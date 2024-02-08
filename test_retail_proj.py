import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import count_orders_state, filter_closed_orders, filter_generic_orders
from lib.ConfigReader import get_app_config

def test_read_customers(spark):
    customer_count = read_customers(spark,'LOCAL').count()
    assert customer_count == 12435

def test_read_orders(spark):
    orders_count = read_orders(spark,'LOCAL').count()
    assert orders_count == 68884

@pytest.mark.transformation
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

@pytest.mark.skip('work in progress')
def test_read_config():
    config = get_app_config('LOCAL')
    print(type(config))
    assert config['orders.file.path'] == 'data/orders.csv'

@pytest.mark.transformation
def test_count_order_state(spark,expected_result):
    customers_df = read_customers(spark,'LOCAL')
    count_agg = count_orders_state(customers_df)
    assert count_agg.collect() == expected_result.collect()


@pytest.mark.parametrize(
        'status,count',
         [('CLOSED',7556),
          ("PENDING_PAYMENT",15030),
          ("COMPLETE",22900)]
)

def test_order_filter_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_df_count = filter_generic_orders(orders_df,status).count()
    assert filtered_df_count == count
