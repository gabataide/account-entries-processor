from pyspark.testing import assertDataFrameEqual
from src.jobs.account_entries import (
    BalanceStatus,
    calculate_remaining_balances_based_on_next_orders
)


def test_calculate_remaining_balances_based_on_next_orders(spark):
    input_balances_df = spark.createDataFrame(
        data=[
            (1, 1, 1200, BalanceStatus.ACTIVE.value),
            (1, 2, 250, BalanceStatus.ACTIVE.value),
            (1, 3, 5780, BalanceStatus.ACTIVE.value),
            (2, 1, 6951, BalanceStatus.ACTIVE.value),
            (2, 2, 635, BalanceStatus.ACTIVE.value),
            (3, 1, 9800, BalanceStatus.ACTIVE.value)
        ],
        schema=['account_id', 'balance_order', 'available_balance', 'status']
    )

    expected_balances_df = spark.createDataFrame(
        data=[
            (1, 1, 1200, BalanceStatus.ACTIVE.value, 6030),
            (1, 2, 250, BalanceStatus.ACTIVE.value, 5780),
            (1, 3, 5780, BalanceStatus.ACTIVE.value, 0),
            (2, 1, 6951, BalanceStatus.ACTIVE.value, 635),
            (2, 2, 635, BalanceStatus.ACTIVE.value, 0),
            (3, 1, 9800, BalanceStatus.ACTIVE.value, 0)
        ],
        schema=['account_id', 'balance_order', 'available_balance', 'status', 'remaining_balance']
    )

    balances_with_calculate_remaining_df = calculate_remaining_balances_based_on_next_orders(input_balances_df)

    assertDataFrameEqual(expected_balances_df, balances_with_calculate_remaining_df)
