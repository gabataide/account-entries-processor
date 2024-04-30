from common import (
    create_dataframe_from_csv,
    create_spark_session
)
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Any, Dict, List, Optional


class BalanceStatus(Enum):
  ACTIVE = 'ACTIVE'
  BALANCE_WITHDREW = 'BALANCE WITHDREW'


WITHDRAW_SUCCESSFUL_MESSAGE = 'withdraw_order {withdraw_order} successful ({balance_withdrew_amount} of {total_withdraw_amount} of total withdraw amount)'
WITHDRAW_UNSUCCESSFUL_MESSAGE = 'withdraw_order {withdraw_order} unsuccessful ({total_withdraw_amount} is greater than account available balance)'


def process_balances_and_withdraws(balances_filepath: str, withdraws_filepath: str):
    """Produce a CSV output file contaning balance entries after processing withdraw entries for each account"""

    try:
        print('process_balances_and_withdraws job started...')

        spark_session = create_spark_session('account_entries')

        balances_df = create_dataframe_from_csv(spark_session, balances_filepath)
        withdraws_df = create_dataframe_from_csv(spark_session, withdraws_filepath)

        balances_df = calculate_remaining_balances_based_on_next_orders(balances_df)

        balances_df = balances_df\
        .withColumnRenamed('available_balance', 'initial_balance')\
        .withColumn('available_balance', F.col('initial_balance'))

        unique_account_ids = [x.account_id for x in balances_df.select('account_id').distinct().collect()]
        processed_account_balances = []
        
        for account_id in unique_account_ids:
            balances_by_account = balances_df.filter(F.col('account_id') == account_id)
            withdraws_by_account = withdraws_df.filter(F.col('account_id') == account_id)
            balances_by_account.show()
            withdraws_by_account.show()

            processed_account_balances.extend(
                process_account_entries(balances_by_account, withdraws_by_account)
            )

        result_df = spark_session.createDataFrame(processed_account_balances)\
            .select('account_id', 'balance_order', 'initial_balance', 'available_balance', 'status', 'validation_result')\
            .orderBy(['account_id', 'balance_order'], ascending=True)

        result_df.coalesce(1).write\
            .option('header', True)\
            .mode('overwrite')\
            .csv('src/jobs/account_entries/result/')
    except Exception as err:
        print(f"Exception occurred when executing process_balances_and_withdraws job: {type(err).__name__} - {err}")
        raise err


def process_account_entries(balances_df: DataFrame, withdraws_df: DataFrame) -> List[Dict[str, Any]]:
    """Subtract withdraws' amount from balance entries considering balance order"""

    try:
        balances_len = balances_df.count()

        processed_account_balances = []

        current_balance_order = 0
        current_withdraw_order = 0

        current_balance_validation = []

        next_balance = True
        next_withdraw = True
        done = False

        while not done:
            if next_balance:
                current_balance_order += 1
                current_balance = get_account_entry_by_order_number(balances_df, 'balance_order', current_balance_order)
                current_balance_validation = []

            if next_withdraw:
                current_withdraw_order += 1
                current_withdraw = get_account_entry_by_order_number(withdraws_df, 'withdraw_order', current_withdraw_order)
                withdraw_amount_reamining = current_withdraw['withdraw_amount'] if current_withdraw else 0

            if not current_balance:
                break

            if not current_withdraw:
                processed_account_balances.append(
                    create_processed_balance_entry(current_balance, format_balance_validation(current_balance_validation))
                )

                next_balance = True
                next_withdraw = False

                continue

            if current_balance['available_balance'] >= withdraw_amount_reamining:
                current_balance_validation.append(
                    WITHDRAW_SUCCESSFUL_MESSAGE.format(
                        withdraw_order=current_withdraw['withdraw_order'], 
                        balance_withdrew_amount=withdraw_amount_reamining,
                        total_withdraw_amount=current_withdraw['withdraw_amount'])
                )

                current_balance['available_balance'] -= withdraw_amount_reamining
                next_withdraw = True

                next_balance = current_balance['available_balance'] == 0
                if next_balance:
                    processed_account_balances.append(
                        create_processed_balance_entry(current_balance, format_balance_validation(current_balance_validation))
                    )

            elif current_balance['available_balance'] + current_balance['remaining_balance'] >= withdraw_amount_reamining:
                withdraw_amount_reamining -= current_balance['available_balance']
                
                current_balance_validation.append(
                    WITHDRAW_SUCCESSFUL_MESSAGE.format(
                        withdraw_order=current_withdraw['withdraw_order'], 
                        balance_withdrew_amount=current_balance['available_balance'],
                        total_withdraw_amount=current_withdraw['withdraw_amount']
                    )
                )

                current_balance['available_balance'] = 0

                processed_account_balances.append(
                    create_processed_balance_entry(current_balance, format_balance_validation(current_balance_validation))
                )

                next_withdraw = False
                next_balance = True
            else:
                current_balance_validation.append(
                    WITHDRAW_UNSUCCESSFUL_MESSAGE.format(
                        withdraw_order=current_withdraw['withdraw_order'],
                        total_withdraw_amount=withdraw_amount_reamining
                    )
                )

                next_withdraw = True
                next_balance = False

            done = current_balance_order > balances_len

        return processed_account_balances
    except Exception as err:
        print(f"Exception occurred when processing account entries (balances and withdraws): {type(err).__name__} - {err}")
        raise err


def calculate_remaining_balances_based_on_next_orders(balances_df: DataFrame) -> DataFrame:
    """Calculate remaining balance considering the next balance entries' available_balance value for each account's balance_order"""

    try:
        bal_window = Window.partitionBy('account_id').orderBy('balance_order').rowsBetween(1, Window.unboundedFollowing)

        return balances_df\
            .withColumn('remaining_balance', F.sum('available_balance').over(bal_window))\
            .na.fill(0)
    except Exception as err:
        print(f"Exception occurred when calculating remaining balance based on next orders: {type(err).__name__} - {err}")
        raise err


def create_processed_balance_entry(current_balance: Dict[str, Any], balance_validation: str) -> Dict[str, Any]:
    """Create an dict as result of account balance entry processing"""

    try:
        return {
            'account_id': current_balance['account_id'],
            'balance_order': current_balance['balance_order'],
            'initial_balance': current_balance['initial_balance'],
            'available_balance': current_balance['available_balance'],
            'status': BalanceStatus.ACTIVE.value if current_balance['available_balance'] > 0 else BalanceStatus.BALANCE_WITHDREW.value,
            'validation_result': balance_validation
        }
    except Exception as err:
        print(f"Exception occurred when creating processed balance entry: {type(err).__name__} - {err}")
        raise err


def format_balance_validation(current_balance_validation: List[str]) -> str:
    """Format balance validation as a text describing whether withdraws were processed or not"""

    try:
        return '\n'.join(current_balance_validation) if current_balance_validation else 'No withdraw_order to process'
    except Exception as err:
        print(f"Exception occurred when formatting balance validation: {type(err).__name__} - {err}")
        raise err


def get_account_entry_by_order_number(account_entries_df: DataFrame, col_name: str, order_number: int) -> Optional[Dict[str, Any]]:
    """Get account entry from account dataframe by entry's order number. Returns it as dict if it exists, otherwise returns None"""

    try:
        account_entry = account_entries_df.filter(F.col(col_name) == order_number).collect()
        return account_entry[0].asDict() if account_entry else None
    except Exception as err:
        print(f"Exception occurred when getting entry from account dataframe by order number: {type(err).__name__} - {err}")
        raise err
