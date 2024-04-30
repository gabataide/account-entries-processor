import argparse

from jobs.account_entries import process_balances_and_withdraws


def main():
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument('-b', '--balances', default='src/jobs/account_entries/resources/balances.csv', help='Balances CSV input filepath')
    args_parser.add_argument('-w', '--withdraws', default='src/jobs/account_entries/resources/withdraws.csv', help='Withdraws CSV input filepath')

    args = args_parser.parse_args()

    process_balances_and_withdraws(args.balances, args.withdraws)

if __name__ == '__main__':
    main()
