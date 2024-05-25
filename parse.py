import csv
from datetime import datetime
import os
import re

import pandas as pd


BROKERAGES = {
    "schwab": {
        "columns": {
            "date": "Date",
            "action": "Action",
            "symbol": "Symbol",
            "quantity": "Quantity",
            "amount": "Amount",
            "fees": "Fees & Comm",
            "description": "Description"
        },
        "actions": {
            "open": ["Buy", "Reinvest Shares", "Security Transfer"],
            "close": ["Sell", "Security Transfer"]
        }
    },
    "fidelity": {
        "columns": {
            "date": "Run Date",
            "action": "Action",
            "symbol": "Symbol",
            "quantity": "Quantity",
            "amount": "Amount ($)",
            "fees": "Fees ($)",
            "description": "Description"
        },
        "actions": {
            "open": ["BOUGHT", "REINVESTMENT"],
            "close": ["SOLD"]
        }
    }
}


def determine_brokerage(file_path: str, brokerage_mapping: dict) -> str:
        with open(file_path, 'r') as file:
            file_content = file.read().lower()

        for brokerage in brokerage_mapping.keys():
            if brokerage in file_path.lower() or brokerage in file_content:
                return brokerage

        raise ValueError(f'Brokerage not supported: {file_path}')


def amount_to_float(amount: str) -> float:
    if amount == '':
        return 0.0
    return float(re.sub(r'[\$,]', '', amount))


def get_trade_actions(brokerage_mapping) -> list:
    actions = []
    for brokerage in brokerage_mapping.values():
        for action_list in brokerage['actions'].values():
            actions.extend([action.lower() for action in action_list])
    return actions


def get_trade_open_actions(brokerage_mapping) -> list:
    actions = []
    for brokerage in brokerage_mapping.values():
        actions.extend([action.lower() for action in brokerage['actions']['open']])
    return actions


def get_trade_close_actions(brokerage_mapping) -> list:
    actions = []
    for brokerage in brokerage_mapping.values():
        actions.extend([action.lower() for action in brokerage['actions']['close']])
    return actions


def list_csvs(dir):
        return [os.path.join(root, file) for root, dirs, files in os.walk(dir) for file in files if file.endswith('.csv')]


def parse_csv_lines(filepath: str) -> list:
    with open(filepath, 'r') as file:
        lines = []
        for line in csv.reader(file):
            line = [re.sub(r'[,]', '', f.strip()) for f in line]
            line = ','.join(line)
            line = line.strip()

            # Skip empty lines and lines without commas
            if line == '' or ',' not in line:
                continue
            if re.match(r'^".*"$', line):
                continue

            lines.append(line)
    return lines


def parse_trade_lines(lines: list, brokerage_mapping: dict) -> list:
    trades = []
    for line in lines:
        if len(trades) != 0:
            line = line.lower()
        if len(trades) != 0 and not any(action in line for action in get_trade_actions(brokerage_mapping)):
            continue
        trades.append(line)
    return trades



def handle_special_cases(trades_df: pd.DataFrame) -> pd.DataFrame:
    # Special cases for symbol changes
    trades_df.loc[trades_df['symbol'] == 'DISCA', 'symbol'] = 'WBD'
    trades_df.loc[trades_df['symbol'] == 'FB', 'symbol'] = 'META'

    # Special cases for stock splits
    trades_df.loc[(trades_df['symbol'] == 'AMZN') & (trades_df['date'] < '2022-06-06'), 'quantity'] *= 20
    trades_df.loc[(trades_df['symbol'] == 'GOOG') & (trades_df['date'] < '2022-07-15'), 'quantity'] *= 20
    trades_df.loc[(trades_df['symbol'] == 'GOOGL') & (trades_df['date'] < '2022-07-15'), 'quantity'] *= 20
    trades_df.loc[(trades_df['symbol'] == 'TQQQ') & (trades_df['date'] < '2022-01-13'), 'quantity'] /= 2

    return trades_df


def parse_trades(filepath: str, brokerage_mapping: dict) -> pd.DataFrame:
    brokerage = determine_brokerage(filepath, brokerage_mapping)
    lines = parse_trade_lines(parse_csv_lines(filepath), brokerage_mapping)
    lines_df = pd.DataFrame([line.split(',') for line in lines[1:]], columns=lines[0].split(','))

    trades = []
    for i, row in lines_df.iterrows():
        # Convert quantity, amount, and fees to float
        quantity = amount_to_float(row[brokerage_mapping[brokerage]['columns']['quantity']])
        amount = amount_to_float(row[brokerage_mapping[brokerage]['columns']['amount']])
        fees = amount_to_float(row[brokerage_mapping[brokerage]['columns']['fees']])

        # Convert date to ISO 8601 format
        date = row[brokerage_mapping[brokerage]['columns']['date']]
        if brokerage == 'schwab':
            date = datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')
        elif brokerage == 'fidelity':
            date = datetime.strptime(date, '%b-%d-%Y').strftime('%Y-%m-%d')

        # Determine action
        action = row[brokerage_mapping[brokerage]['columns']['action']]

        # Special case for Fidelity reinvestments, treat negative quantities as a close
        if brokerage == 'fidelity' and 'reinvestment' in action and quantity < 0:
            action = 'close'

        # Special case for Schwab security transfers
        if brokerage == 'schwab' and 'security transfer' in action:
            action = 'open' if quantity > 0 else 'close'

        if any(a in action for a in get_trade_open_actions(brokerage_mapping)):
            action = 'open'
        elif any(a in action for a in get_trade_close_actions(brokerage_mapping)):
            action = 'close'

        # Handle symbol changes
        symbol = row[brokerage_mapping[brokerage]['columns']['symbol']].upper()
        if symbol == '':
            symbol = 'UNKNOWN'

        description = row[brokerage_mapping[brokerage]['columns']['description']]
        trades.append([date, action, symbol, abs(quantity), abs(amount), abs(fees), description])

    trades_df = pd.DataFrame(trades, columns=['date', 'action', 'symbol', 'quantity', 'amount', 'fees', 'description'])
    trades_df = handle_special_cases(trades_df)
    return trades_df


def save_trades(trades_df: pd.DataFrame, filepath: str = 'trades.csv') -> None:
    trades_df.to_csv(filepath, index=False)


def calculate_holdings(trades_df: pd.DataFrame) -> pd.DataFrame:
    open_df = trades_df[trades_df['action'] == 'open']
    close_df = trades_df[trades_df['action'] == 'close']
    open_quantities = open_df.groupby('symbol')['quantity'].sum()
    close_quantities = close_df.groupby('symbol')['quantity'].sum()
    holdings = open_quantities.subtract(close_quantities, fill_value=0)

    # Drop zero quantity holdings
    holdings = holdings[holdings != 0]

    # Make the series a dataframe
    holdings = holdings.reset_index()
    holdings.columns = ['symbol', 'quantity']

    return holdings


def calculate_pl(trades_df: pd.DataFrame) -> pd.DataFrame:
    total_open_amount = trades_df[trades_df['action'] == 'open']['amount'].sum()
    total_close_amount = trades_df[trades_df['action'] == 'close']['amount'].sum()
    pl = total_close_amount - total_open_amount
    return pl


if __name__ == '__main__':
    DIRECTORY = 'exports/'
    filepaths = list_csvs(DIRECTORY)
    trades = pd.DataFrame(columns=['date', 'action', 'symbol', 'quantity', 'amount', 'fees', 'description'])
    for filepath in filepaths:
        trades = pd.concat([trades, parse_trades(filepath, BROKERAGES)])
    save_trades(trades)
    print('Saved trades to trades.csv')

    print('Your current holdings are:')
    holdings = calculate_holdings(trades)
    print(holdings)
