import pandas as pd
from datetime import timedelta

def main():

    binance22 = pd.read_csv('data/raw/extrato_binance_2022.csv')
    binance23 = pd.read_csv('data/raw/extrato_binance_2023.csv')
    binance = pd.concat([binance22,binance23], ignore_index=True)

    binance.drop(['User_ID', 'Remark'], axis=1, inplace=True)
    binance['UTC_Time'] = pd.to_datetime(binance['UTC_Time'])
    binance.sort_values('UTC_Time', inplace=True)

    mask = binance['UTC_Time'] == binance['UTC_Time'].shift() + timedelta(seconds=1)
    binance.loc[mask, 'UTC_Time'] -= timedelta(seconds=1)

    binance.to_csv('data/processed/extrato_binance.csv')

    return

if __name__=='__main__':
    main()
