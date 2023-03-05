import pandas as pd

def main():

    # O extrato da binance vem como um csv com aspas duplas entre os registros. Tiramos aqui.
    with open('data/raw/extrato_binance_2022.csv', "r+", encoding="utf-8") as csv_file:
        content = csv_file.read()

    with open('data/raw/extrato_binance_2022.csv', "w+", encoding="utf-8") as csv_file:
        csv_file.write(content.replace('"', ''))


    with open('data/raw/extrato_binance_2023.csv', "r+", encoding="utf-8") as csv_file:
        content = csv_file.read()

    with open('data/raw/extrato_binance_2023.csv', "w+", encoding="utf-8") as csv_file:
        csv_file.write(content.replace('"', ''))


    binance22 = pd.read_csv('data/raw/extrato_binance_2022.csv')
    binance23 = pd.read_csv('data/raw/extrato_binance_2023.csv')
    binance = pd.concat([binance22,binance23], ignore_index=True)

    binance.drop(['User_ID', 'Remark'], axis=1, inplace=True)
    binance['UTC_Time'] = pd.to_datetime(binance['UTC_Time'])
    binance.sort_values('UTC_Time', inplace=True)

    binance.to_csv('data/processed/extrato_binance.csv')

    return

if __name__=='__main__':
    main()
