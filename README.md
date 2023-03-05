# binance-ir

This is a Python-based project that allows you to calculate the mean price of cryptocurrencies based on their transaction history downloaded from binance site. The project also supports the calculation of mean prices even in the presence of withdrawals to external wallets.

## Dependencies
The project depends on the following Python libraries:

- pandas
- numpy


You can easily install these dependencies using pip by running the following command:

pip install pandas numpy


## How to use
To use the project, you need to first download the transaction history on your binance account, currently from https://www.binance.com/pt-BR/my/wallet/history/deposit-crypto. Once you have the transaction history, save it as a CSV file:


The dataclean.py algorithm, will format the database to work with the algo binance_IR.py.

## Future improvements
The algo works at the moment with fiat depositis in Brazilian Real (BRL). To be adjusted to other fiat deposits.
Some transactions that need to be simultaneous are presenting a lag of one second, generating a bug. Needs to be fixed.

## License
This project is licensed under the MIT License. Feel free to use and modify the code for your own purposes.

