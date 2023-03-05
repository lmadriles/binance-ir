import pandas as pd
import numpy as np

# treating the data
binance = pd.read_csv('data/processed/extrato_binance.csv', index_col=0)
binance.sort_index(inplace=True)
binance['UTC_Time'] = pd.to_datetime(binance['UTC_Time'])

pd.set_option('display.float_format', lambda x: '%.8f' % x) # change the visualization of a float to eight decimal digits.


# fix one register in a dumb form 
binance.loc[57,'UTC_Time'] = binance.loc[56,'UTC_Time']



# Preparing data
def cache_transaction(df, column):
    df['Ticker_cache'] = df['Coin'].shift() * (df[column] == df[column].shift())
    df['Value_cache'] = df['Change'].shift() * (df[column] == df[column].shift())
    return 
cache_transaction(binance, 'UTC_Time')

# Guardando info do saldo na binance
wallet = binance.groupby('Coin').agg({'UTC_Time': 'first'}).sort_values('UTC_Time') # Primeira ocorrência de cada moeda
wallet['Balance'] = 0
wallet['BRL_spent'] = 0

# trezor
index_names = list(binance[binance['Operation'].isin(['Fiat Withdraw', 'Withdraw'])]['Coin'].unique())
data = {'Amount': [0.0]*len(index_names), 'BRL_spent': [0.0]*len(index_names)}
coldwallet = pd.DataFrame(data, index=index_names)


# function to be aplied to the dataframe.

# put the decorator here


def triagem(serie):

    if serie['Operation']=='Deposit':
        deposit(serie)
    elif serie['Operation']=='Transaction Related':
        buy(serie)
    elif serie['Operation']=='Large OTC Trading':
        convert(serie)
    elif serie['Operation']=='Fiat Withdraw':
        withdraw(serie)
    elif serie['Operation']=='Simple Earn Flexible Subscription': 
        pass # do nothing
    elif serie['Operation']=='Savings Distribution': # do nothing, entrada de LDticker no stake; par de 'Simple Earn Flexible Subscription'
        pass # do nothing
    elif serie['Operation']=='Staking Purchase': # amount goes from spot to earn;  
        change_amount(serie)
    elif serie['Operation']=='Launchpool Interest':
        change_amount(serie)
    elif serie['Operation']=='Simple Earn Flexible Interest':
        change_amount(serie)
    elif serie['Operation']=='Staking Rewards':
        change_amount(serie)
    elif serie['Operation']=='Simple Earn Flexible Redemption':
        pass # do nothing; saida de LDticker no stake
    elif serie['Operation']=='Staking Redemption': # amount goes from spot to earn; carries losses
        change_amount(serie)
    elif serie['Operation']=='Cash Voucher Distribution':
        change_amount(serie)
    elif serie['Operation']=='Withdraw':
        withdraw(serie)
    elif serie['Operation']=='Small Assets Exchange BNB':
        multiple_BNB_trades(serie)
    elif serie['Operation']=='Distribution':
        change_amount(serie)
    else:
        raise ValueError('Operation not recognized') 

        
    if wallet.loc['BRL', 'Balance'] != wallet.loc['BRL', 'BRL_spent']:
        print(wallet.loc['BRL'])

    return
    

def deposit(serie):
    wallet.loc[serie['Coin'],'Balance'] += serie['Change'] # add to wallet
    wallet.loc[serie['Coin'],'BRL_spent'] += serie['Change']
    return

def buy(serie):

    if serie['Ticker_cache']=='': 
        return

    if serie['Coin']=='BRL': 

        if serie['Change']<0: 
            wallet.loc[serie['Ticker_cache'],'BRL_spent'] -= serie['Change'] 
        else: 
            wallet.loc[serie['Ticker_cache'],'BRL_spent'] += wallet.loc[serie['Ticker_cache'],'BRL_spent'] / wallet.loc[serie['Ticker_cache'],'Balance'] * serie['Value_cache'] 

    elif serie['Ticker_cache']=='BRL':

        if serie['Value_cache']<0: 
            wallet.loc[serie['Coin'],'BRL_spent'] -= serie['Value_cache']
        else:
            wallet.loc[serie['Coin'],'BRL_spent'] += wallet.loc[serie['Coin'],'BRL_spent'] / wallet.loc[serie['Coin'],'Balance'] * serie['Change']
        
    else:
        print('No BRL involved in this transaction')
    
    # update balance
    wallet.loc[serie['Coin'],'Balance'] += serie['Change'] 
    wallet.loc[serie['Ticker_cache'],'Balance'] += serie['Value_cache'] 
    wallet.loc['BRL', 'BRL_spent'] = wallet.loc['BRL', 'Balance']

    return

def convert(serie):

    if serie['Ticker_cache']=='': 
        return
    
 
    if serie['Change']<0: # means that serie['Coin'] is leaving; serie['Change'] is negative
        # serie['Coin'] BRL equivalent leaving
        temp = wallet.loc[serie['Coin'],'BRL_spent'] / wallet.loc[serie['Coin'],'Balance'] * serie['Change'] # aways negative
        
    else: # means that serie['Ticker_cache'] is leaving, serie['Value_cache'] is negative
        temp = -wallet.loc[serie['Ticker_cache'],'BRL_spent'] / wallet.loc[serie['Ticker_cache'],'Balance'] * serie['Value_cache'] # temp goes positive here


    # update BRL_spent
    wallet.loc[serie['Ticker_cache'], 'BRL_spent'] -= temp # tira se Ticker_cache for saída
    wallet.loc[serie['Coin'], 'BRL_spent'] += temp # temp é negativa aqui; menos com mais - 

    # update balance
    wallet.loc[serie['Coin'],'Balance'] += serie['Change'] 
    wallet.loc[serie['Ticker_cache'],'Balance'] += serie['Value_cache'] 



def withdraw(serie):
    if serie['Coin']=='BRL': # test to avoid mean price (not needed, but saves computing time (worth it?))
        coldwallet.loc['BRL', 'Amount'] -= serie['Change'] # serie['Change'] is negative
        coldwallet.loc['BRL', 'BRL_spent'] -= serie['Change'] # eliminate this need
        wallet.loc['BRL','Balance'] += serie['Change']  # eliminate this need
        wallet.loc['BRL','BRL_spent'] += serie['Change']

    else:
        coldwallet.loc[serie['Coin'], 'Amount'] -= serie['Change']
        temp = wallet.loc[serie['Coin'],'BRL_spent'] / wallet.loc[serie['Coin'],'Balance'] * serie['Change']
        coldwallet.loc[serie['Coin'], 'BRL_spent'] -=  temp
        wallet.loc[serie['Coin'],'Balance'] += serie['Change']
        wallet.loc[serie['Coin'],'BRL_spent'] += temp


def change_amount(serie):
    '''Change the amount without alter cost '''
    wallet.loc[serie['Coin'],'Balance'] += serie['Change']


def multiple_BNB_trades(serie):
    if serie['Coin']=='BNB':
        wallet.loc['BNB', 'Balance'] += serie['Change']

    else:
        wallet.loc[serie['Coin'], 'Balance'] += serie['Change']
        wallet.loc['BNB', 'BRL_spent'] += wallet.loc[serie['Coin'], 'BRL_spent']
        wallet.loc[serie['Coin'], 'BRL_spent'] = 0

def main():
    binance.apply(triagem, axis=1)

    return


if __name__ == "__main__":
    main()
