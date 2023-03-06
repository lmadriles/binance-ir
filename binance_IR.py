import pandas as pd


def cache_transaction(df, column):
    df['Ticker_cache'] = df['Coin'].shift() * (df[column] == df[column].shift())
    df['Value_cache'] = df['Change'].shift() * (df[column] == df[column].shift())
    return 

def triagem(serie):
    operation = serie['Operation']
    
    actions = {
        'Deposit': deposit,
        'Transaction Related': buy,
        'Large OTC Trading': convert,
        'Fiat Withdraw': withdraw,
        'Staking Purchase': change_amount,
        'Launchpool Interest': change_amount,
        'Simple Earn Flexible Interest': change_amount,
        'Staking Rewards': change_amount,
        'Staking Redemption': change_amount,
        'Cash Voucher Distribution': change_amount,
        'Withdraw': withdraw,
        'Small Assets Exchange BNB': multiple_BNB_trades,
        'Distribution': change_amount,
        'Simple Earn Flexible Subscription': lambda x: None,
        'Savings Distribution': lambda x: None,
        'Simple Earn Flexible Redemption': lambda x: None
    }
    
    action = actions.get(operation)
    
    if action is None:
        raise ValueError('Operation not recognized')
    
    action(serie)

       

def deposit(serie):
    if serie['Coin']=='BRL':
        wallet.loc[serie['Coin'],'Balance'] += serie['Change']
        wallet.loc[serie['Coin'],'BRL_spent'] += serie['Change']
    else:
        BRL_value = coldwallet.loc[serie['Coin'],'BRL_spent']/coldwallet.loc[serie['Coin'],'Amount']*serie['Change']
        wallet.loc[serie['Coin'],'Balance'] += serie['Change']
        wallet.loc[serie['Coin'],'BRL_spent'] += BRL_value
        coldwallet.loc[serie['Coin'],'Amount'] -= serie['Change']
        coldwallet.loc[serie['Coin'],'BRL_spent'] -= BRL_value
    

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
    
    if serie['Coin'] == 'BRL' or serie['Ticker_cache'] == 'BRL':
        buy(serie)
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
    # treating the data
    binance = pd.read_csv('data/processed/extrato_binance.csv', index_col=0)
    binance.sort_index(inplace=True)
    binance['UTC_Time'] = pd.to_datetime(binance['UTC_Time'])

    # fix one register hardcode
    binance.loc[57,'UTC_Time'] = binance.loc[56,'UTC_Time']

    # Preparing data

    cache_transaction(binance, 'UTC_Time')

    # Guardando info do saldo na binance
    wallet = binance.groupby('Coin').agg({'UTC_Time': 'first'}).sort_values('UTC_Time') # Primeira ocorrência de cada moeda
    wallet['Balance'] = 0
    wallet['BRL_spent'] = 0

    # trezor
    index_names = list(binance[binance['Operation'].isin(['Fiat Withdraw', 'Withdraw'])]['Coin'].unique())
    data = {'Amount': [0.0]*len(index_names), 'BRL_spent': [0.0]*len(index_names)}
    coldwallet = pd.DataFrame(data, index=index_names)

    binance.apply(triagem, axis=1) 
    return


if __name__=='__main__':
    main()
