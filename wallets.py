import pandas as pd

def import_data(path):
    """Import the data"""
    global binance
    binance = pd.read_csv(path, index_col=0)
    binance.sort_index(inplace=True)

# Preparing data
def cache_transaction(df, column):
    """
    Caches two column values from the previous register on the next, if the specified column is equal for both.

    Args:
        df (pandas.DataFrame): The DataFrame containing the transaction data.
        column (str): The name of the column to compare.

    Returns:
        None. The function adds two new columns to the DataFrame: 'Ticker_cache' and 'Value_cache'.
        'Ticker_cache' contains the previous value of the 'Coin' column if the value in the specified column matches the previous value.
        'Value_cache' contains the amount entering or leaving from the previous row if the value in the specified column matches the previous value.
    """

    df['Ticker_cache'] = df['Coin'].shift() * (df[column] == df[column].shift())
    df['Value_cache'] = df['Change'].shift() * (df[column] == df[column].shift()) 


def init_wallets():
    """Initialize the wallet and cold wallet.

    This function initializes two global variables `wallet` and `coldwallet`.
    `wallet` contains each coin related in a transaction from `binance` dataframe, 
    with balance and BRL_spent initialized to 0.
    `coldwallet` is a Pandas DataFrame that contains the coins withdrawal from binance, with amount and BRL_spent initialized to 0.0.

    Returns:
    None
    
    """
    # Guardando info do saldo na binance
    global wallet, coldwallet
    wallet = binance.groupby('Coin').agg({'UTC_Time': 'first'}).sort_values('UTC_Time') # Primeira ocorrência de cada moeda
    wallet['Balance'] = 0
    wallet['BRL_spent'] = 0

    # trezor
    index_names = list(binance[binance['Operation'].isin(['Fiat Withdraw', 'Withdraw'])]['Coin'].unique())
    data = {'Amount': [0.0]*len(index_names), 'BRL_spent': [0.0]*len(index_names)}
    coldwallet = pd.DataFrame(data, index=index_names)

      

def deposit(serie):
    """
    Add to your exchange the amount sent to your account.
    
    Args:
    serie (pandas.Series): A pandas series object containing the following fields:
        - Coin (str): The type of currency being deposited.
        - Change (float): The amount being deposited.
        
    If `Coin` is a fiat currency (e.g. BRL), the `Change` amount is simply added to the balance and the `BRL_spent` 
    field is also incremented.
    
    If `Coin` is a cryptocurrency, the function withdraws the corresponding amount from the coldwallet, calculates 
    its cost based on the amount previously spent on that currency in the coldwallet, and adds the resulting BRL value 
    to the balance and the `BRL_spent` field of the corresponding coin in the exchange wallet.
    
    Returns:
    None
    """

    coin = serie['Coin']
    change = serie['Change']


    if coin=='BRL':
        wallet.loc[coin,'Balance'] += change
        wallet.loc[coin,'BRL_spent'] += change
    else:

        brl_value = coldwallet.loc[serie['Coin'],'BRL_spent']/coldwallet.loc[serie['Coin'],'Amount']*serie['Change']
        wallet.loc[coin,'Balance'] += change
        wallet.loc[coin,'BRL_spent'] += brl_value
        coldwallet.loc[coin,'Amount'] -= change
        coldwallet.loc[coin,'BRL_spent'] -= brl_value
    

def buy(serie):
    """Update the wallet balances after a buy transaction.

    Args:
        serie (pandas.Series): A Pandas series representing the transaction data with the following fields:
            - 'Coin' (str): The ticker symbol of the coin being bought.
            - 'Change' (float): The amount of the coin being bought.
            - 'Ticker_cache' (str): The ticker symbol of the coin being sold (empty string if fiat).
            - 'Value_cache' (float): The amount of the coin being sold (0 if fiat).

    Returns:
        None.

    Updates the wallet balances based on the transaction data. If a fiat currency is involved, it simply updates the balance. 
    If both coins are cryptocurrency, it calculates the cost of the purchase using the corresponding balance and spent amount in the wallet,
    and adds the new coins to the wallet balance. The spent amount is also updated accordingly.

    """
    
    coin1 = serie['Coin']
    amount1 = serie['Change']
    coin2 = serie['Ticker_cache']
    amount2 = serie['Value_cache']

    if coin2 == '':
        return

    if coin1 == 'BRL':
        if amount1 < 0:
            wallet.loc[coin2, 'BRL_spent'] -= amount1
        else:
            ticker_cache_brl_spent = wallet.loc[coin2, 'BRL_spent']
            ticker_cache_balance = wallet.loc[coin2, 'Balance']
            brl_value = ticker_cache_brl_spent / ticker_cache_balance * amount2
            wallet.loc[coin2, 'BRL_spent'] += brl_value
    elif coin2 == 'BRL':
        if amount2 < 0:
            wallet.loc[coin1, 'BRL_spent'] -= amount2
        else:
            coin_brl_spent = wallet.loc[coin1, 'BRL_spent']
            coin_balance = wallet.loc[coin1, 'Balance']
            brl_value = coin_brl_spent / coin_balance * amount1
            wallet.loc[coin1, 'BRL_spent'] += brl_value
    else:
        print('No BRL involved in this transaction')

    wallet.loc[coin1, 'Balance'] += amount1
    wallet.loc[coin2, 'Balance'] += amount2
    wallet.loc['BRL', 'BRL_spent'] = wallet.loc['BRL', 'Balance']



def convert(serie):
    """Convert one cryptocurrency to another or to BRL.

    Args:
        serie (pandas.Series): A pandas series with four fields:
            - Coin (str): The code of the cryptocurrency being converted from.
            - Change (float): The amount being converted. Positive values indicate incoming currency and negative values indicate outgoing.
            - Ticker_cache (str): The code of the cryptocurrency being converted to.
            - Value_cache (float): The value being converted to.

    Returns:
        None

    The function converts one cryptocurrency to another or to BRL. If BRL is involved in the transaction, 
    the function delegates the conversion to the buy() function. Otherwise, the 
    BRL equivalent value is subtracted from the BRL_spent field of the outgoing cryptocurrency and 
    added to the BRL_spent field of the incoming cryptocurrency. The balance of the relevant 
    cryptocurrencies is also updated.

    """

    coin1 = serie['Coin']
    amount1 = serie['Change']
    coin2 = serie['Ticker_cache']
    amount2 = serie['Value_cache']


    if coin2=='': 
        return
    
    if coin1 == 'BRL' or coin2 == 'BRL':
        buy(serie)
        return
 
    if amount1<0: # means that serie['Coin'] is leaving; serie['Change'] is negative
        # serie['Coin'] BRL equivalent leaving
        temp = wallet.loc[coin1,'BRL_spent'] / wallet.loc[coin1,'Balance'] * amount1 # aways negative
        
    else: # means that serie['Ticker_cache'] is leaving, serie['Value_cache'] is negative
        temp = -wallet.loc[coin2,'BRL_spent'] / wallet.loc[coin2,'Balance'] * amount2 # temp goes positive here


    # update BRL_spent
    wallet.loc[coin2, 'BRL_spent'] -= temp # tira se Ticker_cache for saída
    wallet.loc[coin1, 'BRL_spent'] += temp # temp é negativa aqui; menos com mais - 

    # update balance
    wallet.loc[coin1,'Balance'] += amount1 
    wallet.loc[coin2,'Balance'] += amount2 



def withdraw(serie):
    """Process a Pandas series representing a Binance withdrawal operation.

    This function takes a Pandas series representing a Binance withdrawal operation and updates the corresponding wallet 
    and coldwallet dataframes with the withdrawn amount. The amount is subtracted from the coldwallet and added to the 
    wallet's balance. If the withdrawn coin is BRL, the BRL_spent becomes redundant, but the function update it as well.

    Parameters:
    serie (pandas.Series): A Pandas series representing a Binance withdrawal operation.

    Returns:
    None
    """

    coin = serie['Coin']
    amount = serie['Change']

    coldwallet.loc[coin, 'Amount'] -= amount # add amount to coldwallet

    if coin=='BRL': # BRL spent not necessary
        coldwallet.loc[coin, 'BRL_spent'] -= amount # eliminate this need
        wallet.loc[coin,'BRL_spent'] += amount

    else: # BRL spent necessary
        brl_value = wallet.loc[coin,'BRL_spent'] / wallet.loc[coin,'Balance'] * amount
        coldwallet.loc[coin, 'BRL_spent'] -=  brl_value
        wallet.loc[coin,'BRL_spent'] += brl_value

    wallet.loc[coin,'Balance'] += amount



def change_amount(serie):
    '''Change the amount without alter cost '''
    wallet.loc[serie['Coin'],'Balance'] += serie['Change']


def multiple_BNB_trades(serie):
    """Process a Pandas series representing a Binance operation involving multiple trades with BNB.

    Binance offers an operation that convert at the same time several coins with small balance to BNB. 
    These operation results in several registers in dataframe, all are covered in this function.
    This fuction recieves a pandas series and lookup for it's `Coin` value
    If the coin is BNB, it increases the amount in `wallet`. 
    If it is not, this function liquidates the coin and adds the BRL_spent to BNB in wallet.

    Parameters:
    serie (pandas.Series): A Pandas series representing a Binance operation involving multiple trades with BNB.

    Returns:
    None
    """

    coin = serie['Coin']
    amount = serie['Change']

    if coin=='BNB':
        wallet.loc['BNB', 'Balance'] += amount

    else:
        wallet.loc[coin, 'Balance'] += amount
        wallet.loc['BNB', 'BRL_spent'] += wallet.loc[coin, 'BRL_spent']
        wallet.loc[coin, 'BRL_spent'] = 0


def triagem(serie):
    """Process a Pandas series representing a Binance operation.

    This function takes a Pandas series representing a Binance operation and calls a corresponding function based on the 
    type of the operation. The function to be called is determined using a dictionary lookup.
    
    Parameters:
    serie (pandas.Series): A Pandas series representing a Binance operation.
    
    Returns:
    None
    """


    operation = serie['Operation']
    
    actions = {
        'Deposit': deposit,
        'Transaction Related': buy,
        'Binance Convert': convert,
        'Fiat Withdraw': withdraw,
        'Staking Purchase': change_amount,
        'Launchpool Interest': change_amount,
        'Simple Earn Flexible Interest': change_amount,
        'Staking Rewards': change_amount,
        'Staking Redemption': change_amount,
        'Cash Voucher Distribution': change_amount,
        'Withdraw': withdraw,
        'Small Assets Exchange BNB': multiple_BNB_trades,
        'Small Assets Exchange BNB (Spot)': multiple_BNB_trades,
        'Distribution': change_amount,
        'Simple Earn Flexible Subscription': lambda x: None,
        'Savings Distribution': lambda x: None,
        'Simple Earn Flexible Redemption': lambda x: None,
        'Crypto Box': lambda x: None,
        'Airdrop Assets': change_amount}
    
    action = actions.get(operation)
    
    if action is None:
        print(serie['Operation'])
        raise ValueError('Operation not recognized')
    
    action(serie)


def treat_wallets():
    """Consolidates the wallet dataframe and computes the mean price of each coin.

    This function consolidates the wallet dataframe by filtering out coins with a balance less than 0.00000001 and 
    computing the mean price of each coin. The mean price is computed as the BRL_spent divided by the Balance for the 
    coins in the wallet dataframe and the BRL_spent divided by the Amount for the coins in the coldwallet dataframe. 
    The resulting consolidated dataframe is stored in the global variable `summarized`.

    Parameters:
    None

    Returns:
    None
    """

    global summarized

    summarized = wallet[wallet['Balance']>=0.00000001].copy()
    summarized['Mean_price'] = summarized['BRL_spent']/summarized['Balance']
    coldwallet['Mean_price'] = coldwallet['BRL_spent']/coldwallet['Amount']

def save_wallets(path):
    """
    Save the summarized and coldwallet dataframes to CSV files in the 'data/final/' directory with the filenames 'saldo_binance.csv' and 'saldo_trezor.csv', respectively.

    Args:
    None

    Returns:
    None
    """
    summarized.to_csv(path + 'saldo_binance.csv')
    coldwallet.to_csv(path + 'saldo_trezor.csv')
    wallet.to_csv(path + 'binance_dates.csv')

def main():
    import_data('data/processed/extrato_binance.csv')
    cache_transaction(binance, 'UTC_Time')
    init_wallets()
    binance.apply(triagem, axis=1) 
    treat_wallets()
    save_wallets('data/final/')

    return


if __name__=='__main__':
    main()


