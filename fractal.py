# %% fractal.py is a python script that ranks fractal energy of differenct assets based based from Doc Severson fractal energy trading strategy.

import pandas as pd
import os
import sqlalchemy
import pandas_ta as ta
from PSEconnect import PSEconnect
from datetime import datetime, timedelta
from jutsu import mansfield_rs


# %% import price data from database
engine = sqlalchemy.create_engine('postgresql://{0}:{1}@{2}:{3}/trading'.format(os.environ.get('db_user'), os.environ.get('db_password'),
                                                                                os.environ.get('db_host'), os.environ.get('db_port')))
# %%


def fractalenergy_stocks(interval='W', energy_level=54, position='long'):

    ticker_list = PSEconnect().pse_stocks_info['ticker'].to_list()
    fractal_table = []

    for ticker in ticker_list:
        data = PSEconnect().stockprice(ticker, interval)
        # Check ticker if it have sufficient market data
        if data.empty is not True:
            base_date = datetime.now() - timedelta(30)
            sector = PSEconnect().pse_stocks_info.set_index(
                'ticker').loc[ticker, 'sector']
            subsector = PSEconnect().pse_stocks_info.set_index(
                'ticker').loc[ticker, 'subsector']

            rs = mansfield_rs(data, timeframe=interval).values[-1]

            # Check ticker if not suspended
            if base_date < data.index[-1]:
                if len(data) < 15:
                    fractal = 0
                else:
                    fractal = data.tail(15).ta.chop(
                        atr_length=1).dropna().values
                if rs >= 0:
                    trend = 'uptrend'
                elif rs < 0:
                    trend = 'downtrend'
                else:
                    trend = 'insufficient data'

                data = ({'ticker': ticker,
                         'CHOP': float(fractal),
                         'sector': sector,
                         'subsector': subsector,
                         'direction': trend})
                if data['CHOP'] >= energy_level:
                    fractal_table.append(data)
                df = pd.DataFrame(fractal_table).sort_values(
                    by='CHOP', ascending=False).set_index('ticker')

                if position == 'long':
                    df = df[df.loc[:, 'direction'] == 'uptrend']
                elif position == 'short':
                    df = df[df.loc[:, 'direction'] == 'downtrend']
                else:
                    df

    return df.drop(['direction'], axis=1)

# %%


def fractalenergy_index(interval='W'):

    indeces = PSEconnect().pse_indeces_info.set_index('ticker')

    ticker_list = indeces.index[2:]
    fractal_table = []

    for ticker in ticker_list:
        data = PSEconnect().indexprice(ticker, interval)

        rs = mansfield_rs(data, timeframe=interval).values[-1]

        fractal = data.tail(15).ta.chop(atr_length=1).dropna().values

        if rs >= 0:
            trend = 'uptrend'
        elif rs < 0:
            trend = 'downtrend'

        data = ({'Index': indeces.to_dict()['name'][ticker],
                 'CHOP': float(fractal),
                 'direction': trend})
        fractal_table.append(data)
        df = pd.DataFrame(fractal_table).sort_values(
            ['direction', 'CHOP'], ascending=False).set_index('Index')

    return df


# %%
if __name__ == '__main__':

    timeframe = 'W'
    strategy = 'long'

    if timeframe == 'W':
        header = 'Weekly Timeframe'
    elif timeframe == 'D':
        header = 'Daily Timeframe'

    print(' \n ')
    print('==================================================================================')
    print('PSE Indeces Overview in {0}'.format(header))
    print('==================================================================================')
    print(' \n ')
    print(fractalenergy_index(interval=timeframe))
    print(' \n ')
    print('==================================================================================')
    print('Candidate Stocks for Swing Trade ({0} Position) in {1}'.format(
        strategy.capitalize(), header))
    print('==================================================================================')
    print(' \n ')
    print(fractalenergy_stocks(interval=timeframe,
          energy_level=54, position=strategy))
    print(' \n ')
    print('==================================================================================')
    print('Date:  {0}'.format(datetime.now().date().strftime("%B %d, %Y")))
    print('==================================================================================')

# %%
