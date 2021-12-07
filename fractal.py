# %% fractal.py is a python script that ranks fractal energy of differenct assets based based from Doc Severson fractal energy trading strategy.

import pandas as pd
import os
import sqlalchemy
import pandas_ta as ta
from PSEconnect import PSEconnect
from datetime import datetime, timedelta


# %% import price data from database
engine = sqlalchemy.create_engine('postgresql://{0}:{1}@{2}:{3}/trading'.format(os.environ.get('db_user'), os.environ.get('db_password'),
                                                                                os.environ.get('db_host'), os.environ.get('db_port')))

# %%


def Mansfield_RS(df, market_average='PSE.PSEi', timeframe='W'):

    main_index = PSEconnect().indexprice(market_average, timeframe)

    # Get common dates
    date_values = df.index.intersection(main_index.index)

    # This Functions match the number of available data in Index to Market Average
    main_index = main_index.loc[date_values, :]
    df = df.loc[date_values, :]

    # Relative Performance
    Dorsey_RS = df[['close']].div(main_index['close'], axis=0)

    # Dorsey 200 day Simple Moving Average
    if timeframe == 'D':
        ma_length = 200
    elif timeframe == 'W':
        ma_length = 52
    elif timeframe == 'M':
        ma_length = 10
    Dorsey_SMA = Dorsey_RS.ta.sma(length=ma_length)

    # Compute for Mansfield Relative Strength
    # Mansfield Relative Performance = (( Today's Standard Relative Performance divided by Today's Standard Relative Performance 52 Week Moving Average )) - 1) * 100
    Mansfield_RS = ((Dorsey_RS.div(Dorsey_SMA, axis=0))-1)*100

    return Mansfield_RS.dropna()

# %%


def fractal_energytrading(asset='stock', interval='W', energy_level=54, position='long'):

    ticker_list = PSEconnect().pse_stocks_info['ticker'].to_list()
    fractal_table = []

    for ticker in ticker_list:
        data = PSEconnect().stockprice(ticker, interval)
        # Check data if empty
        if data.empty is not True:
            base_date = datetime.now() - timedelta(30)
            sector = PSEconnect().pse_stocks_info.set_index(
                'ticker').loc[ticker, 'sector']
            subsector = PSEconnect().pse_stocks_info.set_index(
                'ticker').loc[ticker, 'subsector']

            rs = Mansfield_RS(data, timeframe=interval).values[-1]

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

                ticker_list = ({'ticker': ticker,
                                'choppiness index': float(fractal),
                                'sector': sector,
                                'subsector': subsector,
                                'direction': trend})
                if ticker_list['choppiness index'] >= energy_level:
                    fractal_table.append(ticker_list)
                df = pd.DataFrame(fractal_table).sort_values(
                    by='choppiness index', ascending=False).set_index('ticker')

                if position == 'long':
                    df = df[df.loc[:, 'direction'] == 'uptrend']
                elif position == 'short':
                    df = df[df.loc[:, 'direction'] == 'downtrend']
                else:
                    df

    return df


# %%
if __name__ == '__main__':
    print(fractal_energytrading())

# %%
