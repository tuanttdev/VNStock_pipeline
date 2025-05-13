import vnstock
from datetime import datetime

defaultSymbolStock = 'VHM'
dataSource = 'VCI'

def extract_company_list():
    list_company_df = vnstock.Vnstock().stock(symbol=defaultSymbolStock, source=dataSource).listing.symbols_by_exchange()

    # print(list_company_df)
    return list_company_df

def extract_price_board(symbols_list=[defaultSymbolStock]):
    price_board_df = vnstock.Vnstock().stock(symbol=defaultSymbolStock, source=dataSource).trading.price_board(symbols_list=symbols_list)
    # print(price_board_df)
    return price_board_df

def extract_OHLCVT_history(symbol=defaultSymbolStock):
    OHLCVT_history = vnstock.Vnstock().stock(symbol=symbol, source=dataSource).quote.history(start=datetime.now().strftime('%Y-%m-%d'), end=datetime.now().strftime('%Y-%m-%d'), interval='1m')

    return OHLCVT_history

def extract_matching_data(symbol=defaultSymbolStock, last_time = None):
    ## if need, convert time to int
    # last_time = int(datetime.now().timestamp() )
    # print(last_time)

    intraday = vnstock.Vnstock().stock(symbol=symbol, source=dataSource).quote.intraday(symbol=symbol, page_size=1000 , last_time=last_time)

    return intraday

def extract_symbol_by_group(symbol=defaultSymbolStock):
    return vnstock.Vnstock().stock(symbol=symbol, source=dataSource).listing.symbols_by_group('VN100')

# print(extract_matching_data())