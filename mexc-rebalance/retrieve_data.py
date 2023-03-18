from dotenv import load_dotenv
from mexc_api_client import MexcAPIClient
import os
import pyodbc
import pandas as pd

load_dotenv("../.env")


class DataClient():

    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        self.mexc_client = MexcAPIClient()


    def get_price_data(self, start_ts, end_ts, interval = "1m", all_symbols = None, symbol = None):
        if not all_symbols and not symbol:
            raise Exception("Must specify all or symbol")

        if all_symbols:
            for symbol in self.mexc_client.symbols:
                symbol = symbol.replace("_", "")
                self._internal_get_price_data(start_ts, end_ts, interval, symbol)
        else:
            return self._internal_get_price_data(start_ts, end_ts, interval, symbol)


    def _internal_get_price_data(self, start_ts, end_ts, interval, symbol):
        try:
            print("=====================================")
            print(f"Getting data for {symbol}")
            print("=====================================")

            excepted_records = int((end_ts - start_ts) / self.mexc_client.interval_map[interval])

            if symbol in self.db.list_collection_names():
                db_fetch = self.db[f"{symbol}"].find({"timestamp": {"$gte": start_ts, "$lte": end_ts}}).sort("timestamp", 1)
                result = [ _ for _ in db_fetch]

                if len(result) == excepted_records:
                    print("> All records already in db")
                    return result
                else:
                    print("> Some records in DB. Retrieving new records...")
                    db_start_ts = int(result[0]["timestamp"])
                    db_end_ts = int(result[-1]["timestamp"])

                    if start_ts != db_start_ts:
                        end_ts = db_start_ts
                        # Gets new records from new start_ts to start of db records
                        result = self.mexc_client.get_kline_data(symbol, interval, start_ts, db_start_ts)
                        print("> Inserting new records into db....")
                        self.db[f"{symbol}"].insert_many(result, ordered=False)

                    elif end_ts != db_end_ts:
                        # Gets new records from end of db recordsstart_ts to new end_ts
                        result = self.mexc_client.get_kline_data(symbol, interval, db_end_ts+self.mexc_client.interval_map[interval], end_ts)
                        print("> Inserting new records into db....")
                        self.db[f"{symbol}"].insert_many(result, ordered=False)
                    
            else:
                print("> Retrieving new records....")
                api_result = self.mexc_client.get_kline_data(symbol, interval, start_ts, end_ts)
                print("> Inserting new records into db....")
                self.db[f"{symbol}"].insert_many(api_result, ordered=False)

            db_fetch = self.db[f"{symbol}"].find({"timestamp": {"$gte": start_ts, "$lte": end_ts}}).sort("timestamp", 1)
            result = [ _ for _ in db_fetch]
            return result
        
        except Exception as e:
            print(f"Error getting data for {symbol}")
            print(e)

    
    def get_future_orderbook_data(self, symbol, exchange, start_ts=None, end_ts=None):

        if not start_ts and not end_ts:
            print("> Using function defined start and end times for orderbook")
            start_ts = self.start_time
            end_ts = self.end_time

        if exchange not in ["mexc", "binance"]:
            raise Exception("Exchange must be mexc or binance")

        if exchange == "mexc":
            conn = pyodbc.connect(os.getenv('AZURE_SQL_URI'))
            query = f"SELECT * FROM [dbo].[{symbol}_orderbook] where timestamp >= { start_ts } and timestamp < {end_ts} order by timestamp desc"
            df = pd.read_sql(query, conn)
            conn.close()
            df.rename(columns={"v": f"{symbol}_v", "price": f"{symbol}_price"}, inplace=True)
            return df
        
        if exchange == "binance" and symbol == "BTC_USDT":
            symbol = "BTCUSDT"
            # Hack to get binance data; Should put in db first
            df = pd.read_csv(f"data\BTCUSDT-trades-2023-03-02.csv")
            df.drop(columns=["id", "is_buyer_maker", "quote_qty"], inplace=True)
            df.rename(columns={"time": "timestamp", "qty": "BTC_USDT_v", "price": "BTC_USDT_price"}, inplace=True)

            return df


    def get_rebalance_data(self, symbol, start_ts=None, end_ts=None):
    
        if not start_ts and not end_ts:
            print("> Using function defined start and end times for rebalance")
            start_ts = self.start_time
            end_ts = self.end_time

        symbol = symbol.replace("_USDT", "")
    
        conn = pyodbc.connect(os.getenv('AZURE_SQL_URI'))
        query = f"SELECT * FROM [dbo].[rebalances] where (etfCoin = '{ symbol }3S' or etfCoin = '{ symbol }3L' ) and rebalanceTime >= { start_ts } and rebalanceTime < {end_ts} order by rebalanceTime desc"
        df = pd.read_sql(query, conn)
        return df






