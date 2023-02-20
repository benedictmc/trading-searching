import pymongo
from dotenv import load_dotenv
from mexc_api_client import MexcAPIClient
import os
import time

load_dotenv("../.env")


class DataClient():

    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time
        mongo_client = pymongo.MongoClient(os.getenv('AZURE_MONGODB_URI'))
        self.db = mongo_client["trading"]
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

    
    def get_futures_data(self, symbol, start_ts=None, end_ts=None):
        print()

        start_ts = 1676926426000
        end_ts = 1676930026000
        query = {"ts": {"$gt":start_ts, "$lt":end_ts}, "symbol": symbol}

        if not start_ts and not end_ts:
            print("> Using function defined start and end times")
            start_ts = self.start_time
            end_ts = self.end_time
        
        query_timing = int(time.time())
        result = self.db["mexc_raw"].find(query)

        for i in result:
            print(i)

        print(f"> Query took {int(time.time()) - query_timing} seconds")


# DataClient(start_time=1676655774996, end_time=1676655774996).get_futures_data("STMX_USDT")