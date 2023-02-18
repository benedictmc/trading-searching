
class DataClient():


    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time


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