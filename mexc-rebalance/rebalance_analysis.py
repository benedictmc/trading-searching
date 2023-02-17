import json
import os
import pandas as pd
from retrieve_mexc_data import MexcAPIClient
from chart_rebalance import chart_ohlc_data
import numpy as np
import pymongo
from dotenv import load_dotenv
import math
from trade_simulation import TradeSimulation

load_dotenv("../.env")

class RebalanceAnalysis():

    def __init__(self, start_ts, end_ts):
        self.mexc_client = MexcAPIClient()
        self.mongo_client = pymongo.MongoClient(f"mongodb://{os.getenv('LOCAL_MONGODB_URI')}")
        self.db = self.mongo_client["MEXC"]
        self.trade_client = TradeSimulation()

        self.start_ts = start_ts
        self.end_ts = end_ts

        # self.find_15_percent_change()  
        # self.create_15pct_df("OSMOUSDT")  

        for symbol in self.mexc_client.symbols: 
            df = self.create_15pct_df(symbol.replace("_", ""))[0]
            TradeSimulation(df).start()
            # exit()


    def get_amnormal_rebalance_times(self, symbol=None, redownload=False):

        if redownload:
            self.mexc_client.get_mexc_rebalances()

        if symbol:
            try:
                with open(f"data/rebalance_{symbol}3S.json", "r") as f:
                    data = json.load(f)
                
                rebal_list = data["data"]["resultList"]
                return self._internal_get_amnormal_rebalance_times(symbol, rebal_list)

            except Exception as e:
                print(f"Could not find data for {symbol}. Exception: {e}")
                return
        else:
            amnormal_rebalance_dict = {}     
            for file in os.listdir("data"):
                if "rebalance" in file:
                    coin_name = file.replace("rebalance_", "").replace("S3", "").replace(".json", "")
                    with open(f"data/{file}", "r") as f:
                        data = json.load(f)

                    rebal_list = data["data"]["resultList"]
                    amnormal_rebalance_dict.update(self._internal_get_amnormal_rebalance_times(coin_name, rebal_list))
                    # amnormal_rebalance_dict[coin_name] = self._internal_get_amnormal_rebalance_times(coin_name, rebal_list)

            return amnormal_rebalance_dict


    def _internal_get_amnormal_rebalance_times(self, coin_name, rebal_list):
        
        amnormal_rebalance_dict = {}

        for rebal in rebal_list:
            rebal_time = int(rebal["rebalanceTime"])
            rebal_time = pd.to_datetime(rebal_time, unit="ms")
            
            if rebal_time.hour != 16 and rebal_time.minute != 0:
            # If the rebalance time is not 16:00:00, then we need to find the last rebalance time   
                if rebal_time.hour < 16:
                    last_rebal_time = rebal_time - pd.Timedelta(days=1)
                else:
                    last_rebal_time = rebal_time

                # Floor datetime to 00:00:00
                last_rebal_time = last_rebal_time.floor("D")
                last_rebal_time = last_rebal_time + pd.Timedelta(hours=16)

                amnormal_rebalance_dict[f"{coin_name}_{rebal_time}"] = {
                    "rebal_time": int(rebal_time.timestamp()),
                    "last_rebal_time": int(last_rebal_time.timestamp())
                }

        return amnormal_rebalance_dict


    # Legacy code
    def find_15_percent_change(self):
        amnormal_rebalance_dict = self.get_amnormal_rebalance_times()


        for key, value in amnormal_rebalance_dict.items():
            coin_pair = key.split("_")[0].replace("3S","").replace("3L","")
            rebal_dt = pd.to_datetime(value['rebal_time'], unit='s').floor("min")


            print("=====================================")
            print(f"Checking {coin_pair}")
            print(f"The time of the amnormal rebalance was: {pd.to_datetime(value['rebal_time'], unit='s')}")
            print(f"The time of the previous rebalance was: {pd.to_datetime(value['last_rebal_time'], unit='s')}")

            last_rebal_ms = value["last_rebal_time"]*1000
            # last_rebal_ms plus one day
            next_rebal_ms = last_rebal_ms + 86400000

            res = self.mexc_client.get_kline_data(f"{coin_pair}USDT", "1m", last_rebal_ms, next_rebal_ms)

            if type(res) == dict:
                continue

            rebalance_price = float(res[0][1])
            max_pct_change = 0

            ohlc_data, rebalance_signal, fifteen_signal = [], [], []
            added_15 = False

            for ohlc in res:
                ohlc_dt = pd.to_datetime(ohlc[0], unit='ms')
                ohlc_dict = {"ts": ohlc[0], "open": float(ohlc[1]), "high": float(ohlc[2]), "low": float(ohlc[3]), "close": float(ohlc[4])}
                ohlc_data.append(ohlc_dict)

                if ohlc_dt == rebal_dt:
                    rebalance_signal.append(ohlc_dict["open"])
                else:
                    rebalance_signal.append(np.nan)

                pct_change = (ohlc_dict["open"] - rebalance_price)/rebalance_price
                
                if pct_change > 0.15:
                    added_15 = True
                    print("Adding 15% marker")
                    fifteen_signal.append(ohlc_dict["open"])
                else:
                    fifteen_signal.append(np.nan)

            df = pd.DataFrame(ohlc_data)
            df.set_index("ts", inplace=True)
            df.index = pd.to_datetime(df.index, unit="ms")

            df["rebalance_marker"] = rebalance_signal
            df["fifteen_marker"] = fifteen_signal
            
            if not added_15:
                continue
            
            marker_plots = [df.rebalance_marker.values]

            # Check is list is all nan
            if not all([math.isnan(x) for x in df.fifteen_marker.values]):
                marker_plots.append(df.fifteen_marker.values)

            chart_ohlc_data(df, marker_plots)


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
            
    
    def create_15pct_df(self, symbol=None):

        if not symbol:
            raise Exception("Must specify symbol")
        
        ohlc_data = self.get_price_data(self.start_ts, self.end_ts, symbol=symbol)
        print(f"Retrieved {len(ohlc_data)} records for {symbol}")

        # Drop the _id field and set the timestamp as the index
        df = pd.DataFrame(ohlc_data).drop("_id", axis=1)
        df.set_index("timestamp", inplace=True)
        df.index = pd.to_datetime(df.index, unit="ms")
        # Make open high low close columns to be type float
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

        normal_rebalance_marker, abnormal_rebalance_marker, fifteen_pct_marker = [], [], []
        rebalance_price, rebalance_open = np.inf, False

        # Get the rebalance times for the symbol
        amnormal_rebalance = self.get_amnormal_rebalance_times(symbol.replace("USDT", ""))
        
        amnormal_rebalance_dt = [pd.to_datetime(v["rebal_time"] - v["rebal_time"]%60, unit="s") for k, v in amnormal_rebalance.items()]

        for index, row in df.iterrows():

            # Adds abnormal marker, open to put the marker at the price line
            if index in amnormal_rebalance_dt:
                abnormal_rebalance_marker.append(row["open"])
            else:
                abnormal_rebalance_marker.append(np.nan)

            # Adds normal marker, if the index is 4PM and the rebalance is open
            if index.hour == 16 and index.minute == 0:
                rebalance_price = row["open"]
                
                normal_rebalance_marker.append(row["open"])
                rebalance_open = True
            else:
                normal_rebalance_marker.append(np.nan)

            # Adds 15% marker, if the rebalance is open and the price is 15% higher than the rebalance price
            if rebalance_open and (row["high"] - rebalance_price) / rebalance_price > 0.15:
                print("Rebalance price: ", rebalance_price)
                print(row)
                fifteen_pct_marker.append(row["high"])
                rebalance_open = False
                print("Adding 15% marker")
            else:
                fifteen_pct_marker.append(np.nan)

        marker_plots = [normal_rebalance_marker]

        # Check is list is all nan
        if not all([math.isnan(x) for x in fifteen_pct_marker]):
            marker_plots.append(fifteen_pct_marker)
        
        if not all([math.isnan(x) for x in abnormal_rebalance_marker]):
            marker_plots.append(abnormal_rebalance_marker)

        df["normal_rebalance"] = normal_rebalance_marker
        df["fifteen_pct_chg"] = fifteen_pct_marker
        df["abnormal_rebalance"] = abnormal_rebalance_marker
        
        df.to_csv(f"{symbol}.csv")
        return df, marker_plots


    def plot_15pct_df(self, df, marker_plots):
        chart_ohlc_data(df, marker_plots)






start_ts =  1672531200000
end_ts = 1675123200000

RebalanceAnalysis(start_ts, end_ts)