import json
import os
import pandas as pd
from retrieve_mexc_data import MexcAPIClient
from chart_rebalance import chart_ohlc_data
import numpy as np

class RebalanceAnalysis():

    def __init__(self):
        self.mexc_client = MexcAPIClient()
        # self.find_15_percent_change()


    def get_amnormal_rebalance_times(self):
        amnormal_rebalance_dict = {}
        

        if os.path.exists("data") == False:
            os.mkdir("data")
            self.mexc_client.get_mexc_rebalances()

        for file in os.listdir("data"):
            if "rebalance" in file:
                rebal_list = self.get_recent_rebalance_json(file)
                coin_name = file.replace("rebalance_", "").replace(".json", "")
                for rebal in rebal_list:
                    rebal_time = int(rebal["rebalanceTime"])
                    rebal_time = pd.to_datetime(rebal_time, unit="ms")
                    if rebal_time.hour != 16 and rebal_time.minute != 0:

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


    def get_recent_rebalance_json(self, file_name):
        with open(f"data/{file_name}", "r") as f:
            data = json.load(f)
        
        return data["data"]["resultList"][:5]


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
                
            chart_ohlc_data(df, df.rebalance_marker.values, df.fifteen_marker.values)
            # exit()


# 0	Open time
# 1	Open
# 2	High
# 3	Low
# 4	Close
# 5	Volume
# 6	Close time
# 7	Quote asset volume


rebalance_analysis = RebalanceAnalysis()

symbols = rebalance_analysis.mexc_client.symbols

for symbol in symbols:
    symbol = symbol.replace("_", "")
    # 01/01/2023 00:00:00 to timestamp
    start_ts =  1672531200000

    # 31/01/2023 00:00:00 to timestamp
    end_ts = 1675203405000

    res = rebalance_analysis.mexc_client.get_kline_data(symbol, "1m", start_ts, end_ts)
    print(len(res))
    exit()