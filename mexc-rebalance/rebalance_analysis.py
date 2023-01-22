import json
import os
import pandas as pd
from retrieve_mexc_data import MexcAPIClient


class RebalanceAnalysis():

    def __init__(self):
        self.mexc_client = MexcAPIClient()

        self.find_15_percent_change()


    def get_amnormal_rebalance_times(self):
        amnormal_rebalance_dict = {}
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
            print("=====================================")
            print(f"Checking {coin_pair}")
            print(f"The time of the amnormal rebalance was: {pd.to_datetime(value['rebal_time'], unit='s')}")
            print(f"The time of the previous rebalance was: {pd.to_datetime(value['last_rebal_time'], unit='s')}")


            res = self.mexc_client.get_kline_data(f"{coin_pair}USDT", "1m", value["last_rebal_time"]*1000, value["rebal_time"]*1000)
            if type(res) == dict:
                continue
            rebalance_price = float(res[0][1])
            max_pct_change = 0

            count = 0
            for ohlc in res:
                new_price = float(ohlc[1])
                pct_change = (new_price - rebalance_price)/rebalance_price
                
                if pct_change > 0.15:
                    print(f">  15% Change at {pd.to_datetime(ohlc[0], unit='ms')}")
                    print(f"> Rebalance price: {rebalance_price}")
                    print(f"> New price: {new_price}")
                    count += 1

                if count == 5:
                    break


                max_pct_change = max(max_pct_change, pct_change)


            # print(res)
            # exit()

            # try:
            #     print(f"Checking {symbol}")
            #     
            #     max_pct_change = 0

            #     if "data" in res and len(res["data"]) == 0:
            #         continue

            #     start_price = float(res["data"][0][1])
            #     # print("Start price: ", start_price)

            #     for i in res["data"]:
            #         new_price = float(i[2])
            #         pct_change = (new_price - start_price)/start_price
            #         if pct_change > 0.15:
            #             print(f">  15% Change at {i[0]}")
            #         max_pct_change = max(max_pct_change, pct_change)

            #     # print("Max pct change: ", max_pct_change)
            # except:
            #     print(res)
            #     print("Error")

    

RebalanceAnalysis()