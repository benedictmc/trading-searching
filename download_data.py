import requests
import pymongo
from datetime import datetime
import time
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

class RetrieveDeltaData():

    def __init__(self):
        
        self.general_api = "https://api.delta.exchange/v2/"
        client = pymongo.MongoClient(f"mongodb://{os.getenv('MONGODB_URI')}")
        self.db = client["prediction_db"]
        self.period_map = {
            "5m" : 300
        }


    def get_delta_data(self, period: str, start_time: int, end_time: int, symbol: str) -> list:
        collection = self.db[f"{symbol}_{period}_data"]
        period_query = {
            "time" : {
                "$gte": start_time,
                "$lt": end_time
            }
        }

        # Gets amount of results from DB and compares to expected
        excepted_results_count = (end_time - start_time)/self.period_map[period]
        document_count = collection.count_documents(period_query)

        if excepted_results_count != document_count:
            print(f"Documents dont match, {excepted_results_count, document_count}")
            api_result = self.__get_future_api_data(period, start_time, end_time, symbol)
            last_timestamp = api_result["result"][-1]['time']

            while last_timestamp != start_time:
                print("Needs pagination...")
                end_time = last_timestamp - self.period_map[period]
                additional_results = self.__get_future_api_data(period, start_time, end_time, symbol)
                api_result["result"].extend(additional_results["result"])
                last_timestamp = additional_results["result"][-1]['time']

            ohlc_data = []
            for _ in api_result["result"]:
                _.update({"_id":_["time"]})
                ohlc_data.append(_)
            try:
                collection.insert_many(ohlc_data, ordered=False)
            except Exception as e:
                print(f"Insertion error documents inserted")

            document_count = collection.count_documents(period_query)
            
            if excepted_results_count != document_count:
                print("Documents still don't match. Exiting... ")
                exit()
        
        print("Getting results from DB..")
        # Returns documents from DB
        return [_ for _ in collection.find(period_query, sort=[("time", 1)])]


    def get_delta_move_data(self, period: str, start_time: int, end_time: int, symbol: str) -> list:
        collection = self.db[f"move_data"]
        period_query = {
            "time" : {
                "$gte": start_time,
                "$lt": end_time
            }
        }

        # Gets amount of results from DB and compares to expected
        document_count = collection.count_documents(period_query)

        if document_count != 0:
            print("Doing something")
            move_data = [_ for _ in collection.find(filter=period_query, sort=[("time", 1)])]
            # print(start_time+43200, end_time)

            # print(pd.to_datetime(start_time+43200, unit="s"))
            print(f"Start time in DB: {pd.to_datetime(move_data[0]['time'], unit='s')}")
            print(f"Start_time+43200 in Function: { pd.to_datetime(start_time+43200, unit='s') }")
            print(f"End time in DB: {pd.to_datetime(move_data[-1]['time'], unit='s')}")
            print(f"end_time in Function: { pd.to_datetime(end_time, unit='s') }")

            if end_time > move_data[-1]['time']:
                print("Downlaod more end data")
                start_time = move_data[-1]['time']
                print(f"New start time in Function: { pd.to_datetime(start_time, unit='s') }")
                print(f"end_time in Function: { pd.to_datetime(end_time, unit='s') }")
            elif start_time < move_data[0]['time']:
                print("Downlaod more start data. NOT DONE")
                exit()
            else:
                return move_data

        asset = symbol
        initial_start_time = start_time+43200
        end_date = datetime.utcfromtimestamp(end_time).strftime('%d%m%y')
        move_data = []
        start_strike_price = 16800

        while True:
            end_date = datetime.utcfromtimestamp(end_time).strftime('%d%m%y')
            start_time = int(time.mktime(datetime.strptime(end_date, "%d%m%y").timetuple()))-43200
            end_time = start_time+86400

            print(initial_start_time, start_time)
            if start_time <= initial_start_time:
                print("Breaking. Saving data....")

                break

            for i in range(10):
                upper_strike_price = start_strike_price + (i*100)
                lower_strike_price = start_strike_price - (i*100)

                upper_symbol = f"MV-{asset}-{upper_strike_price}-{end_date}"
                upper_api_result = self.__get_future_api_data(period, start_time, end_time, upper_symbol)

                if len(upper_api_result["result"]) != 0:
                    start_strike_price = upper_strike_price
                    api_result = upper_api_result
                    break
                
                lower_symbol = f"MV-{asset}-{lower_strike_price}-{end_date}"
                lower_api_result = self.__get_future_api_data(period, start_time, end_time, lower_symbol)

                if len(lower_api_result["result"]) != 0:
                    start_strike_price = lower_strike_price
                    api_result = lower_api_result
                    break
       
                if i == 9:
                    print(f"Strike price not found for {end_date}")
                    exit()

            
            for _ in api_result["result"]:
                _.update({"_id":_["time"], "strike_price": start_strike_price})
                move_data.append(_)

            end_time = end_time - 86400

        try:
            collection.insert_many(move_data, ordered=False)
        except Exception as e:
            print(e)
            print(f"Insertion error documents inserted")

        return [_ for _ in collection.find(filter=period_query, sort=[("time", 1)])]
        




    def __get_future_api_data(self, period: str, start_time: int, end_time: int, symbol: str) -> dict:
        self.api_url = "history/candles"
        request_url = f"{self.general_api}{self.api_url}?resolution={period}&start={start_time}&end={end_time}&symbol={symbol}"
        print(f"Getting data for Delta with request: {request_url}")
        response = requests.get(request_url)
        if response.status_code != 200:
            print("********")
            print("Error")

        return response.json()
