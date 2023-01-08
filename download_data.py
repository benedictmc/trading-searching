import requests
import pymongo
from datetime import datetime
import time
from dotenv import load_dotenv
import os
import pandas as pd
import asyncio
import websockets
import json
from collections import deque 


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
        # for i in collection.find(period_query, sort=[("time", 1)]):
        #     print(i)
        return [_ for _ in collection.find(period_query, sort=[("time", 1)])]


    def get_delta_move_data(self, period: str, start_time: int, end_time: int, symbol: str) -> list:
        collection = self.db[f"move_data"]
        all_period_query = {
            "time" : {
                "$gte": start_time,
                "$lt": end_time
            }
        }

        if start_time > end_time:
            print("Start time is greater than end time")
            return 

        days_to_iterate = int(end_time/86400) - int(start_time/86400)
        move_timestamp = start_time
        move_list = []

        for day in range(0, days_to_iterate):
            date = datetime.utcfromtimestamp(move_timestamp).strftime('%d%m%y')
            move_start_ts = move_timestamp - 43200
            move_end_ts = move_start_ts + 86400

            period_query = {
                "time" : {
                    "$gte": move_start_ts,
                    "$lt": move_end_ts
                }
            }
            document_count = collection.count_documents(period_query)

            if document_count > 0 and document_count != 288:
                print(f"Document count for day is incomplete. Documents {document_count}")
                api_result, strike_price = self.__create_move_query(date, move_start_ts, move_end_ts, symbol)
                add_results = []

                for _ in api_result:
                    _.update({"_id":_["time"], "strike_price": strike_price})
                    add_results.append(_)

                try:
                    collection.insert_many(add_results, ordered=False)
                except Exception as e:
                    print(e)
                    print(f"Insertion error documents inserted")
            else:
                print(f"All documents in DB for {date}. Retrieving....")

            move_timestamp += 86400


        move_list.extend([_ for _ in collection.find(filter=all_period_query, sort=[("time", 1)])])

        return move_list


    def __create_move_query(self, date, move_start_ts, move_end_ts, asset):
        period = "5m"

        strike_price = 16800

        for i in range(10):
            upper_strike_price = strike_price + (i*100)
            lower_strike_price = strike_price - (i*100)

            upper_symbol = f"MV-{asset}-{upper_strike_price}-{date}"
            upper_api_result = self.__get_future_api_data(period, move_start_ts, move_end_ts, upper_symbol)

            if len(upper_api_result["result"]) != 0:
                strike_price = upper_strike_price
                api_result = upper_api_result
                break
            
            lower_symbol = f"MV-{asset}-{lower_strike_price}-{date}"
            lower_api_result = self.__get_future_api_data(period, move_start_ts, move_end_ts, lower_symbol)

            if len(lower_api_result["result"]) != 0:
                strike_price = lower_strike_price
                api_result = lower_api_result
                break
    
            if i == 9:
                print(f"Strike price not found for {date}")
                exit()

        return api_result["result"], strike_price
    

    def __get_future_api_data(self, period: str, start_time: int, end_time: int, symbol: str) -> dict:
        self.api_url = "history/candles"
        request_url = f"{self.general_api}{self.api_url}?resolution={period}&start={start_time}&end={end_time}&symbol={symbol}"
        print(f"Getting data for Delta with request: {request_url}")
        response = requests.get(request_url)
        if response.status_code != 200:
            print("********")
            print("Error")

        return response.json()


    def delta_websocket(self):
        self.message_list = deque([])
        collection = self.db[f"ws_move_data"]
        self.first_message = True

        async def receive_messages(websocket):
            

            while True:
                message = await websocket.recv()
                if self.first_message:
                    self.first_message = False
                    continue

                ts = int(time.time())
                if message:
                    message = json.loads(message)
                    message.update({"_id": message["timestamp"]})
                    self.message_list.appendleft(message)
                    
                if ts%60 == 0 and len(self.message_list) > 0:
                    print("Adding messages and emptying queue..")
                    collection.insert_many(self.message_list, ordered=False)
                    self.message_list = deque([])

                
        async def subscribe(websocket, channel):
            subscription_request = {
                "type": "subscribe",
                "payload": {
                    "channels": [
                        {
                            "name": "mark_price",
                            "symbols": [
                                "MARK:MV-BTC-16900-090123"
                            ]
                        }, 
                    ]
                }
            }
            await websocket.send(json.dumps(subscription_request))

        async def run_client():
            async with websockets.connect("wss://socket.delta.exchange") as websocket:
                await subscribe(websocket, "my_channel")
                await receive_messages(websocket)

        asyncio.get_event_loop().run_until_complete(run_client())



RetrieveDeltaData().delta_websocket()
