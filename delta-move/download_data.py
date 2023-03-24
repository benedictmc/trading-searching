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


load_dotenv("../.env")

class RetrieveDeltaData():

    def __init__(self):
        
        self.general_api = "https://api.delta.exchange/v2/"
        self.period_map = {
            "1m" : 60,
            "5m" : 300
        }
        self.move_skip_date = ["161222", "021222", "041122", "111122", "181122"]
        self.strike_price = 24600


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


    def get_delta_move_data(self, period: str, coin: str, start_date: int, end_date: int) -> list:
        
        with open("data/move_contract_meta.json", "r") as f:
            contract_meta = json.load(f)

        start_datetime, end_datetime = pd.to_datetime(start_date, unit='s'), pd.to_datetime(end_date, unit='s')
        settlement_dates = pd.date_range(start_datetime, end_datetime).strftime("%Y-%m-%d").tolist()
        ohlc_data = []

        for settlement_date in settlement_dates:
            
            if contract_meta[settlement_date][coin]:
                # Just getting first move contract for now
                symbol = contract_meta[settlement_date][coin][0]

            # Settlement_date to timestamp
            date = datetime.strptime(settlement_date, "%Y-%m-%d")

            start_time = int(date.timestamp()) - (60*60*24*2)
            end_time = int(date.timestamp()) + (60*60*24*2)
        
            day_ohlc_data = self.__get_future_api_data(period, start_time, end_time, symbol)
            ohlc_data.extend(day_ohlc_data)

        return ohlc_data[::-1]


    def __get_future_api_data(self, period: str, start_time: int, end_time: int, symbol: str) -> dict:
        self.api_url = "history/candles"
        request_url = f"{self.general_api}{self.api_url}?resolution={period}&start={start_time}&end={end_time}&symbol={symbol}"
        print(f"Getting data for Delta with request: {request_url}")
        response = requests.get(request_url)
        if response.status_code != 200:
            print("********")
            print("Error")

        return response.json()["result"]


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


    def get_move_contract_metadata(self):
        self.api_url = "products"
        request_url = f"{self.general_api}{self.api_url}?contract_types=move_options&states=expired"
        print(f"Getting data for Delta with request: {request_url}")
        response = requests.get(request_url)
        if response.status_code != 200:
            print("********")
            print("Error")

        result = response.json()

        with open("data/move_contract_meta.json", "r") as f:
            contract_metadata = json.load(f)

        for contract in result["result"]:
            settlement_time = contract["settlement_time"]
            settlement_date = str(pd.to_datetime(settlement_time).date())
            coin = contract["contract_unit_currency"]
            symbol = contract["symbol"]

            print(f"Coin: {coin}, Symbol: {symbol}, Settlement Date: {settlement_date}")

            if contract_metadata.get(settlement_date) and contract_metadata.get(settlement_date).get(coin) and symbol in contract_metadata[settlement_date][coin]:
                continue
            
            if contract_metadata.get(settlement_date) and contract_metadata.get(settlement_date).get(coin):
                contract_metadata[settlement_date][coin].append(symbol)

            elif contract_metadata.get(settlement_date):
                contract_metadata[settlement_date][coin] = [symbol]

            else:
                contract_metadata[settlement_date] = {coin: [symbol]}

        with open("data/move_contract_meta.json", "w") as f:
            json.dump(contract_metadata, f)
