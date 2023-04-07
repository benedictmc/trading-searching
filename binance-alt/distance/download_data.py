import requests
from dotenv import load_dotenv


# Map of supported exchanges to their API endpoints
EXCHANGE_API_MAP = {
    "binance" : {
        "base_endpoint" : "https://api.binance.com",
        "spot_uri" : "/api/v3/klines",
        "future_uri" : "/fapi/v1/klines",
    }, 
    "delta" : {
        "base_endpoint" : "https://api.delta.exchange/v2",
        "spot_uri" : "unknown",
        "future_uri" : "unknown",
    },
}

# Map of period to seconds
PERIOD_MAP = {
    "1m" : 60,
    "5min" : "5m"
}


load_dotenv("../../.env")

class RetrieveCryptoData():

    def __init__(self, exchange: str):

        if exchange not in EXCHANGE_API_MAP:
            raise Exception("Exchange not supported")
        
        self.exchange = exchange
        self.exchange_api_map = EXCHANGE_API_MAP[exchange]
        self.general_api = self.exchange_api_map["base_endpoint"]


    def future_coin_ohlc(self, period: str, start_time: int, end_time: int, symbol: str) -> list:
        request_uri = self.exchange_api_map["future_uri"]
        period = PERIOD_MAP[period]
        request_url = f"{self.general_api}{request_uri}?symbol={symbol}&interval={period}&startTime={start_time}&endTime={end_time}"

        print(f"> Requesting data from {self.exchange}: {request_url}")
        response = requests.get(request_url)

        if response.status_code != 200:
            print("********")
            print("Error")
            print(response)
            raise Exception(f"Could not retrieve future data from {self.exchange}")

        return response.json()
        

    def spot_coin_ohlc(self, period: str, start_time: int, end_time: int, symbol: str) -> list:
        request_uri = self.exchange_api_map["spot_uri"]
        period = PERIOD_MAP[period]
        request_url = f"{self.general_api}{request_uri}?symbol={symbol}&interval={period}&startTime={start_time}&endTime={end_time}"
        print(f"> Requesting data from {self.exchange}: {request_url}")

        response = requests.get(request_url)

        if response.status_code != 200:
            print("********")
            print("Error")
            print(response)

            raise Exception(f"Could not retrieve future data from {self.exchange}")

        return response.json()
