from dotenv import load_dotenv
import pandas as pd

load_dotenv("../.env")

class RetrieveData():

    def __init__(self):
        
        self.period_map = {
            "1m" : 60,
            "5m" : 300
        }


    def get_binance_future_data(self, symbol: str, start_time: int, end_time: int):

        start_datetime, end_datetime = pd.to_datetime(start_time, unit='s'), pd.to_datetime(end_time, unit='s')
        date_range = pd.date_range(start_datetime, end_datetime).strftime("%Y-%m-%d").tolist()
        df = pd.DataFrame()
        
        for date in date_range:
            filename = f"data/{symbol}-trades-{date}.csv"
            date_df = pd.read_csv(filename, index_col=0)
            # For now, return the first date
            return date_df  