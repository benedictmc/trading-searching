from retrieve_data import DataClient


class DatasetBuilder():

    def __init__(self, start_time, end_time, symbol):
        self.start_time = start_time
        self.end_time = end_time
        self.data_client = DataClient(start_time, end_time)


    def build_dataset(self):
        df = self.data_client.get_future_orderbook_data(self.symbol)
        print(df.head())







