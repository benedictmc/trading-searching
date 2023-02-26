from collections import deque
import time
import pymongo
from dotenv import load_dotenv
import os
import json
import threading
import websocket
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io 
import pyodbc

load_dotenv("../.env")


class MexcWSClient():

    def __init__(self):
 
        client = pymongo.MongoClient(os.getenv('AZURE_MONGODB_URI'))
        self.db = client["trading"]
        self.channels = ["OSMO_USDT","ZIL_USDT","MAGIC_USDT","TON_USDT","BNX_USDT","LIT_USDT","SWEAT_USDT","LDO_USDT","XLM_USDT","BEL_USDT","ICP_USDT","SRM_USDT","DEGO_USDT","REN_USDT","DASH_USDT","THETA_USDT","MTL_USDT","AUDIO_USDT","ZRX_USDT","LTC_USDT","BONK_USDT","ANT_USDT","FLR_USDT","ETHW_USDT","QTUM_USDT","BCH_USDT","SUSHI_USDT","ENJ_USDT","ADA_USDT","XEC_USDT","SNX_USDT","TRB_USDT","ONT_USDT","ZEC_USDT","FOOTBALL_USDT","SXP_USDT","GMX_USDT","RACA_USDT","CRV_USDT","ALPINE_USDT","CREAM_USDT","MASK_USDT","RLC_USDT","GRT_USDT","CTK_USDT","CEL_USDT","IOTA_USDT","PEOPLE_USDT","ALPHA_USDT","YFI_USDT","SAND_USDT","ANKR_USDT","LRC_USDT","UNFI_USDT","DOT_USDT","BAKE_USDT","UNI_USDT","CRO_USDT","APE_USDT","EGLD_USDT","MINA_USDT","IMX_USDT","XMR_USDT","FITFI_USDT","PSG_USDT","DEFI_USDT","AGLD_USDT","CELR_USDT","XTZ_USDT","AXS_USDT","IOTX_USDT","ETC_USDT","STMX_USDT","SANTOS_USDT","APT_USDT","XRP_USDT","FLM_USDT","DUSK_USDT","STG_USDT","OMG_USDT","FILECOIN_USDT","BIT_USDT","WAVES_USDT","TRX_USDT","ARPA_USDT","BONE_USDT","DC_USDT","OGN_USDT","GALA_USDT","GTC_USDT","TOMO_USDT","OCEAN_USDT","KLAY_USDT","CVC_USDT","DOGE_USDT","SKL_USDT","RVN_USDT","CSPR_USDT","LOOKS_USDT","SHIB_USDT","ICX_USDT","C98_USDT","LUNC_USDT","DGB_USDT","PORTO_USDT","HNT_USDT","NEAR_USDT","NEO_USDT","CELO_USDT","BAT_USDT","ALICE_USDT","COMP_USDT","BSV_USDT","CVX_USDT","DENT_USDT","SNFT_USDT","KAVA_USDT","IOST_USDT","SLP_USDT","COTI_USDT","BTS_USDT","GLMR_USDT","AVAX_USDT","RUNE_USDT","DAR_USDT","SPELL_USDT","KSM_USDT","JASMY_USDT","MKR_USDT","CTSI_USDT","FLOW_USDT","CHZ_USDT","DYDX_USDT","RSR_USDT","CHR_USDT","ACH_USDT","AAVE_USDT","ONE_USDT","FTM_USDT","ROSE_USDT","PERP_USDT","ANC_USDT","GAL_USDT","EOS_USDT","H3RO_USDT","OP_USDT","STORJ_USDT","REEF_USDT","ALGO_USDT","STEPN_USDT","LAZIO_USDT","MATIC_USDT","LINA_USDT","WOO_USDT","ENS_USDT","VINU_USDT","MANA_USDT","RAY_USDT","LINK_USDT","KNC_USDT","BLZ_USDT","YFII_USDT","NKN_USDT","LUNA_USDT","QNT_USDT","XEM_USDT","HT_USDT","ZEN_USDT","BAL_USDT","SFP_USDT"]
        self.collection = self.db["mexc_raw"]
        self.blob_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_BLOB_URI'))
        self.container_name = "mexcorderbook"
        self.blob_container = self.blob_client.get_container_client(self.container_name)

        self.symbol_deque_map, self.symbol_ping_map = self._create_symbol_maps()
        
        self.last_ping = int(time.time())


    def _create_symbol_maps(self):
        symbol_deque_map = {}
        symbol_ping_map = {}

        for symbol in self.channels:
            symbol_deque_map[symbol] = deque()
            symbol_ping_map[symbol] = int(time.time())
        
        return symbol_deque_map, symbol_ping_map


    def start(self):
        print("==================================")
        print(f"Starting Mexc Data Collection at {int(time.time())}")
        print("==================================")

        for symbol in self.channels:
            # Start the websocket in a separate thread
            ws_thread = threading.Thread(target=self.start_websocket, args=(symbol,))
            ws_thread.start()

        # Start the data saving thread
        save_thread = threading.Thread(target=self.save_message_list)
        save_thread.start()


    def start_websocket(self, symbol):
        print("Starting websocket for symbol: ", symbol)
        
        ws = websocket.WebSocketApp("wss://contract.mexc.com/ws", on_message=self.receive_messages)
        ws.on_open = lambda ws: ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": symbol}}))
        ws.run_forever()


    def receive_messages(self, ws, message):
        ts = int(time.time())

        if message:
            message = json.loads(message)
            symbol = message["symbol"]
            price_update = {
                "symbol": symbol,
                "timestamp": message["data"]["t"],
                "price": message["data"]["p"],
                "v": message["data"]["v"],
                "T": message["data"]["T"],
                "O": message["data"]["O"],
                "M": message["data"]["M"],
            }

            # Ping every 30 seconds
            if ts - self.symbol_ping_map[symbol] > 30:
                ws.send('''{"method": "ping"}''')
                self.symbol_ping_map[symbol] = ts

            self.symbol_deque_map[message["symbol"]].append(price_update)


    def save_message_list(self):
        print("Starting data saving thread")
        try:
            sleep_index = 0
            while True:
                # Save every 20 minutes
                time.sleep(60*60)
                print("==================================")
                print(f"Saving data at {int(time.time())}")
                print("==================================")
                
                for key, message_list in self.symbol_deque_map.items():
                    list_len = len(message_list)

                    if list_len == 0:
                        continue
                    try:
                        df_bytes = pd.DataFrame(message_list).to_csv(index=False).encode('utf-8')
                        df_stream = io.BytesIO(df_bytes)
                        blob_client = self.blob_container.get_blob_client(f"{key}/mexc_orderbook_{int(time.time())}.csv")
                        blob_client.upload_blob(df_stream, overwrite=True)
                        print(f"Saved {list_len} {key} messages since last save")

                    except Exception as e:
                        print(f"Execption occured: {e}")
                        
                    self.symbol_deque_map[key].clear()
                
                # Adds new blob storage data to database
                self.compile_blob_data()


        except Exception as e:
            print(f"Exception occured: {e}")
    

    # Ran once to create tables
    def create_tables(self):
        conn = pyodbc.connect(os.getenv('AZURE_SQL_URI'))
        cursor = conn.cursor()
        for symbol in self.channels:
            try:
                cursor.execute(f"CREATE TABLE {symbol}_orderbook (symbol VARCHAR(20), timestamp BIGINT, price FLOAT, v FLOAT, T VARCHAR(20), O VARCHAR(20), M VARCHAR(20))")
                conn.commit()
            except Exception as e:
                print(f"Exception occured: {e}")
        conn.close()


    def compile_blob_data(self):
        print("==================================")
        print(f"Compiling data at {int(time.time())}")
        print("==================================")

        for symbol in self.channels:
            print("Compiling symbol: ", symbol)

            # Get last timestamp
            conn = pyodbc.connect(os.getenv('AZURE_SQL_URI'))
            cursor = conn.cursor()
            cursor.execute(f"SELECT TOP 1 timestamp FROM {symbol}_orderbook ORDER BY timestamp DESC")
            row = cursor.fetchone()
            last_timestamp = int(row[0]) if row else 0

            blob_list = self.blob_container.list_blobs(name_starts_with=f"{symbol}/")
            data_list = []

            for blob in blob_list:
                content = self.blob_client.get_blob_client(self.container_name, blob.name).download_blob().readall().decode("utf-8")
                blob_timestamp = int(blob.name.split("_")[-1].split(".")[0])

                
                if blob_timestamp <= last_timestamp:
                    # Blob is already in database
                    print("Blob already in database; Moving to archive", blob.name)
                    blob_url = f"https://benslake.blob.core.windows.net/mexcorderbook/{blob.name}"
                    destination_blob_name = blob.name
                    self.blob_client.get_blob_client("orderbookarchive", destination_blob_name).start_copy_from_url(blob_url)
                    self.blob_client.get_blob_client(self.container_name, blob.name).delete_blob()
                else:
                    # Blob needs to be added to database
                    content = content.split("\n")

                    for row in content[1:-1]:
                        data_list.append(tuple(row.split(",")))
            
            if len(data_list) == 0:
                continue
            else:
                print("Data list length: ", len(data_list))
                print("Adding datalist to database")
                cursor = conn.cursor()
                sql = f"INSERT INTO {symbol}_orderbook (symbol, timestamp, price, v, T, O, M) VALUES (?, ?, ?, ?, ?, ?, ?)"
                cursor.executemany(sql, data_list)
                conn.commit()
                conn.close()
