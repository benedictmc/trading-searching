from collections import deque
import time
from dotenv import load_dotenv
import os
import json
import threading
import websocket
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io 
import pyodbc
from mexc_api_client import MexcAPIClient
import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler

load_dotenv("../.env")

logger = logging.getLogger(__name__)

logger.addHandler(AzureLogHandler(
    connection_string=os.getenv('AZURE_INSIGHTS_URI'))
)

logger.warning("Starting MexcWSClient....")



class MexcWSClient():

    def __init__(self):
        logger.warning("Another log")
        self.mexc_api_client = MexcAPIClient()
        self.channels = ["OSMO_USDT","ZIL_USDT","MAGIC_USDT","BNX_USDT","LIT_USDT","SWEAT_USDT","LDO_USDT","XLM_USDT","BEL_USDT","ICP_USDT","SRM_USDT","DEGO_USDT","REN_USDT","DASH_USDT","THETA_USDT","MTL_USDT","AUDIO_USDT","ZRX_USDT","LTC_USDT","BONK_USDT","ANT_USDT","FLR_USDT","ETHW_USDT","QTUM_USDT","BCH_USDT","SUSHI_USDT","ENJ_USDT","ADA_USDT","XEC_USDT","SNX_USDT","TRB_USDT","ONT_USDT","ZEC_USDT","FOOTBALL_USDT","SXP_USDT","GMX_USDT","RACA_USDT","CRV_USDT","ALPINE_USDT","CREAM_USDT","MASK_USDT","RLC_USDT","GRT_USDT","CTK_USDT","CEL_USDT","IOTA_USDT","PEOPLE_USDT","ALPHA_USDT","YFI_USDT","SAND_USDT","ANKR_USDT","LRC_USDT","UNFI_USDT","DOT_USDT","BAKE_USDT","UNI_USDT","CRO_USDT","APE_USDT","EGLD_USDT","MINA_USDT","IMX_USDT","XMR_USDT","FITFI_USDT","PSG_USDT","AGLD_USDT","CELR_USDT","XTZ_USDT","AXS_USDT","IOTX_USDT","ETC_USDT","STMX_USDT","SANTOS_USDT","APT_USDT","XRP_USDT","FLM_USDT","DUSK_USDT","STG_USDT","OMG_USDT","FILECOIN_USDT","BIT_USDT","WAVES_USDT","TRX_USDT","ARPA_USDT","BONE_USDT","DC_USDT","OGN_USDT","GALA_USDT","GTC_USDT","OCEAN_USDT","KLAY_USDT","CVC_USDT","DOGE_USDT","SKL_USDT","RVN_USDT","CSPR_USDT","LOOKS_USDT","SHIB_USDT","ICX_USDT","C98_USDT","LUNC_USDT","DGB_USDT","PORTO_USDT","HNT_USDT","NEAR_USDT","NEO_USDT","CELO_USDT","BAT_USDT","ALICE_USDT","COMP_USDT","BSV_USDT","CVX_USDT","DENT_USDT","SNFT_USDT","KAVA_USDT","IOST_USDT","SLP_USDT","COTI_USDT","BTS_USDT","GLMR_USDT","AVAX_USDT","RUNE_USDT","DAR_USDT","SPELL_USDT","KSM_USDT","JASMY_USDT","MKR_USDT","CTSI_USDT","FLOW_USDT","CHZ_USDT","DYDX_USDT","RSR_USDT","CHR_USDT","ACH_USDT","AAVE_USDT","ONE_USDT","FTM_USDT","ROSE_USDT","PERP_USDT","ANC_USDT","GAL_USDT","EOS_USDT","OP_USDT","STORJ_USDT","REEF_USDT","ALGO_USDT","STEPN_USDT","LAZIO_USDT","MATIC_USDT","LINA_USDT","WOO_USDT","ENS_USDT","VINU_USDT","MANA_USDT","RAY_USDT","LINK_USDT","KNC_USDT","BLZ_USDT","YFII_USDT","NKN_USDT","LUNA_USDT","XEM_USDT","HT_USDT","ZEN_USDT","BAL_USDT","SFP_USDT"]

        self.blob_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_BLOB_URI'))
        self.container_name = "mexcorderbook"
        self.blob_container = self.blob_client.get_container_client(self.container_name)

        self.symbol_deque_map, self.symbol_ping_map = self._create_symbol_maps()
        self.last_ping = int(time.time())
        self.ws_symbol_map, self.ws_thread_map = {}, {}
        self.message_count = 0


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

        # Start the data saving thread
        log_thread = threading.Thread(target=self.log_info)
        log_thread.start()


    def start_websocket(self, symbol):
        print("Starting websocket for symbol: ", symbol)
        # logger.warning("Starting websocket for symbol: ", symbol)

        try:
            ws = websocket.WebSocketApp("wss://contract.mexc.com/ws", on_message=self.receive_messages, on_error=self.on_error, on_close=self.on_close )
            ws.on_open = lambda ws: ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": symbol}}))
            
            self.ws_symbol_map[str(ws)] = symbol

            self.ws_thread_map[str(ws)] = {
                "symbol": symbol,   
                "total_messages": 0,
                "status": "running",
                "error": []
            }

            ping_thread = threading.Thread(target=self.ping, args=(ws,))
            ping_thread.start()
            ws.run_forever()

        except Exception as e:
            print("Websocket error. Restarting websocket")
            logger.error(f"Websocket error. Restarting websocket, {e}")

    
    def on_error(self, ws, error):
        message = f"Error occured: {error} on ws {self.ws_symbol_map[str(ws)]}"
        print(message)
        logger.error(message)
        self.ws_thread_map[str(ws)]["error"].append(message)


    def on_close(self, ws, close_status_code, close_msg):
        print(f"Closed websocket: Status {close_status_code} {close_msg} on ws {self.ws_symbol_map[str(ws)]}")
        logger.error(f"Closed websocket: Status {close_status_code} {close_msg} on ws {self.ws_symbol_map[str(ws)]}")
        self.ws_thread_map[str(ws)]["status"] = "closed"
        ws_thread = threading.Thread(target=self.start_websocket, args=(self.ws_symbol_map[str(ws)],))
        ws_thread.start()


    def ping(self, ws):
        while True:
            time.sleep(30)
            ws.send('''{"method": "ping"}''')


    def receive_messages(self, ws, message):
        self.ws_thread_map[str(ws)]["total_messages"] += 1
        symbol = self.ws_symbol_map[str(ws)]
        if message and "symbol" in message:
            self.message_count += 1
            self.symbol_deque_map[symbol].append(message)


    def save_message_list(self):
        print("Starting data saving thread")
        try:
            while True:
                # Save every 24 hours
                time.sleep(60*60*24)
                print("==================================")
                print(f"Saving data at {int(time.time())}")
                print("==================================")
                
                for key, message_list in self.symbol_deque_map.items():
                    list_len = len(message_list)

                    if list_len == 0:
                        continue
                    try:
                        format_message_list = []

                        for raw_message in message_list:
                            message = json.loads(raw_message)
                            symbol = message["symbol"]
                            format_message_list.append({
                                "symbol": symbol,
                                "timestamp": message["data"]["t"],
                                "price": message["data"]["p"],
                                "v": message["data"]["v"],
                                "T": message["data"]["T"],
                                "O": message["data"]["O"],
                                "M": message["data"]["M"],
                            })

                        df_bytes = pd.DataFrame(format_message_list).to_csv(index=False).encode('utf-8')
                        df_stream = io.BytesIO(df_bytes)

                        # Save blob to storage with millisecond timestamp
                        blob_client = self.blob_container.get_blob_client(f"{key}/mexc_orderbook_{round(time.time() * 1000)}.csv")
                        blob_client.upload_blob(df_stream, overwrite=True)

                    except Exception as e:
                        print(f"Execption occured: {e}")
                        logger.error(f"Exception occured: {e}")

                        
                    self.symbol_deque_map[key].clear()
                
                # Adds new blob storage data to database
                self.compile_blob_data()


        except Exception as e:
            print(f"Exception occured: {e}")
            logger.error(f"Exception occured: {e}")
    

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
            blobs_to_move = {} 

            for blob in blob_list:
                content = self.blob_client.get_blob_client(self.container_name, blob.name).download_blob().readall().decode("utf-8")
                blob_timestamp = int(blob.name.split("_")[-1].split(".")[0])
                blobs_to_move[blob.name] = f"https://benslake.blob.core.windows.net/mexcorderbook/{blob.name}"
                
                if blob_timestamp <= last_timestamp:
                    # Blob is already in database
                    print("Blob already in database; Moving to archive", blob.name)
                    print("NOTE: This statement shouldn't be reached")
                    logger.error("This statement shouldn't be reached. Blob already in database")
                else:
                    # Blob needs to be added to database
                    content = content.split("\n")

                    for row in content[1:-1]:
                        data_list.append(tuple(row.split(",")))

            if len(data_list) == 0:
                print("No new data to add to database")
                print("==================================")
                continue
            else:
                print("Data list length: ", len(data_list))
                print("Adding datalist to database")
                cursor = conn.cursor()
                sql = f"INSERT INTO {symbol}_orderbook (symbol, timestamp, price, v, T, O, M) VALUES (?, ?, ?, ?, ?, ?, ?)"
                cursor.executemany(sql, data_list)
                conn.commit()
                conn.close()
            
            print(f"Moving {len(blobs_to_move)} blobs to archive")
            # Move blobs to archive
            for key, value in blobs_to_move.items():
                self.blob_client.get_blob_client("orderbookarchive", key).start_copy_from_url(value)
                self.blob_client.get_blob_client(self.container_name, key).delete_blob()

            print("Done compiling symbol: ", symbol)
            print("==================================")


    def get_rebalance_data(self):
        conn = pyodbc.connect(os.getenv('AZURE_SQL_URI'))
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM rebalances")
        row = cursor.fetchall()
        ids = set([x[0] for x in row])
        rebalance_list = []
        for symbol in self.channels:

            symbol_rebalances = self.mexc_api_client.get_mexc_rebalances(symbol)
            for rebalance in symbol_rebalances:

                # Only add rebalance if it not in database
                if rebalance["id"] in ids:
                    continue

                rebalance_list.append(tuple(list(rebalance.values())[:-1]))

            if len(rebalance_list) == 0:
                continue

        print(f"Attempting to add {len(rebalance_list)} rebalance data to database")
        conn = pyodbc.connect(os.getenv('AZURE_SQL_URI'))
        cursor = conn.cursor()
        sql = f"INSERT INTO rebalances (id, rebalanceTime, etfCoin, basketBefore , basketAfter, delta, leveraged, rebalance) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cursor.executemany(sql, rebalance_list)
        conn.commit()
        conn.close()


    def log_info(self):
        print("==================================")
        print(f"Starting data logging thread at {int(time.time())}")
        print("==================================")

        logger.info(f"Starting data logging thread at {int(time.time())}")

        while True:
            for _, ws_data in self.ws_thread_map.items():
                logger.info(f">  WS for symbol {ws_data['symbol']} is {ws_data['status']}. There was been a total of {ws_data['total_messages']} messages on the socket")

            time.sleep(60*10)



MexcWSClient().get_rebalance_data()