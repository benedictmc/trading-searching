from collections import deque
import time
import pymongo
from dotenv import load_dotenv
import os
import json
import threading
import websocket
import pandas as pd
import requests


load_dotenv("../.env")


class MexcWSClient():

    def __init__(self):
 
        client = pymongo.MongoClient(f"mongodb://{os.getenv('MONGODB_URI')}")
        self.db = client["trading"]
        self.collection = self.db["mexc_raw"]
        self.message_list = deque([{"begining": int(time.time()) }])
        self.last_ping = int(time.time())
        self.save_index = 0

        # Start the websocket in a separate thread
        ws_thread = threading.Thread(target=self.start_websocket)
        ws_thread.start()

        # Start the data saving thread
        save_thread = threading.Thread(target=self.save_message_list)
        save_thread.start()


    def start_websocket(self):
        channels = ["OSMO_USDT","ZIL_USDT","MAGIC_USDT","TON_USDT","BNX_USDT","LIT_USDT","SWEAT_USDT","LDO_USDT","XLM_USDT","BEL_USDT","ICP_USDT","SRM_USDT","DEGO_USDT","REN_USDT","DASH_USDT","THETA_USDT","MTL_USDT","AUDIO_USDT","ZRX_USDT","LTC_USDT","BONK_USDT","ANT_USDT","FLR_USDT","ETHW_USDT","QTUM_USDT","BCH_USDT","SUSHI_USDT","ENJ_USDT","ADA_USDT","XEC_USDT","SNX_USDT","TRB_USDT","ONT_USDT","ZEC_USDT","FOOTBALL_USDT","SXP_USDT","GMX_USDT","RACA_USDT","CRV_USDT","ALPINE_USDT","CREAM_USDT","MASK_USDT","RLC_USDT","GRT_USDT","CTK_USDT","CEL_USDT","IOTA_USDT","PEOPLE_USDT","ALPHA_USDT","YFI_USDT","SAND_USDT","ANKR_USDT","LRC_USDT","UNFI_USDT","DOT_USDT","BAKE_USDT","UNI_USDT","CRO_USDT","APE_USDT","EGLD_USDT","MINA_USDT","IMX_USDT","XMR_USDT","FITFI_USDT","PSG_USDT","DEFI_USDT","AGLD_USDT","CELR_USDT","XTZ_USDT","AXS_USDT","IOTX_USDT","ETC_USDT","STMX_USDT","SANTOS_USDT","APT_USDT","XRP_USDT","FLM_USDT","DUSK_USDT","STG_USDT","OMG_USDT","FILECOIN_USDT","BIT_USDT","WAVES_USDT","TRX_USDT","ARPA_USDT","BONE_USDT","DC_USDT","OGN_USDT","GALA_USDT","GTC_USDT","TOMO_USDT","OCEAN_USDT","KLAY_USDT","CVC_USDT","DOGE_USDT","SKL_USDT","RVN_USDT","CSPR_USDT","LOOKS_USDT","SHIB_USDT","ICX_USDT","C98_USDT","LUNC_USDT","DGB_USDT","PORTO_USDT","HNT_USDT","NEAR_USDT","NEO_USDT","CELO_USDT","BAT_USDT","ALICE_USDT","COMP_USDT","BSV_USDT","CVX_USDT","DENT_USDT","SNFT_USDT","KAVA_USDT","IOST_USDT","SLP_USDT","COTI_USDT","BTS_USDT","GLMR_USDT","AVAX_USDT","RUNE_USDT","DAR_USDT","SPELL_USDT","KSM_USDT","JASMY_USDT","MKR_USDT","CTSI_USDT","FLOW_USDT","CHZ_USDT","DYDX_USDT","RSR_USDT","CHR_USDT","ACH_USDT","AAVE_USDT","ONE_USDT","FTM_USDT","ROSE_USDT","PERP_USDT","ANC_USDT","GAL_USDT","EOS_USDT","H3RO_USDT","OP_USDT","STORJ_USDT","REEF_USDT","ALGO_USDT","STEPN_USDT","LAZIO_USDT","MATIC_USDT","LINA_USDT","WOO_USDT","ENS_USDT","VINU_USDT","MANA_USDT","RAY_USDT","LINK_USDT","KNC_USDT","BLZ_USDT","YFII_USDT","NKN_USDT","LUNA_USDT","QNT_USDT","XEM_USDT","HT_USDT","ZEN_USDT","BAL_USDT","1INCH_USDT","SFP_USDT"]
        ws = websocket.WebSocketApp("wss://contract.mexc.com/ws", on_message=self.receive_messages)
        ws.on_open = lambda ws: [ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": f"{channel}"}})) for channel in channels]
        ws.run_forever()


    def save_message_list(self):

        while True:
            time.sleep(10)

            if len(self.message_list) > 0:
                try:
                    self.collection.insert_many(self.message_list, ordered=False)
                    self.save_index +=1
                    
                except Exception as e:
                    print(f"Execption occured: {e}")
                    
                self.message_list.clear()

            if self.save_index % 6 == 0:
                print(f"Saving at {pd.to_datetime(int(time.time()), unit='s')}")



    def receive_messages(self, ws, message):
        ts = int(time.time())

        if ts - self.last_ping > 30:
            ws.send('''{"method": "ping"}''')
            self.last_ping = ts 

        if message:
            message = json.loads(message)
            self.message_list.appendleft(message)

            

class MexcAPIClient():

    def __init__(self):
        self.symbols = ["OSMO_USDT","ZIL_USDT","MAGIC_USDT","TON_USDT","BNX_USDT","LIT_USDT","SWEAT_USDT","LDO_USDT","XLM_USDT","BEL_USDT","ICP_USDT","SRM_USDT","DEGO_USDT","REN_USDT","DASH_USDT","AUDIO_USDT","ZRX_USDT","LTC_USDT","BONK_USDT","ANT_USDT","FLR_USDT","ETHW_USDT","QTUM_USDT","BCH_USDT","SUSHI_USDT","ENJ_USDT","ADA_USDT","XEC_USDT","SNX_USDT","TRB_USDT","ONT_USDT","ZEC_USDT","FOOTBALL_USDT","SXP_USDT","GMX_USDT","RACA_USDT","CRV_USDT","ALPINE_USDT","CREAM_USDT","MASK_USDT","RLC_USDT","GRT_USDT","CTK_USDT","CEL_USDT","IOTA_USDT","PEOPLE_USDT","ALPHA_USDT","YFI_USDT","SAND_USDT","ANKR_USDT","LRC_USDT","UNFI_USDT","DOT_USDT","BAKE_USDT","UNI_USDT","CRO_USDT","APE_USDT","EGLD_USDT","MINA_USDT","IMX_USDT","XMR_USDT","FITFI_USDT","PSG_USDT","DEFI_USDT","AGLD_USDT","CELR_USDT","XTZ_USDT","AXS_USDT","IOTX_USDT","ETC_USDT","STMX_USDT","SANTOS_USDT","APT_USDT","XRP_USDT","FLM_USDT","DUSK_USDT","STG_USDT","OMG_USDT","FILECOIN_USDT","BIT_USDT","WAVES_USDT","TRX_USDT","ARPA_USDT","BONE_USDT","DC_USDT","OGN_USDT","GALA_USDT","GTC_USDT","TOMO_USDT","OCEAN_USDT","KLAY_USDT","CVC_USDT","DOGE_USDT","SKL_USDT","RVN_USDT","CSPR_USDT","LOOKS_USDT","SHIB_USDT","ICX_USDT","C98_USDT","LUNC_USDT","DGB_USDT","PORTO_USDT","HNT_USDT","NEAR_USDT","NEO_USDT","CELO_USDT","BAT_USDT","ALICE_USDT","COMP_USDT","BSV_USDT","CVX_USDT","DENT_USDT","SNFT_USDT","KAVA_USDT","IOST_USDT","SLP_USDT","COTI_USDT","BTS_USDT","GLMR_USDT","AVAX_USDT","RUNE_USDT","DAR_USDT","SPELL_USDT","KSM_USDT","JASMY_USDT","MKR_USDT","CTSI_USDT","FLOW_USDT","CHZ_USDT","DYDX_USDT","RSR_USDT","CHR_USDT","ACH_USDT","AAVE_USDT","ONE_USDT","FTM_USDT","ROSE_USDT","PERP_USDT","ANC_USDT","GAL_USDT","EOS_USDT","H3RO_USDT","OP_USDT","STORJ_USDT","REEF_USDT","ALGO_USDT","STEPN_USDT","LAZIO_USDT","MATIC_USDT","LINA_USDT","WOO_USDT","ENS_USDT","VINU_USDT","MANA_USDT","RAY_USDT","LINK_USDT","KNC_USDT","BLZ_USDT","YFII_USDT","NKN_USDT","LUNA_USDT","QNT_USDT","XEM_USDT","HT_USDT","ZEN_USDT","BAL_USDT","1INCH_USDT","SFP_USDT"]
        
        if os.path.exists("data") == False:
            os.mkdir("data")
            
        self.interval_map = {
            "1m": 60*1000,
        }

    def get_kline_data(self, symbol, interval, start_time, end_time):
        result = []
        # end_time = end_time + self.interval_map[interval]
        current_time = start_time

        while current_time < end_time:
            response = self.__internal_get_kline_data(symbol, interval, current_time, end_time)
            result.extend(response)

            current_time = min(end_time, response[-1][0]+self.interval_map[interval])
            time.sleep(.1)

        return self._format_kline(result)


    def __internal_get_kline_data(self, symbol, interval, start_time, end_time):
        url = f"https://api.mexc.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit=1000"
        print(f" > Getting  {url}")
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        # Throws ecxeption

        raise Exception({"status":"error", "message":"Error getting kline data"})


    def get_mexc_rebalances(self):
        for symbol in self.symbols:
            symbol = f"{symbol.split('_')[0]}3S"
            url = f"https://www.mexc.co/api/platform/spot/market/etf/rebalance/list?coinName={symbol}&pageNum=1&pageSize=1000"
            print(f"Getting {url} ... ")
            response = requests.get(url)

            if response.status_code == 200:
                with open(f"data/rebalance_{symbol}.json", "w") as f:
                    json.dump(response.json(), f, indent=4)

            time.sleep(1)

    
    def _format_kline(self, res):
        return [{"_id": r[0], "timestamp": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5], } for r in res ]

        