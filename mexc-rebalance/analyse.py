import json
import pandas as pd
import requests
import time
import os
import mplfinance as mpf
from dotenv import load_dotenv

load_dotenv("../.env")
channels = ["OSMO_USDT","ZIL_USDT","MAGIC_USDT","TON_USDT","BNX_USDT","LIT_USDT","SWEAT_USDT","LDO_USDT","XLM_USDT","BEL_USDT","ICP_USDT","SRM_USDT","DEGO_USDT","REN_USDT","DASH_USDT","THETA_USDT","MTL_USDT","AUDIO_USDT","ZRX_USDT","LTC_USDT","BONK_USDT","ANT_USDT","FLR_USDT","ETHW_USDT","QTUM_USDT","BCH_USDT","SUSHI_USDT","ENJ_USDT","ADA_USDT","XEC_USDT","SNX_USDT","TRB_USDT","ONT_USDT","ZEC_USDT","FOOTBALL_USDT","SXP_USDT","GMX_USDT","RACA_USDT","CRV_USDT","ALPINE_USDT","CREAM_USDT","MASK_USDT","RLC_USDT","GRT_USDT","CTK_USDT","CEL_USDT","IOTA_USDT","PEOPLE_USDT","ALPHA_USDT","YFI_USDT","SAND_USDT","ANKR_USDT","LRC_USDT","UNFI_USDT","DOT_USDT","BAKE_USDT","UNI_USDT","CRO_USDT","APE_USDT","EGLD_USDT","MINA_USDT","IMX_USDT","XMR_USDT","FITFI_USDT","PSG_USDT","DEFI_USDT","AGLD_USDT","CELR_USDT","XTZ_USDT","AXS_USDT","IOTX_USDT","ETC_USDT","STMX_USDT","SANTOS_USDT","APT_USDT","XRP_USDT","FLM_USDT","DUSK_USDT","STG_USDT","OMG_USDT","FILECOIN_USDT","BIT_USDT","WAVES_USDT","TRX_USDT","ARPA_USDT","BONE_USDT","DC_USDT","OGN_USDT","GALA_USDT","GTC_USDT","TOMO_USDT","OCEAN_USDT","KLAY_USDT","CVC_USDT","DOGE_USDT","SKL_USDT","RVN_USDT","CSPR_USDT","LOOKS_USDT","SHIB_USDT","ICX_USDT","C98_USDT","LUNC_USDT","DGB_USDT","PORTO_USDT","HNT_USDT","NEAR_USDT","NEO_USDT","CELO_USDT","BAT_USDT","ALICE_USDT","COMP_USDT","BSV_USDT","CVX_USDT","DENT_USDT","SNFT_USDT","KAVA_USDT","IOST_USDT","SLP_USDT","COTI_USDT","BTS_USDT","GLMR_USDT","AVAX_USDT","RUNE_USDT","DAR_USDT","SPELL_USDT","KSM_USDT","JASMY_USDT","MKR_USDT","CTSI_USDT","FLOW_USDT","CHZ_USDT","DYDX_USDT","RSR_USDT","CHR_USDT","ACH_USDT","AAVE_USDT","ONE_USDT","FTM_USDT","ROSE_USDT","PERP_USDT","ANC_USDT","GAL_USDT","EOS_USDT","H3RO_USDT","OP_USDT","STORJ_USDT","REEF_USDT","ALGO_USDT","STEPN_USDT","LAZIO_USDT","MATIC_USDT","LINA_USDT","WOO_USDT","ENS_USDT","VINU_USDT","MANA_USDT","RAY_USDT","LINK_USDT","KNC_USDT","BLZ_USDT","YFII_USDT","NKN_USDT","LUNA_USDT","QNT_USDT","XEM_USDT","HT_USDT","ZEN_USDT","BAL_USDT","1INCH_USDT","SFP_USDT"]

def get_mexc_rebalances(coin_name):
    with open("data/symbols.json", "r") as f:
        symbols = json.load(f)

    for symbol in symbols["pageProps"]["spotMarkets"]["USDT"]:
        if "3S" in symbol["currency"] or "3L" in symbol["currency"]:
            coin_name = symbol["currency"]
            url = f"https://www.mexc.co/api/platform/spot/market/etf/rebalance/list?coinName={coin_name}&pageNum=1&pageSize=1000"
            print(f"Getting {url} ... ")
            response = requests.get(url)

            if response.status_code == 200:
                with open(f"data/rebalance_{coin_name}.json", "w") as f:
                    json.dump(response.json(), f, indent=4)

            time.sleep(1)


def get_rebalance_df():
    if "complied_reblance.csv" in os.listdir():
        df = pd.read_csv("complied_reblance.csv", index_col=0)
        return df

    rebalance_list = []
    for filename in os.listdir("data"):
        if not ("3S" in filename or "3L" in filename):
            continue

        with open(f"data/{filename}") as f:
            raw_data = json.load(f)
        
        rebalance_list.extend(raw_data["data"]["resultList"])


    df = pd.DataFrame(rebalance_list)
    df.set_index("id", inplace=True)
    df.to_csv("complied_reblance.csv")
    return df


def get_price_data():
    

    for filename in os.listdir("data"):
        price_data = {}
        if "3S" in filename:
            with open(f"data/{filename}") as f:
                raw_data = json.load(f)
            filename = filename.replace("rebalance_", "")
            filename = filename.replace("3S", "")
            coin_name = filename.replace(".json", "")

        elif "3L" in filename:
            with open(f"data/{filename}") as f:
                raw_data = json.load(f)
            filename = filename.replace("rebalance_", "")
            filename = filename.replace("3L", "")
            coin_name = filename.replace(".json", "")
        else:
            continue
        
        price_data = {}
        for result in raw_data["data"]["resultList"]:
            ts = result["rebalanceTime"]
            ts = int(ts/1000)
            start = ts-(60*60)
            end = ts+(60*60)
            price_data[result["id"]] = []
            url = f"https://contract.mexc.com/api/v1/contract/kline/{coin_name}_USDT?start={start}&end={end}"
            print(url)
            response = requests.get(url)

            if response.status_code == 200:
                res = response.json()

                if res["success"] != True:
                    continue

                length = len(res["data"]["time"])

                price_data[result["id"]] = [
                    {   "ts": res["data"]["time"][i], 
                        "open": res["data"]["open"][i], 
                        "high": res["data"]["high"][i],
                        "low": res["data"]["low"][i],
                        "close": res["data"]["close"][i],
                        "vol": res["data"]["vol"][i],
                        "amount": res["data"]["amount"][i],
                    } for i in range(length)
                ]

        with open(f"data/data_{coin_name}.json", "w") as f:
            json.dump(price_data, f, indent=4)


def chart_rebalance():
    df = get_rebalance_df()
    for i, row in df.iterrows():
        try:
            pct_chg = round((row['basketAfter'] - row['basketBefore']) / row['basketBefore'], 3)
        except ZeroDivisionError:
            continue

        if not abs(pct_chg) >= 0.5:
            continue

        coin = row["etfCoin"].replace("3S", "").replace("3L", "")
        with open(f"data/data_{coin}.json") as f:
            coin_data = json.load(f)
        
        i = str(i)

        if not i in coin_data:
            continue

        ohlc_data = coin_data[i]
        df = pd.DataFrame(ohlc_data)

        if df.empty:
            continue
        else:
            df.set_index("ts", inplace=True)
            df.index = pd.to_datetime(df.index, unit="s")
            
            
            signal = [float('nan')]*len(df)
            rebalance_index = int(row.rebalanceTime/1000) 
            rebalance_seconds_past = rebalance_index%60
            if rebalance_seconds_past != 0:
                print(f">  {rebalance_seconds_past} Seconds past the minute")
                rebalance_index = rebalance_index - rebalance_seconds_past

            print(f"> COIN: {coin}. Percent Change: {pct_chg}, Delta: {round(row['delta'])}")

            signal[df.index.get_loc(pd.to_datetime(rebalance_index, unit="s"))] = df.loc[pd.to_datetime(rebalance_index, unit="s")].open
            rebalance =  mpf.make_addplot(signal, type='scatter', markersize=125, marker='^')

            mpf.plot(df, type='candle', title=f"COIN: {coin}. Percent Change: {pct_chg}, Delta: {round(row['delta'])}", addplot=rebalance, savefig="save.jpg")
            time.sleep(3)


import time
import pymongo
import os
import json

client = pymongo.MongoClient(f"mongodb://{os.getenv('MONGODB_URI')}")
db = client["trading"]
collection = db["mexc_raw"]

start_price = 0.629


# 1674144000000 4PM 19th
# 1674226800000 3PM 20th
# 1674230400000 4PM 20th
count = 0

for i in collection.find({"symbol": "MAGIC_USDT", "ts": { "$gte": 1674331680000, "$lte": 1674331740000 } }):
    second_ts = int(i["ts"]/1000)
    count += 1
    # if second_ts % 60 == 0:
    print(pd.to_datetime(i["ts"], unit="ms"))
    # print(i["data"])
    price = i["data"]["p"]
    volume = i["data"]["v"]
    print(i["data"])
    exit()
    print(f"Price is: {price}, Amount is: {volume*100}")
    # print((price - start_price))

        # pct_chng = round(, 4)*100
        # print(f"Percent change is {pct_chng}")
    
print(count)    