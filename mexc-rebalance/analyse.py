import json
import pandas as pd
import requests
import time
import os
import mplfinance as mpf

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
                # exit()

        


chart_rebalance()





# 

# pcts = []
# for i, row in df.iterrows():
#     try:
#         ts = row["rebalanceTime"]
#         hour = (int(ts / 1000) % (60*60*24))  / (60*60)
#         if int(hour) != 16:
#             print(row)

#         pct_chng = (row.basketAfter-row.basketBefore)/row.basketBefore
#         # print(f"pct_chng: {pct_chng*100}%")
#         if pct_chng > 1:
#             print(row)
#         pcts.append(pct_chng)



#     except ZeroDivisionError:
#         continue


# # print(sorted(pcts)[:3])

# print(df.nlargest(10, "leveraged"))
