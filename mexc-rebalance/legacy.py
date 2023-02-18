# File to hold legacy functions that are no longer used
# but are kept for reference

# Legacy code
def find_15_percent_change(self):
    amnormal_rebalance_dict = self.get_amnormal_rebalance_times()


    for key, value in amnormal_rebalance_dict.items():
        coin_pair = key.split("_")[0].replace("3S","").replace("3L","")
        rebal_dt = pd.to_datetime(value['rebal_time'], unit='s').floor("min")


        print("=====================================")
        print(f"Checking {coin_pair}")
        print(f"The time of the amnormal rebalance was: {pd.to_datetime(value['rebal_time'], unit='s')}")
        print(f"The time of the previous rebalance was: {pd.to_datetime(value['last_rebal_time'], unit='s')}")

        last_rebal_ms = value["last_rebal_time"]*1000
        # last_rebal_ms plus one day
        next_rebal_ms = last_rebal_ms + 86400000

        res = self.mexc_client.get_kline_data(f"{coin_pair}USDT", "1m", last_rebal_ms, next_rebal_ms)

        if type(res) == dict:
            continue

        rebalance_price = float(res[0][1])
        max_pct_change = 0

        ohlc_data, rebalance_signal, fifteen_signal = [], [], []
        added_15 = False

        for ohlc in res:
            ohlc_dt = pd.to_datetime(ohlc[0], unit='ms')
            ohlc_dict = {"ts": ohlc[0], "open": float(ohlc[1]), "high": float(ohlc[2]), "low": float(ohlc[3]), "close": float(ohlc[4])}
            ohlc_data.append(ohlc_dict)

            if ohlc_dt == rebal_dt:
                rebalance_signal.append(ohlc_dict["open"])
            else:
                rebalance_signal.append(np.nan)

            pct_change = (ohlc_dict["open"] - rebalance_price)/rebalance_price
            
            if pct_change > 0.15:
                added_15 = True
                print("Adding 15% marker")
                fifteen_signal.append(ohlc_dict["open"])
            else:
                fifteen_signal.append(np.nan)

        df = pd.DataFrame(ohlc_data)
        df.set_index("ts", inplace=True)
        df.index = pd.to_datetime(df.index, unit="ms")

        df["rebalance_marker"] = rebalance_signal
        df["fifteen_marker"] = fifteen_signal
        
        if not added_15:
            continue
        
        marker_plots = [df.rebalance_marker.values]

        # Check is list is all nan
        if not all([math.isnan(x) for x in df.fifteen_marker.values]):
            marker_plots.append(df.fifteen_marker.values)

        chart_ohlc_data(df, marker_plots)
