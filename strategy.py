import mplfinance as mpf
import json
import pandas as pd

def load_data():
    with open("future_data.json", "r") as f:
        future_data = json.load(f)

    with open("move_data.json", "r") as f:
        move_data = json.load(f)

    future_df = pd.DataFrame(future_data["result"][::-1])
    future_df = future_df.set_index('time')
    future_df.index = pd.to_datetime(future_df.index, unit="s")

    move_df = pd.DataFrame(move_data["result"][::-1])
    move_df = move_df.set_index('time')
    move_df.index = pd.to_datetime(move_df.index, unit="s")

    return future_df, move_df

    

def chart_data():
    future_df, move_df = load_data()

    mpf.plot(future_df, type='candle', savefig='future.jpg')

    mpf.plot(move_df, type='candle', savefig='move.jpg')


def interate_data():

    future_df, move_df = load_data()

    strike_price = 16500
    for index, row in future_df.iterrows():
        open_price = row['open']
        move_row = move_df.loc[index]
        print("********")
        print(f"Time is {index}")
        # print(f"Open price is {row['open']}")

        # print(f"Close price is {row['close']}")
        if open_price >= strike_price:
            pass
            # print("Price is over strike price")
        else:
            print("***********")
            print("Price is UNDER strike price")

        print(f"Absolute difference in price is: {abs(open_price-strike_price)}, Price of MOVE contract is {move_row['close']}")


from scipy.optimize import newton

def implied_volatility(market_price, strike_price):
    # Define a function to calculate the difference between the
    # market price and theoretical price of the option
    def price_difference(sigma):
        # theoretical_price = calculate_theoretical_price(sigma, underlying_price, strike_price)
        theoretical_price = (16652.0+16649.0)/2
        return market_price - theoretical_price
    
    # Use the Newton-Raphson method to find the root of the price_difference function
    implied_vol = newton(price_difference, x0=0.2)
    
    return implied_vol


market_price = 16649.5
strike_price = 16700
iv = implied_volatility(market_price, strike_price )




