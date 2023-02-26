import pandas as pd
import numpy as np



class TradeSimulation():


    def __init__(self, df = None):

        # Sample data
        if df is None:
            self.df = pd.read_csv("OSMOUSDT.csv", index_col=0)
        else:
            self.df = df

        self.balance = 500
        self.pnl = 0


    def start(self):
        currently_bought = False
        count = 0
        for index, row in self.df.iterrows():

            # Buy signal
            if not np.isnan(row.fifteen_pct_chg) and not currently_bought:
                buy_ticket = self.buy(row, index)
                currently_bought = True

            if currently_bought:
                print("Open Price Change:")
                print((row.open - buy_ticket["buy_price"])/ buy_ticket["buy_price"] )

                print("Close Price Change:")
                print((row.close - buy_ticket["buy_price"])/ buy_ticket["buy_price"] )
                count += 1
            
            # Sell signal
            if count == 5:
                sell_ticket = self.sell(buy_ticket, row, index)
                currently_bought = False
                count = 0


        print("==================================")
        print(f"PnL: {self.pnl}")
        print("==================================")

        
    def buy(self, row, index):
        
        buy_ticket = {
            "timestamp": index,
            "buy_price": row.high,
            "buy_amount": self.balance / row.high,
            "amount_spent": self.balance,
        }
        print("==================================")
        print("Bought")
        print(buy_ticket)
        print("==================================")

        return buy_ticket

    def sell(self, buy_ticket, row, index):
        sell_ticket = {
            "timestamp": index,
            "sell_price": row.close,
            "sell_amount": buy_ticket["buy_amount"],
            "amount_earned": buy_ticket["buy_amount"] * row.close,
            "profit": (buy_ticket["buy_amount"] * row.close) - buy_ticket["amount_spent"],
        }
        print("==================================")
        print("Sold")
        print(sell_ticket)
        print("==================================")
        self.pnl += sell_ticket["profit"]

        return sell_ticket


# TradeSimulation().start()