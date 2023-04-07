from build_dataset import BinanceDataset
import matplotlib.pyplot as plt

bds = BinanceDataset(start_date='2023-03-13', end_date='2023-03-13', symbol='MAGICUSDT')
bds.build()
bds.save_dataset()

def plot_chart(dataset):

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(dataset.index, dataset.price, color='blue', label='Price Direction')

    ax.plot(dataset[dataset['target_variable'] == 1].index, dataset['price'][dataset['target_variable'] == 1], 'g^')
    ax.plot(dataset[dataset['target_variable'] == -1].index, dataset['price'][dataset['target_variable'] == -1], 'r^')

    # set the x-axis frequency of the date markers
    ax.xaxis.set_major_locator(plt.MaxNLocator(10))  # set 10 markers on the x-axis

    # set the title and labels for the axes
    ax.set_title('Time Series Plot')
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')

    # show the plot
    plt.show()

plot_chart(bds.dataset)