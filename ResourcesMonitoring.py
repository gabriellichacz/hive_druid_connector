import psutil as ps
import pandas as pd
import time
from matplotlib import pyplot as plt

def plotResults(df):
    fig, (ax1, ax2) = plt.subplots(2, 1, constrained_layout=True)
    ax1.plot(df['CPU_%_usage'])
    ax2.plot(df['RAM_%_usage'])
    ax1.set_title('CPU')
    ax2.set_title('RAM')
    fig.supxlabel('Czas [s]')
    fig.supylabel('Wykorzystanie zasobów [%]')
    plt.savefig('plot_resources.png')

def exitHandler(df):
    plotResults(df)
    
try:
    df = pd.DataFrame()
    df['CPU_%_usage'] = []
    df['RAM_%_usage'] = []

    while True:
        new_row = pd.DataFrame({'CPU_%_usage': ps.cpu_percent(), 'RAM_%_usage': ps.virtual_memory().percent}, index=[0])
        df = pd.concat([new_row,df.loc[:]]).reset_index(drop=True)
        time.sleep(1)
finally:
    exitHandler(df)