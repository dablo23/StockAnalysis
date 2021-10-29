# Used to update stock quotations that have already been fetch.
# Currently this scrip is not functioning due to a probelm at YAHOO (29/10-2021)

import os
import datetime as dt
import pandas as pd
import pandas_datareader as web

#Global variables
m_end = dt.datetime.now().date()
delete = dt.date(1990,1,1)
#print(delete)


def retreiveDataFrameEndDate(df,companyName):
    if df.empty == True:
        print('DataFrame empty for company name: {}'.format(companyName))
        start = dt.datetime(1990,1,1)
    else:
        idx = df.tail(1).index
#        print(idx.to_pydatetime())
        start = idx.to_pydatetime()[0].date()
#        start_datetime = pd.to_datetime(idx)
#        print('start          = {}'.format(start))
#        print('start          = {}'.format(start))
#        print('start_datetime = {}'.format(start_datetime))
#        start = dt.date(idx.to_pydatetime())
#    print('Last date entry of {} was: {}'.format(companyName, start))
    return start +dt.timedelta(days=1)


def updateStockQuotations():
    folderPath = '..\\Data\\SP500\\'
#    i = 1
    exceptions = []
    for filename in os.listdir(folderPath):
        print(filename)
        if filename.endswith('.csv'):
            ticker = filename[:-4]
            df_old = pd.read_csv(folderPath+filename, index_col=0, parse_dates=True)
            start = retreiveDataFrameEndDate(df_old, ticker)
            print('start = {}'.format(start))
            print('end = {}'.format(m_end))
            if start < m_end:
                try:
                    df_new = web.DataReader(ticker, 'yahoo', start, m_end)
                    df = pd.concat([df_old, df_new])
                    df.to_csv('{fname}/{name}.csv'.format(fname=folderPath, name=ticker))
                except Exception:
                    print('Exception was thrown for: {}'.format(ticker))
                    exceptions.append(ticker)
                    pass   
            else:
                print('Already have latest quotation on {}. No Update needed.'.format(ticker))

    return exceptions
#        if i == 2:
#            break
#        i +=1
#    print(df_old.tail())
#    print(df_new.head())

    print( 'Function ran successfully!')

exceptionsThrown = updateStockQuotations()
print(exceptionsThrown)

