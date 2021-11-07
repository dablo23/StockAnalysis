import psycopg2
from config import config
import numpy as np
import pandas as pd
import os

class MyPostgreSQL:
    "This class handles the PostgreSQL connection and basic features"

    def __init__(self, ticker='AAPL', folder=''):
        
        self.ticker = ticker
        if folder == '':
            folder = os.path.dirname(os.path.realpath(__file__))+'\\SP500\\'
        self.folder = folder
        (self.conn, self.cursor) = self.connectPostgreSQL()
        
        print(f'Constructor: The ticker is set to "{ticker}" and absolut data path is "{folder}"')

    def __del__(self):
        self.disconnectPostgreSQL()

    def connectPostgreSQL(self):
        "Connects to a PostgreSQL server"  
        
        # Get params from inputfile
        params = config()

        # Establish connection
        print('Connecting to database server...')
        self.conn = psycopg2.connect(**params)

        # Creat cursor
        self.cursor = self.conn.cursor()

        # Execute query to verify version
        self.cursor.execute("SELECT version()")

        data = self.cursor.fetchone()
        print('Connection established to: ', data)

        return (self.conn, self.cursor)

    def disconnectPostgreSQL(self):
        #Closing the connection:
        self.conn.close()
        print('Disconnected: the connection is now closed.')

    def createSQLtable(self, ticker=''):
        #Create the table, named from the ticker
        if  ticker == '':
            ticker = self.ticker

        query = f"""
                DROP TABLE IF EXISTS {ticker};
                
                CREATE TABLE {ticker}(
                    date DATE PRIMARY KEY UNIQUE,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    open DOUBLE PRECISION, 
                    close DOUBLE PRECISION, 
                    volume INTEGER,
                    adj_close DOUBLE PRECISION     
                );
                """
        self.cursor.execute(query)

    def copyData(self, ticker, df):
        #Go through each row in the CSV file and insert to the database
        for i in range(df.shape[0]):
            arr =df.iloc[i, :].to_numpy()
            values = np.array2string(arr, separator=',')[1:-1]
            query = f""" 
                    INSERT INTO {ticker}(date,high,low,open,close,volume,adj_close)
                    VALUES({values})
                    """

            self.cursor.execute(query)
            self.conn.commit()  

    def insertWrapper(self, ticker, df):
        #Used to increase performance of the data transformation

        #Unlogg the table to increase speed.
        query = f""" 
                ALTER TABLE {ticker} SET UNLOGGED;
                """
        
        self.cursor.execute(query)
        self.conn.commit()

        #Insert data
        self.copyData(ticker, df)

        #Change back to logged
        query = f""" 
                ALTER TABLE {ticker} SET LOGGED;
                """
        
        self.cursor.execute(query)
        self.conn.commit()

    def convertCSVtoSQL(self):
        
        try:
            df = pd.read_csv(self.folder + self.ticker+'.csv')
        except:
            raise Exception(f'The ticker <{self.ticker}> did not exists in the absolute path <{self.folder}>')
            exit()
                
        #Create table query
        self.createSQLtable(self.ticker)
        print(f'Table created for {self.ticker}')

        #Copy data from CSV to the database
        self.insertWrapper(self.ticker, df)
        

if __name__ == '__main__':
    myClass = MyPostgreSQL()
    myClass.convertCSVtoSQL()


