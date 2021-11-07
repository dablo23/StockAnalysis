import psycopg2
from config import config
import numpy as np
import pandas as pd
import os

def connectPostgreSQL():
    # Connects to a PostgreSQL server   
    
    # Get params from inputfile
    params = config()

    # Establish connection
    print('Connecting to database server...')
    conn = psycopg2.connect(**params)

    # Creat cursor
    cursor = conn.cursor()

    # Execute query to verify version
    cursor.execute("SELECT version()")

    data = cursor.fetchone()
    print('Connection established to: ', data)

    return (conn, cursor)

def disconnectPostgreSQL(conn):
    #Closing the connection:
    conn.close()
    print('Disconnected: the connection is now closed.')

def createSQLtable(ticker, cursor):
    #Create the table, named from the ticker
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
    cursor.execute(query)

def copyData(ticker, conn, cursor, df):
    #Go through each row in the CSV file and insert to the database
    for i in range(df.shape[0]):
        arr =df.iloc[i, :].to_numpy()
        values = np.array2string(arr, separator=',')[1:-1]
        query = f""" 
                INSERT INTO {ticker}(date,high,low,open,close,volume,adj_close)
                VALUES({values})
                """

        
        cursor.execute(query)
        conn.commit()  

def insertWrapper(ticker, conn, cursor, df):
    #Used to increase performance of the data transformation

    #Unlogg the table to increase speed.
    query = f""" 
            ALTER TABLE {ticker} SET UNLOGGED;
            """
    
    cursor.execute(query)
    conn.commit()

    #Insert data
    copyData(ticker, conn, cursor, df)

    #Change back to logged
    query = f""" 
            ALTER TABLE {ticker} SET LOGGED;
            """
    
    cursor.execute(query)
    conn.commit()

def convertCSVtoSQL(ticker):
    folder = os.path.dirname(os.path.realpath(__file__))+'\\SP500\\'

    #Read the stock prices in the CSV file
    df = pd.read_csv(folder+ticker+'.csv')

    #Create connection to PostgreSQL
    (conn, cursor) = connectPostgreSQL()

    #Create table query
    createSQLtable(ticker, cursor)
    print(f'Table created for {ticker}')

    #Copy data from CSV to the database
    insertWrapper(ticker, conn, cursor, df)
    
    #Disconnect from the database
    disconnectPostgreSQL(conn)


if __name__ == '__main__':
    convertCSVtoSQL('AAPL')


