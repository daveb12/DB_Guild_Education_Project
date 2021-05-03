### This file is for anything to do with SQL
import sqlite3
from sqlite3 import Error
import os
import logging

import pprint
pp = pprint.PrettyPrinter(indent=2)

class SQL():
    def __init__(self):
        logging.basicConfig(filename='GELogging.log',format='%(asctime)s %(message)s',level=logging.DEBUG)
    
    def create_database_connection(self, file_path):
        # create a database connection to a SQLite database 
        conn = None
        try:
            conn = sqlite3.connect(file_path)
            logging.info('Database connection created successfully')
            return conn
        except Error as e:
            logging.error('Database connection failed to setup and here is the error: ' + str(e))
    
    def close_connection(self, conn):
        if conn:
            conn.close()
            logging.info('Connection closed successfully')

    def create_table(self, conn, c, table_name, df):
        try:
            df.to_sql(table_name, conn, if_exists='fail')
            logging.info(str(table_name) + ' created successfully')
        except ValueError as e:
            logging.error(e)
        return 'done'
    
    def run_sql_query(self, conn, c, query):
        try:
            c.execute(query)
            countrows = c.fetchall()
            logging.info('the query ran successfully')
            pp.pprint(countrows)
        except:
            logging.debug('the query failed to execute successfully')
