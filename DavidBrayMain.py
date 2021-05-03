from SubFolders import config, logic, sqlcode
import logging
import requests
import zipfile
import os
import pandas as pd


import pprint
pp = pprint.PrettyPrinter(indent=2)

logging.basicConfig(filename='GELogging.log',format='%(asctime)s %(message)s',level=logging.DEBUG)

Vars = config.VARIABLES()
runner = logic.LOGIC()
sql = sqlcode.SQL()

def main():
    
    runner.get_data_files(Vars.end_point, Vars.save_path)
    runner.extract_csv_files(Vars.save_path, Vars.csv_extract_path)
    dfs = runner.build_data_frames(Vars.csv_extract_path)
    obj = runner.fix_stringified_object(dfs['movies_metadata.csv_df'], Vars.collist)
    new_lists = runner.convert_col_to_list(obj['df'], Vars.collist)
    splitter = runner.split_list_to_rows(new_lists)
    new_tables = runner.create_new_tables(splitter, obj['df']['id'])

    runner.convert_df_to_csv(new_tables, Vars.csv_extract_path)

    conn = sql.create_database_connection(Vars.sql_file_path)
    c = conn.cursor()

    sql.create_table(conn, c, 'df_genres', new_tables['df_genres'])
    sql.create_table(conn, c, 'df_production_companies', new_tables['df_production_companies'])
    sql.create_table(conn, c, 'movies_metadata', dfs['movies_metadata.csv_df'])

    query = "select g.df_genres, strftime('%Y',m.release_date) as 'Year', sum(m.revenue) from df_genres g inner join movies_metadata m on m.id = g.id group by g.df_genres, strftime('%Y',m.release_date) limit 10"
    sql.run_sql_query(conn, c, query)
    sql.close_connection(conn)

    
    pp.pprint('estamos aqui')




if __name__ == "__main__":
    main()