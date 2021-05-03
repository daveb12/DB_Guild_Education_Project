### This file is where we will be putting all of our logic behind extracting and transforming the data
import logging
import requests
import zipfile
import os
import pandas as pd
import ast
from ast import literal_eval
import json
from datetime import datetime

import pprint
pp = pprint.PrettyPrinter(indent=2)

class LOGIC():
    def __init__(self):
        logging.basicConfig(filename='GELogging.log',format='%(asctime)s %(message)s',level=logging.DEBUG)
        pass

    def get_data_files(self, end_point, save_path, chunk_size=128):
        try:
            r = requests.get(end_point, stream=True)
            with open(save_path, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    fd.write(chunk)
            return logging.info('capturing of files completed successfully')
        except:
            return logging.error('unable to retrieve files from url')
    
    def extract_csv_files(self, save_path, csv_extract_path):
        try:
            with zipfile.ZipFile(save_path, 'r') as zip_ref:
                zip_ref.extractall(csv_extract_path)
            logging.info('CSV Files successfully extracted')
            return 'CSV Files Extracted'
        except:
            logging.debug('csv files failed to extract')
            return 'CSV File Extraction Failed'

    def build_data_frames(self, csv_extract_path):
        dfs = {}
        try:
            for filename in os.listdir(csv_extract_path):
                csv_string = str(csv_extract_path) + '/' + str(filename)
                dfs[str(filename)+'_df'] = pd.read_csv(csv_string)
                dfs[str(filename)+'_df']['date_last_uploaded'] = datetime.now()
            logging.info('Data Frames have been created')
            return dfs
        except:
            logging.error('data frames failed to build')
            return 'error'
    
    def fix_stringified_object(self, df, col):

        removed_vals = []
        
        for x in range(len(col)):
            for index, row in df.iterrows():
                try:
                    if ']' == row[col[x]][-1]:
                        pass
                    else:
                        removed_vals.append(index)
                except:
                    removed_vals.append(index)

        dropped_from_df = df.iloc[removed_vals]
        df = df.drop(removed_vals)

        logging.info('incorrect values have been removed from dataframe')

        val = {
            'df':df,
            'dropped': dropped_from_df
            }
        return val

    def convert_col_to_list(self, df, col):

        new_dfs = {}
        
        for y in range(len(col)):
            try:
                obj = df[col[y]].apply(ast.literal_eval).apply(lambda x : [i['name'] for i in x])
                new_dfs['df_'+ str(col[y])] = obj
                logging.info('Stringified columns have been converted to list')
            except:
                logging.error('Stringified columns failed to convert to list')
        return new_dfs
    
    def split_list_to_rows(self, dfs):

        for key in dfs.keys():
            p1 = dfs[key].apply(lambda x : pd.Series(x))
            p2 = p1.apply(lambda x: pd.Series(x),axis=1)
            p3 = p2.apply(lambda x : pd.Series(x)).stack()
            p4 = p3.apply(lambda x : pd.Series(x)).reset_index(level=1, drop=True)
            dfs[key] = p4
        logging.info('Stringified lists have been converted to columns')
        return dfs
    
    def create_new_tables(self, dfs, ogdf):
        merged_df = {}
        for key in dfs.keys():
            try:
                merged_df[key] = dfs[key].merge(ogdf, left_index = True, right_index = True)
                merged_df[key].columns = [key, 'id']
                logging.info(str(key) + ' DataFrame built successfully')
            except:
                logging.error(str(key) + ' DataFrame failed to build')
        return merged_df
    
    def convert_df_to_csv(self, dfs, csv_path):

        for key in dfs.keys():
            try:
                path = str(csv_path) + '/' + str(key) + '.csv'
                dfs[key].to_csv(path, index = False, header = True)
                logging.info(str(dfs[key]) + 'CSV file created')
            except:
                logging.error(str(dfs[key]) + 'CSV file failed to create')
        return 'done'
        
    
