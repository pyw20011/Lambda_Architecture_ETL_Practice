'''
Created on 2024. 12. 21.

@author: pyw20
'''

import configparser
import os
import time
from com.aaa.etl.us_states import US_STATES

from fredapi import Fred
from pyarrow import fs 

class Fred2Hdfs(object):

    def __init__(self):

        config = configparser.ConfigParser()

        path = 'C:\\eclipse-workspace\\ETL-Stream-Python\\com\\aaa\\etl'

        os.chdir(path)
        config.read('resources/SystemConfig.ini')

        _api_key = config['FRED_CONFIG']['api_key']

        self._fred = Fred(api_key=_api_key)
        self._hdfs = fs.HadoopFileSystem('localhost', 9000)

        self._list_state = [state.value for state in US_STATES]

    def getFredDF(self, freq, state, str_search):
        str_search_text = str_search + state
        print(str_search_text)

        df_fred_col = self._fred.search(str_search_text)
        
        time.sleep(0.5)

        if df_fred_col is None:
            return None

        mask_df = (df_fred_col.title == str_search_text) & (df_fred_col.frequency_short == freq) & (df_fred_col.seasonal_adjustment_short == 'NSA')

        df_fred_col = df_fred_col.loc[mask_df, :]
        df_fred_col['state'] = state

        if not df_fred_col.empty:
            df_etl_col = self._fred.get_series(df_fred_col.iloc[0].id)
            time.sleep(0.7)

            df_etl_col = df_etl_col.to_frame(name='values')
            df_etl_col['realtime_start'] = df_fred_col.iloc[0].realtime_start
            df_etl_col['realtime_end'] = df_fred_col.iloc[0].realtime_end
            df_etl_col['state'] = df_fred_col.iloc[0].state
            df_etl_col['id'] = df_fred_col.iloc[0].id
            df_etl_col['title'] = df_fred_col.iloc[0].title.replace(', ', '_')
            df_etl_col['frequency_short'] = df_fred_col.iloc[0].frequency_short
            df_etl_col['units_short'] = df_fred_col.iloc[0].units_short
            df_etl_col['seasonal_adjustment_short'] = df_fred_col.iloc[0].seasonal_adjustment_short

            return df_etl_col

    def getListFredDF(self, freq, title):
        df_list=map(lambda state: self.getFredDF(freq, state, title), self._list_state)
        df_list=list(filter(lambda df: df is not None, df_list))

        return df_list

    def writeCsv2Hdfs(self, filename, df_data):
        with self._hdfs.open_output_stream(filename) as native_fs:
            line = df_data.to_csv(index_label='date')
            print(line)
            native_fs.write(line.encode())

    def appendCsv2Hdfs(self, filename, df_data):
        with self._hdfs.open_append_stream(filename) as native_fs:
            line = df_data.to_csv(header=False)
            print(line)
            native_fs.write(line.encode())

    def clear_input_files(self, fdir, fname):
        path = fdir + "/" + fname

        if os.path.isfile(path):
            os.remove(path)
            print("{}를 삭제하였습니다.".format(path))
        