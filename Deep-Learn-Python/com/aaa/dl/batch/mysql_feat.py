'''
Created on 2024. 12. 21.

@author: pyw20
'''

from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder

import sqlalchemy as db
import pandas as pd

class MySQL2DLFeat(object):

    def __init__(self):

        user = 'root'
        password = 'hsd12121'
        database = 'etlmysql'

        mysql_url = 'mysql+pymysql://{user}:{password}@localhost/{database}'\
                .format(user=user, password=password, database = database)
        self._engine = db.create_engine(mysql_url)

    def getSeries(self, tablename):
        df = pd.read_sql_table(tablename, con = self._engine, index_col=['state', 'date'])
        col_name = ('_').join(tablename.split('_')[1:])
        df.rename(columns = {'value' : col_name}, inplace=True)

        return df[[col_name]]

    def getDataFrame(self):
        series_civil_force = self.getSeries('table_civil_force_ann')
        series_gdp = self.getSeries('table_gdp_ann')
        series_house_income = self.getSeries('table_house_income_ann')
        series_pov = self.getSeries('table_pov_ann')
        series_tax_exemp = self.getSeries('table_tax_exemp_ann')
        series_unempl = self.getSeries('table_unempl_ann')

        df = pd.concat([series_civil_force, series_gdp, series_house_income, series_pov,
                        series_tax_exemp, series_unempl], axis=1)
        df.interpolate()

        scaler = MinMaxScaler()
        scaler.fit(df)
        ndarray_scaled = scaler.transform(df)
        df_scaled = pd.DataFrame(data=ndarray_scaled, columns=df.columns, index=df.index)

        return df_scaled

    def getFeature(self):
        df = self.getDataFrame()
        df.reset_index(inplace=True)

        ohe = OneHotEncoder(sparse=False)
        ohe.fit(df[['state']])
        one_hot_encoded = ohe.transform(df[['state']])
        ohe_df = pd.DataFrame(one_hot_encoded, columns=ohe.categories_[0])

        df = pd.concat([df, ohe_df], axis=1)

        return df
        