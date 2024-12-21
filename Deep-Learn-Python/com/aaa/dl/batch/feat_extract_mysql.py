'''
Created on 2024. 12. 21.

@author: pyw20
'''

from com.aaa.dl.batch.mysql_feat import MySQL2DLFeat

import matplotlib.pyplot as plt
import seaborn as sns


def draw_ficture(x, y, data_title, df):
    sns.lineplot(ax = ax[x][y],
                 data = df,
                 x = df.index.get_level_values('date'),
                 y = df[data_title],
                 hue = df.index.get_level_values('state'),
                 marker = 'o',
                 legend=False)

if __name__ == '__main__':
    feat_mysql = MySQL2DLFeat()

    df = feat_mysql.getDataFrame()

    print(df.columns)
    print(df)

    fig, ax = plt.subplots(2, 3, figsize=(70,60))
    draw_ficture(0, 0, 'civil_force_ann', df)
    draw_ficture(0, 1, 'gdp_ann', df)
    draw_ficture(0, 2, 'house_income_ann', df)
    draw_ficture(1, 0, 'pov_ann', df)
    draw_ficture(1, 1, 'tax_exemp_ann', df)
    draw_ficture(1, 2, 'unempl_ann', df)

    plt.show()

    feat = feat_mysql.getFeature()

    print(feat.columns)
    print(feat)