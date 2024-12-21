'''
Created on 2024. 12. 21.

@author: pyw20
'''

from com.aaa.dl.pipeline.mongo_feat import Mongo2DLFeat

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
    feat_mongo = Mongo2DLFeat()

    df = feat_mongo.getDataFrame()

    print(df.columns)
    print(df)

    fig, ax = plt.subplots(2,5, figsize=(70,60))

    draw_ficture(0, 0, 'earn_Construction_month', df)
    draw_ficture(0, 1, 'earn_Financial_Activities_month', df)  
    draw_ficture(0, 2, 'earn_Goods_Producing_month', df)  
    draw_ficture(0, 3, 'earn_Leisure_and_Hospitality_month', df)  
    draw_ficture(1, 0, 'earn_Manufacturing_month', df)
    draw_ficture(1, 1, 'earn_Private_Service_Providing_month', df)
    draw_ficture(1, 2, 'earn_Professional_and_Business_Services_month', df)
    draw_ficture(1, 3, 'earn_Trade_Transportation_and_Utilities_month', df)
    draw_ficture(1, 4, 'unempl_month', df)

    plt.show()

    feat = feat_mongo.getFeature()

    print(feat.columns)
    print(feat)