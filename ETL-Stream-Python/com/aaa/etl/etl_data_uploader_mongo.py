'''
Created on 2024. 12. 21.

@author: pyw20
'''

from com.aaa.etl.kafka_mongo_stream import Kafka2MongoStream

if __name__ == '__main__':
    stream2mongo = Kafka2MongoStream()
    
    df_unempl_mon = stream2mongo.getDF('topic_unempl_mon')
    stream2mongo.saveDF2MongoDB(df_unempl_mon, 'coll_unempl_month')
    
    df_earn_Construction_month = stream2mongo.getDF('topic_earn_Construction_mon')
    stream2mongo.saveDF2MongoDB(df_earn_Construction_month, 'coll_earn_Construction_month')

    df_financial_Activities_month = stream2mongo.getDF('topic_earn_Financial_Activities_mon')
    stream2mongo.saveDF2MongoDB(df_financial_Activities_month, 'coll_earn_Financial_Activities_month')
    
    df_goods_Producing_month = stream2mongo.getDF('topic_earn_Goods_Producing_mon')
    stream2mongo.saveDF2MongoDB(df_goods_Producing_month, 'coll_earn_Goods_Producing_month')
    
    df_leisure_and_Hospitality_month = stream2mongo.getDF('topic_earn_Leisure_and_Hospitality_mon')
    stream2mongo.saveDF2MongoDB(df_leisure_and_Hospitality_month, 'coll_earn_Leisure_and_Hospitality_month')
    
    df_manufacturing_month = stream2mongo.getDF('topic_earn_Manufacturing_mon')
    stream2mongo.saveDF2MongoDB(df_manufacturing_month, 'coll_earn_Manufacturing_month')
    
    df_private_Service_Providing_month = stream2mongo.getDF('topic_earn_Private_Service_Providing_mon')
    stream2mongo.saveDF2MongoDB(df_private_Service_Providing_month, 'coll_earn_Private_Service_Providing_month')
    
    df_professional_and_Business_Services_month = stream2mongo.getDF('topic_earn_Professional_and_Business_Services_mon')
    stream2mongo.saveDF2MongoDB(df_professional_and_Business_Services_month, 'coll_earn_Professional_and_Business_Services_month')
    
    df_trade_Transportation_and_Utilities_month = stream2mongo.getDF('topic_earn_Trade_Transportation_and_Utilities_mon')
    stream2mongo.saveDF2MongoDB(df_trade_Transportation_and_Utilities_month, 'coll_earn_Trade_Transportation_and_Utilities_month')
    
    stream2mongo.getSparkSession().streams.awaitAnyTermination()