'''
Created on 2024. 12. 21.

@author: pyw20
'''

from com.aaa.etl.hdfs_kafka import Hdfs2Kafka

if __name__ == '__main__':
    
    kafka = Hdfs2Kafka()
    
    kafka.getHdFileInfo('unemployee_annual.csv')
    list_data = kafka.readHdFile('unemployee_annual.csv')
    kafka.sendData2Kafka('topic_unempl_ann', list_data)

    kafka.getHdFileInfo('household_income.csv')
    list_data = kafka.readHdFile('household_income.csv')
    kafka.sendData2Kafka('topic_house_income_ann', list_data)

    kafka.getHdFileInfo('tax_exemption.csv')
    list_data = kafka.readHdFile('tax_exemption.csv')
    kafka.sendData2Kafka('topic_tax_exemp_ann', list_data)

    kafka.getHdFileInfo('civilian_force.csv')
    list_data = kafka.readHdFile('civilian_force.csv')
    kafka.sendData2Kafka('topic_civil_force_ann', list_data)

    kafka.getHdFileInfo('poverty.csv')
    list_data = kafka.readHdFile('poverty.csv')
    kafka.sendData2Kafka('topic_pov_ann', list_data)

    kafka.getHdFileInfo('real_gdp.csv')
    list_data = kafka.readHdFile('real_gdp.csv')
    kafka.sendData2Kafka('topic_gdp_ann', list_data)

    kafka.getHdFileInfo('unemployee_monthly.csv')
    list_data = kafka.readHdFile('unemployee_monthly.csv')
    kafka.sendData2Kafka('topic_unempl_mon', list_data)

    kafka.getHdFileInfo('earnings_Construction.csv')
    list_data = kafka.readHdFile('earnings_Construction.csv')
    kafka.sendData2Kafka('topic_earn_Construction_mon', list_data)

    kafka.getHdFileInfo('earnings_Financial_Activities.csv')
    list_data = kafka.readHdFile('earnings_Financial_Activities.csv')
    kafka.sendData2Kafka('topic_earn_Financial_Activities_mon', list_data)

    kafka.getHdFileInfo('earnings_Goods_Producing.csv')
    list_data = kafka.readHdFile('earnings_Goods_Producing.csv')
    kafka.sendData2Kafka('topic_earn_Goods_Producing_mon', list_data)
    
    kafka.getHdFileInfo('earnings_Leisure_and_Hospitality.csv')
    list_data = kafka.readHdFile('earnings_Leisure_and_Hospitality.csv')
    kafka.sendData2Kafka('topic_earn_Leisure_and_Hospitality_mon', list_data)

    kafka.getHdFileInfo('earnings_Manufacturing.csv')
    list_data = kafka.readHdFile('earnings_Manufacturing.csv')
    kafka.sendData2Kafka('topic_earn_Manufacturing_mon', list_data)

    kafka.getHdFileInfo('earnings_Private_Service_Providing.csv')
    list_data = kafka.readHdFile('earnings_Private_Service_Providing.csv')
    kafka.sendData2Kafka('topic_earn_Private_Service_Providing_mon', list_data)

    kafka.getHdFileInfo('earnings_Professional_and_Business_Services.csv')
    list_data = kafka.readHdFile('earnings_Professional_and_Business_Services.csv')
    kafka.sendData2Kafka('topic_earn_Professional_and_Business_Services_mon', list_data)

    kafka.getHdFileInfo('earnings_Trade_Transportation_and_Utilities.csv')
    list_data = kafka.readHdFile('earnings_Trade_Transportation_and_Utilities.csv')
    kafka.sendData2Kafka('topic_earn_Trade_Transportation_and_Utilities_mon', list_data)