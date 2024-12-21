'''
Created on 2024. 12. 21.

@author: pyw20
'''

from com.aaa.etl.kafka_mysql_batch import Kafka2MySQLBatch

if __name__ == '__main__':
    batch_unempl_ann = Kafka2MySQLBatch()
    df_unempl_ann = batch_unempl_ann.getDF("topic_unempl_ann")
    batch_unempl_ann.saveDF2MysqlDB(df_unempl_ann, "table_unempl_ann")

    batch_house_income_ann = Kafka2MySQLBatch()
    df_house_income_ann = batch_house_income_ann.getDF("topic_house_income_ann")
    batch_house_income_ann.saveDF2MysqlDB(df_house_income_ann, "table_house_income_ann")

    batch_tax_exemp_ann = Kafka2MySQLBatch()
    df_tax_exemp_ann = batch_tax_exemp_ann.getDF("topic_tax_exemp_ann")
    batch_tax_exemp_ann.saveDF2MysqlDB(df_tax_exemp_ann, "table_tax_exemp_ann")

    batch_civil_force_ann = Kafka2MySQLBatch()
    df_civil_force_ann = batch_civil_force_ann.getDF("topic_civil_force_ann")
    batch_civil_force_ann.saveDF2MysqlDB(df_civil_force_ann, "table_civil_force_ann")

    batch_pov_ann = Kafka2MySQLBatch()
    df_pov_ann = batch_pov_ann.getDF("topic_pov_ann")
    batch_pov_ann.saveDF2MysqlDB(df_pov_ann, "table_pov_ann")

    batch_gdp_ann = Kafka2MySQLBatch()
    df_gdp_ann = batch_gdp_ann.getDF("topic_gdp_ann");
    batch_gdp_ann.saveDF2MysqlDB(df_gdp_ann, "table_gdp_ann")