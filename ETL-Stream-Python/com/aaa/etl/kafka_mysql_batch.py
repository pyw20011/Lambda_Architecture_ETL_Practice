'''
Created on 2024. 12. 21.

@author: pyw20
'''

import configparser

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_csv 


class Kafka2MySQLBatch(object):

    def __init__(self):

        self._config = configparser.ConfigParser()
        self._config.read('resources/SystemConfig.ini')

        appName = self._config['SPARK_CONFIG']['spark.batch.name']
        self._spark = SparkSession.builder.master("local[*]").appName(appName).getOrCreate()

    def getDF(self, kafka_topic):
        df = self._spark.read.format("kafka")\
            .option("kafka.bootstrap.servers", self._config['KAFKA_CONFIG']['kafka.brokerlist'])\
            .option("subscribe", kafka_topic)\
            .option("startingOffsets", self._config['KAFKA_CONFIG']['kafka.resetType'])\
            .load()
        df = df.selectExpr("CAST(value AS STRING) as column").filter(col("column").startswith('date') == False)

        return df

    def saveDF2MysqlDB(self, df, tableName):
        csv_schema = """date DATE,
                        value FLOAT,
                        state STRING,
                        id STRING,
                        title STRING,
                        frequency_short STRING,
                        units_short STRING,
                        seasonal_adjustment_short STRING"""
        
        dfs = df.select(from_csv(df.column, csv_schema).alias("EntityPojo"))\
            .selectExpr("EntityPojo.date", "EntityPojo.value", \
                        "EntityPojo.state", "EntityPojo.id", \
                        "EntityPojo.title", "EntityPojo.frequency_short", \
                        "EntityPojo.units_short", "EntityPojo.seasonal_adjustment_short")

        dfs.show()
        dfs.printSchema()

        mysql_user = self._config['MYSQL_CONFIG']['mysql.user']
        mysql_password = self._config['MYSQL_CONFIG']['mysql.password']

        jdbc_properties = {"user": mysql_user, "password": mysql_password}

        mysql_host_url = self._config['MYSQL_CONFIG']['mysql.host.url']
        dfs.write.mode("overwrite").jdbc(mysql_host_url, tableName, properties=jdbc_properties)


        self._spark.stop()
        