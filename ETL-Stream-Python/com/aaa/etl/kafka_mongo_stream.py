'''
Created on 2024. 12. 21.

@author: pyw20
'''

import configparser

from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, from_csv

class Kafka2MongoStream(object):

    def __init__(self):

        self._config = configparser.ConfigParser()
        self._config.read('resources/SystemConfig.ini')

        appName = self._config['SPARK_CONFIG']['spark.stream.name']
        
        self._spark = SparkSession.builder.master("local[*]").appName(appName).getOrCreate()
      
    def getSparkSession(self):
        if self._spark is not None:
            return self._spark
        
        return None
        
    def getDF(self, kafka_topic):
        kafka_brokerlist = self._config['KAFKA_CONFIG']['kafka.brokerlist']
        kafka_resetType = self._config['KAFKA_CONFIG']['kafka.resetType']
        
        df = self._spark.readStream.format("kafka")\
                .option("kafka.bootstrap.servers", kafka_brokerlist)\
                .option("subscribe", kafka_topic)\
                .option("startingOffsets", kafka_resetType)\
                .load()
        df = df.selectExpr("CAST(value AS STRING) as column").filter(col("column").startswith('date') == False)
        
        return df
    
    def process_row(self, each_df, batch_id, coll_name):
                
        mongo_uri = self._config['MONGO_CONFIG']['mongodb.output.uri']
        mongo_db = self._config['MONGO_CONFIG']['mongodb.output.database'] 
        mongo_col = coll_name

        print(mongo_uri + mongo_db + '.' + mongo_col)
        each_df.write.format("mongo").mode("overwrite")\
            .option("uri", mongo_uri).option("database", mongo_db)\
            .option("collection", mongo_col).save()
    
    def saveDF2MongoDB(self, df, coll_name):
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
        #dfs.show()
        dfs.printSchema()

        dfs.writeStream.outputMode('append')\
            .foreachBatch(lambda df, epochId: self.process_row(df, epochId, coll_name)).start()