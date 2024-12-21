'''
Created on 2024. 12. 21.

@author: pyw20
'''

from ctypes import CDLL
CDLL("C:\Anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-a623cb30b19b231c2b1b0086b6350736.dll")

from confluent_kafka import Producer 

from pyarrow import fs

import configparser
import os
import subprocess

class Hdfs2Kafka(object):

    def __init__(self):

        classpath = subprocess.Popen(["C:\\hadoop-3.3.2\\bin\\hdfs.cmd", "classpath", "--glob"], stdout=subprocess.PIPE).communicate()[0]

        os.environ["CLASSPATH"] = classpath.decode("utf-8")

        os.environ["ARROW_LIBHDFS_DIR"] = "C:\\hadoop-3.3.2\\lib\\native"
        
        self._hdfs = fs.HadoopFileSystem('localhost', port=9000)
        
        config = configparser.ConfigParser()
        config.read('resources/SystemConfig.ini')
        kafka_brokers = config['KAFKA_CONFIG']['kafka.brokerlist']
        kafka_resetType = config['KAFKA_CONFIG']['kafka.resetType']

        conf = {'bootstrap.servers': kafka_brokers, 'auto.offset.reset': kafka_resetType}
        self._producer = Producer(conf)

    def getHdFileInfo(self, filename):
        f_Info = self._hdfs.get_file_info(filename)
        print('파일 종류 : ' + str(f_Info.type))
        print('파일 경로 : ' + str(f_Info.path))
        print('파일 크기 : ' + str(f_Info.size))
        print('파일 수정 일자 : ' + str(f_Info.mtime))

    def readHdFile(self, filename):
        with self._hdfs.open_input_file(filename) as inf:
            read_data = inf.read().decode('utf-8').splitlines()
            newline = [line.split(",") for line in read_data]

            return newline

    def sendData2Kafka(self, topic, list_line):
        for data in list_line:
            str_tmp = ",".join(data).split(",")
            modified_data = ",".join(str_tmp[:2]) + "," + ",".join(str_tmp[4:])
            print(modified_data)
            
            self._producer.poll(0)
            self._producer.produce(topic, value=modified_data.encode('utf-8'), callback=kafka_producer_callback)
            self._producer.flush()
            

def kafka_producer_callback(err, msg):
    if err is not None:
        print("실패한 메시지: error={}. message={}".format(err, msg))
    else:
        print("전달된 메시지: %s [%d] @ %d\n" %(msg.topic(), msg.partition(), msg.offset()))
        print("message.topic={}".format(msg.topic()))
        print("message.timestamp={}".format(msg.timestamp()))
        print("message.key={}".format(msg.key()))
        print("message.value={}".format(msg.value().decode('utf-8')))
        print("message.partition={}".format(msg.partition()))
        print("message.offset={}".format(msg.offset()))