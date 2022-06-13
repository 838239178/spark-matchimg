# -*- coding:utf-8 -*-
import json
import os
import random

import happybase as db
from numpy import isin
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def test_db():
    conn = db.Connection("master")
    try:
        tb = conn.table('student')
        key = str(random.randint(1000, 99999)).encode()
        tb.put(key, {
            b'info:age': str(random.randint(10, 30)).encode()
        })
        print(f"{dict(tb.row(key))}")
    finally:
        conn.close()


def test_sc():
    ss = SparkSession.builder\
        .master("yarn")\
        .appName("test")\
        .config("spark.driver.host", "master")\
        .getOrCreate()
    sc = ss.sparkContext
    rdd = sc.binaryFiles("/images")
    df = ss.createDataFrame(rdd)
    print(df.columns)
    print(df.count())


def test_ping():
    print("ping pong!")
    assert 1 == 1


def test_hbase_rdd_read():
    conf = SparkConf().setMaster('local[*]').setAppName("ReadHBase")
    sc = SparkContext(conf=conf)
    host = 'master'
    table = 'pxmap'
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapreduce.inputtable": table}

    # 这个Java类用来将 HBase 的行键转换为字符串
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    # 这个Java类用来将 HBase 查询得到的结果，转换为字符串
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        conf=conf, keyConverter=keyConv, valueConverter=valueConv
    )

    output = hbase_rdd.flatMapValues(lambda v: v.split('\n'))\
        .mapValues(json.loads)\
        .filter(lambda kv: kv[1]['columnFamily'] == 'info')\
        .mapValues(lambda d: {d['qualifier']: d['value']})\
        .reduceByKey(lambda d1, d2: {**d1, **d2})\
        .count()
    
    assert output > 0, "output nothing"


def test_hbase_rdd_write():
    conf = SparkConf().setMaster('local[2]').setAppName("WriteHBase")
    sc = SparkContext(conf=conf)
    host = 'localhost'
    table = 'student'
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    conf = {
        "hbase.zookeeper.quorum": host,
        "hbase.mapred.outputtable": table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    }

    rawData = ['3,info,name,Rongcheng', '4,info,name,Guanhua']
    # ( rowkey , [ row key , column family , column name , value ] )
    sc.parallelize(rawData)\
        .map(lambda x: (x[0], x.split(',')))\
        .saveAsNewAPIHadoopDataset(conf=conf, keyConverter=keyConv, valueConverter=valueConv)
