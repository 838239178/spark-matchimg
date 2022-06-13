import json
from happybase import ConnectionPool, Connection
from pyspark import RDD, SparkContext

# 这个Java类用来将 HBase 的行键转换为字符串
KEY_READ_CONV = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
# 这个Java类用来将 HBase 查询得到的结果，转换为字符串
VALUE_READ_CONV = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

KEY_WRITE_CONV = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
VALUE_WRITE_CONV = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"

OUTPUT_CONF = {
    "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
    "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
    "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
}


class HbaseClient(object):
    pool: ConnectionPool
    zk_host: str

    def __init__(self, host: str, pool_size=0, namespace="") -> None:
        self.zk_host = host
        if pool_size > 0:
            self.pool = ConnectionPool(
                size=pool_size, host=host, table_prefix=namespace)

    def connection(self) -> Connection:
        return self.pool.connection()

    def _check_zk(self):
        if self.zk_host == "":
            raise BaseException("no zookeeper host")

    def spark_rdd(self, sc: SparkContext, hbase_table_name: str, family: str) -> RDD:
        """
        return RDD's key is str, value is dict[str,str]
        """
        self._check_zk()
        # 配置项要包含 zookeeper 的 ip
        conf = {
            "hbase.zookeeper.quorum": self.zk_host,
            "hbase.mapreduce.inputtable": hbase_table_name
        }
        # 第一个参数是 hadoop 文件的输入类型
        # 第二个参数是 HBase rowkey 的类型
        # 第三个参数是 HBase 值的类型
        # 这三个参数不用改变
        # 读取后的 rdd，每个元素是一个键值对，(key, value)
        rdd = sc.newAPIHadoopRDD(
            "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
            "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "org.apache.hadoop.hbase.client.Result",
            keyConverter=KEY_READ_CONV,
            valueConverter=VALUE_READ_CONV,
            conf=conf
        )

        rdd = rdd.flatMapValues(lambda v: v.split("\n"))\
            .mapValues(json.loads)

        if ':' in family:
            family, qualifier = family.split(':')
            rdd = rdd.filter(lambda kv: kv[1]['columnFamily'] == family and kv[1]['qualifier'] == qualifier)\
                .mapValues(lambda d: {qualifier: d['value']})
        else:
            rdd = rdd.filter(lambda kv: kv[1]['columnFamily'] == family)\
                .mapValues(lambda d: {d['qualifier']: d['value']})\
                .reduceByKey(lambda d1, d2: {**d1, **d2})
        return rdd

    def write_rdd(self, tb: str, rdd: RDD):
        self._check_zk()
        conf = {
            "hbase.zookeeper.quorum": self.zk_host,
            "hbase.mapred.outputtable": tb,
            **OUTPUT_CONF
        }
        rdd.saveAsNewAPIHadoopDataset(
            conf=conf, keyConverter=KEY_WRITE_CONV, valueConverter=VALUE_WRITE_CONV)
