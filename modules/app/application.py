import pyspark


class Application(object):
    ctx: pyspark.SparkContext
    hdfs_url: str
    hdfs_api: str

    def __init__(self, app_name: str, host: str, pyfile: str, master="local[2]", log_level="ERROR") -> None:
        self.hdfs_url = f"hdfs://{host}:9000"
        self.hdfs_api = f"http://{host}:14000/webhdfs/v1"
        conf = pyspark.SparkConf() \
            .setMaster(master) \
            .setAppName(app_name)
        self.ctx = pyspark.SparkContext(conf=conf)
        self.ctx.setLogLevel(log_level)
        self.ctx.addPyFile(pyfile)
