# spark-matchimg
基于 Spark+Hbase+HDFS 大数据高速图片匹配技术

- 运行前使用 `cmd/upload_pkg.sh -i` 上传Python依赖，并再其他节点安装第三方库
- HDFS需要上传`$SPARK_HOME/jars`
- 目录下的`spark-examples_pythonconverter.jar`需要和Hbase的一些jar包的放在一起，配置到`spark-env.sh`中，每个节点的Spark都要一样
