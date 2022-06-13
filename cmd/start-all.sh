echo "=========start HDFS========="
start-dfs.sh
hdfs --daemon start httpfs
echo "=========start HBASE========="
docker start zk
start-hbase.sh
# hbase-daemon.sh start thrift
echo "=========start YARN========="
start-yarn.sh
echo "=========JPS========="
jps
