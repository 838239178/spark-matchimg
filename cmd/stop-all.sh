echo "=========STOP HBASE========="
# hbase-daemon.sh stop thrift
stop-hbase.sh
docker stop zk
echo "=========STOP DFS========="
hdfs --daemon stop httpfs
stop-dfs.sh
echo "=========STOP YARN========="
stop-yarn.sh