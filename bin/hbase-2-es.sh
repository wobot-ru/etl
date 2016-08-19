#!/usr/bin/env bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1-SNAPSHOT
export FLINK_BIN=$FLINK_HOME/bin

$FLINK_BIN/flink run --yarnname HBase-export-to-file --jobmanager yarn-cluster -yn 4 -ys 8 -yjm 8192 -ytm 32536 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --hbase-export --hbase-out-dir hdfs://hdp-01/user/nutch/tmp/post-to-es
$FLINK_BIN/flink run --yarnname files-to-es --jobmanager yarn-cluster -yn 4 -ys 2 -yjm 8192 -ytm 32536 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --upload-to-es --es-hosts 192.168.1.136:9300,192.168.1.110:9300 --hbase-out-dir hdfs://hdp-01/user/nutch/tmp/post-to-es
