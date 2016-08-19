#!/usr/bin/env bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1-SNAPSHOT
export FLINK_BIN=$FLINK_HOME/bin

$FLINK_BIN/flink run --yarnname HBase-Build-Profile-Views --jobmanager yarn-cluster -yn 4 -ys 8 -yjm 8192 -ytm 24536 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --hbase --hbase-build-profile
$FLINK_BIN/flink run --yarnname HBase-Build-Post-Views --jobmanager yarn-cluster -yn 4 -ys 8 -yjm 8192 -ytm 32536 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --hbase --hbase-build-post
$FLINK_BIN/flink run --yarnname HBase-Join-Views --jobmanager yarn-cluster -yn 4 -ys 8 -yjm 8192 -ytm 32536 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --hbase --hbase-build-post-join

                          

