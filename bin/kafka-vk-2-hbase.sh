#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1-SNAPSHOT
export FLINK_BIN=$FLINK_HOME/bin

$FLINK_BIN/flink run --yarnname Kafka-VK-To-Hbase --jobmanager yarn-cluster -yn 6 -ys 6 -yjm 5120 -ytm 16096 -yt $FLINK_HOME/lib $FLINK_HOME/lib/etl-0.2.0-SNAPSHOT.jar --kafka --bootstrap.servers "hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667" --topic-post VK-POST --topic-profile VK-PROFILE --topic-detailed-post VK-MERGED-POST --auto.offset.reset earliest --group.id 201609121-vk-to-hbase