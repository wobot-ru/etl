#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLINK_HOME=/opt/flink-1.1-SNAPSHOT
export FLINK_BIN=$FLINK_HOME/bin

while true; do
  if [ -e ".STOP" ]
  then
   echo "STOP file found - escaping loop"
   break
  fi
  
  echo `date` ": Starting streaming from Kafka to HBase"
  ./kafka-2-hbase.sh > fb_app_id 2>&1 &
#  echo $! > consumer.pid

  ./kafka-vk-2-hbase.sh > vk_app_id 2>&1 &
#  echo $! > consumer.pid  
  sleep 900

  FB_APP_ID=`grep "Yarn cluster with application id" fb_app_id | awk 'END {print $NF}'`
  FB_JOB_ID=`sed -n 's/^.*ID:\s\+\([0-9a-z]\+\).*$/\1/p' fb_app_id | head -n1`

  VK_APP_ID=`grep "Yarn cluster with application id" vk_app_id | awk 'END {print $NF}'`
  VK_JOB_ID=`sed -n 's/^.*ID:\s\+\([0-9a-z]\+\).*$/\1/p' vk_app_id | head -n1`

  echo `date` ": Cancelling job" "$JOB_ID"
  $FLINK_BIN/flink cancel "$FB_JOB_ID" --jobmanager yarn-cluster -yid "$FB_APP_ID"
  $FLINK_BIN/flink cancel "$VK_JOB_ID" --jobmanager yarn-cluster -yid "$VK_APP_ID"
  rm fb_app_id
  rm vk_app_id

  echo `date` ": Start building HBase views..."
  ./hbase-build-view.sh
  echo `date` ": Finish building HBase views"


  echo `date` ": Start indexing ..."
  ./hbase-2-es.sh
  echo `date` ": Finish indexing"

  echo `date` ": Start cleaning HBase processing tables..."
  hbase shell <<END 
       truncate 'profile-to-process'
       truncate 'post-to-es'
       truncate 'post'
       disable 'post-to-process'
       drop 'post-to-process'
       snapshot 'post-without-profile', 'post-without-profile-snapshot'
       clone_snapshot 'post-without-profile-snapshot', 'post-to-process'
       delete_snapshot 'post-without-profile-snapshot'
       truncate 'post-without-profile'
       exit
END
  echo `date` ": Finish cleaning HBase processing tables"

  echo `date` ": Start cleaning tmp directories..."
  hadoop fs -rm -r -skipTrash tmp/post-to-es/*
  echo `date` ": Finish cleaning tmp directories"

done