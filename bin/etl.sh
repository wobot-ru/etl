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
  ./kafka-2-hbase.sh > app_id 2>&1 &
  echo $! > consumer.pid
  # wait 15 min for fb
  sleep 900  

  APP_ID=`grep "Yarn cluster with application id" app_id | awk 'END {print $NF}'`
  JOB_ID=`sed -n 's/^.*ID:\s\+\([0-9a-z]\+\).*$/\1/p' app_id | head -n1`

  echo `date` ": Cancelling job" "$JOB_ID"
  $FLINK_BIN/flink cancel "$JOB_ID" --jobmanager yarn-cluster -yid "$APP_ID"
  rm app_id

  echo `date` ": Start building HBase views..."
  ./hbase-build-view.sh
  echo `date` ": Finish building HBase views"


  echo `date` ": Start indexing ..."
  ./hbase-2-es.sh
  echo `date` ": Finish indexing"

  echo `date` ": Start cleaning HBase processing tables..."
  hbase shell <<END 
       truncate 'profile-to-process'
       truncate 'post-to-process'
       truncate 'post-to-es'
       truncate 'post'
       exit
END
  echo `date` ": Finish cleaning HBase processing tables"

  echo `date` ": Start cleaning tmp directories..."
  hadoop fs -rm -r -skipTrash tmp/post-to-es/*
  echo `date` ": Finish cleaning tmp directories"

done