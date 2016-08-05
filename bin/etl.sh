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

  ./kafka2hbase.sh > app_id 2>&1 &
  echo $! > consumer.pid
  sleep 1800

  APP_ID=`grep "Yarn cluster with application id" app_id | awk 'END {print $NF}'`
  JOB_ID=`sed -n 's/^.*ID:\s\+\([0-9a-z]\+\).*$/\1/p' app_id | head -n1`

  echo "Cancelling job" "$JOB_ID"
  $FLINK_BIN/flink cancel "$JOB_ID" --jobmanager yarn-cluster -yid "$APP_ID"
  rm app_id

  echo "Start building HBase views on iteration "$a
  ./hbase-build-view.sh

  echo "Start cleaning HBase processing tables..."
  hbase shell <<END 
       truncate 'profile-to-process'
       exit
END
  echo "Finished cleaning HBase processing tables."

done