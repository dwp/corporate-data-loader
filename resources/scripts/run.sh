#!/bin/bash

export HADOOP_CLASSPATH=$(find /usr/lib/hbase/ -type f -name '*.jar' \! -name 'slf4j-log4j12*' | xargs | tr ' ' ':'):/etc/hbase/conf:./corporate-data-loader-1.0-SNAPSHOT-all.jar
export HBASE_TABLE='agent_core:agentToDo'
export S3_BUCKET=danc-nifi-stub
export S3_PREFIX=data/agent-core
export MAP_REDUCE_OUTPUT_DIRECTORY=/user/hadoop/import/${1:?Usage: ./run.sh output-directory-number}

hadoop jar ./corporate-data-loader-1.0-SNAPSHOT-all.jar
