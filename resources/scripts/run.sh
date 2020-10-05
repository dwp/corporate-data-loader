#!/bin/bash

export HADOOP_CLASSPATH=$(find /usr/lib/hbase/ -type f -name '*.jar' \! -name 'slf4j-log4j12*' | xargs | tr ' ' ':'):/etc/hbase/conf:./corporate-data-loader-1.0-SNAPSHOT-all.jar
export HBASE_TABLE='calculator:calculationParts'
export MAP_REDUCE_OUTPUT_DIRECTORY=/user/hadoop/import/${1:?Usage: ./run.sh output-directory-number}
export S3_BUCKET=danc-nifi-stub
export S3_PREFIX=data/
export TOPIC_NAME=db.calculator.calculationParts
export METADATA_STORE_USERNAME=k2hbwriter
export METADATA_STORE_PASSWORD_SECRET_NAME=metadata-store-k2hbwriter
export METADATA_STORE_DATABASE_NAME=metadatastore
export METADATA_STORE_ENDPOINT=metadata-store.cluster-cg4gggiy7hda.eu-west-2.rds.amazonaws.com
export METADATA_STORE_PORT=3306
export K2HB_RDS_CA_CERT_PATH=/opt/emr/AmazonRootCA1.pem

hadoop jar ./corporate-data-loader-1.0-SNAPSHOT-all.jar
