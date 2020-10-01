# corporate-data-loader

## Overview
The corporate data loader (hereinafter termed 'CDL') is a tool for disaster recovery. It restores previously streamed 
data to HBase i.e. records that came over the kafka stream. It would be used where a table inconsistency or some other 
hbase issue has caused a data loss.

The data that has been put into HBase by Kafka-to-hbase is additionally written to s3. It is these s3 objects that CDL
processes and writes into HBase.

The process uses map reduce to prepare HFiles directly, hbase is then directed to adopt these files.

Each run of CDL should process files all destined for the same table , so many runs of CDL are needed to perform a full 
restore of each table's streamed data - one for each table in HBase. This is a requirement of the HBases incremental load 
feature which this application uses.  

## Local running
The application can only run on EMR. Until we have a licence for localstack pro (which provides EMR), testing will be
performed by the end to end tests.

## Running in the development environment.
Build the jar

    gradle build

Transfer the jar to s3 (replace the words `development-bucket` with and actual writable bucket in 33.)

    aws --profile dataworks-development s3 cp ./build/libs/corporate-data-loader-1.0-SNAPSHOT-all.jar s3://development-bucket/

Transfer the script to run the jar to the same bucket (one time activity)

    aws --profile dataworks-development s3 cp ./resources/scripts/run.sh s3://development-bucket/

Log onto to the hbase master

    aws ssm ....
    
Fetch the jar and the script

    aws --profile dataworks-development s3 cp s3://development-bucket/corporate-data-loader-1.0-SNAPSHOT-all.jar .
    aws --profile dataworks-development s3 cp s3://development-bucket/run.sh .

Create a table to load into (must match the table name specified in the script)

    hbase shell
    > create table 'agent_core:agentToDo', { NAME => 'cf', VERSIONS => 100 }

.. or truncate an existing table
    
    > truncate 'agent_core:agentToDo'

.. or do nothing and use an existing table.

Kick off the job

    ./run.sh 10
    
The script is very rough and ready and only for kicking off dev runs, alter the exported environment variables therein 
to suit your needs. The number passed in should be chosen to ensure a not existing output directory is used - 
incrementing this number on each run should do the trick.

Check the logs

First you need to get the application Id which loks something like this `application_1601048545520_0023`, look for the 
line like this on the console after the run:

    20/10/01 08:45:43 INFO impl.YarnClientImpl: Submitted application application_1601048545520_0023
 
Then so see the logs:

    yarn logs --applicationId <id-determined-above> 

## AWS Deployment notes

|Name |Purpose |Default value |Notes|
|-----|--------|--------------|-----|
|AWS_REGION|Location of infrastructure|eu-west-2|Default probably suitable for deployed instances |                                                                                                                                            
|AWS_USE_LOCALSTACK|Indicates whether the code is running in localstack environment (for integration tests) |false|Can use default for deployed instances |                                                                                                                                                                 
|HBASE_ARCHIVED_FILE_PATTERN|The filename pattern of the s3 objects persisted by k2hb |(db\.[-\w]+\.[-\w]+)_(\d+)_(\d+)-(\d+)\.jsonl\.gz$|The pattern needs to capture the topic name, the partition, and the first and last offsets. May need to be overridden for equalities data? |                                                                                                           
|HBASE_TABLE|The table to which the data should be written.|data|Needs to target a single table for each run and so to some extent needs to be ably to be set dynamically by whatever initiates a load. |                                                                                                                                                                         
|K2HB_RDS_CA_CERT_PATH|For connection to the metadata store |/certs/AmazonRootCA1.pem|Only needs to be set if running in aws and if CDL needs to write metadatastore entries for each record processed|                                                                                                                                           
|MAP_REDUCE_OUTPUT_DIRECTORY|Where the Hfiles should be written to and from where HBase will pick them up when it is directed to adopt them |/user/hadoop/bulk|These are intermediate files, it should _not_ specify the location in s3 where the files will ultimately reside, hbase takes care of that. A regular file path will indicate a location in HDFS which may be more performant than an s3 location. |                                                                                                                                            
|METADATA_STORE_DATABASE_NAME|If writing to the metadatastore which database to use |metadatastore|Will need to be set explicitly for aws deployed instances.|                                                                                                                                               
|METADATA_STORE_ENDPOINT|If writing to the metadatastore, the database hostname |metadatastore|Will need to be set explicitly for aws deployed instances.|                                                                                                                                                    
|METADATA_STORE_PASSWORD_SECRET_NAME|If writing to the metadatastore, the name of the secret which holds the database user password |password|Will need to be set explicitly for aws deployed instances. |                                                                                                                                             
|METADATA_STORE_PORT|If writing to the metadatastore, the port on which to establish a connection to the metadatastore |3306|Default probably ok for aws deployed instances |                                                                                                                                                                 
|METADATA_STORE_TABLE|If writing to the metadatastore, the table to insert entries onto|ucfs|Should be 'ucfs' or 'equalities' |                                                                                                                                                                
|METADATA_STORE_UPDATE|Whether to additionally write each processed record to the metadatastore for later reconcilliation.  |false| |                                                                                                                                                               
|METADATA_STORE_USERNAME|If writing to the metadatastore, the user to connect as|k2hbwriter|Will need to be set explicitly for aws deployed instances. |                                                                                                                                                       
|METADATA_STORE_USE_AWS_SECRETS|If writing to the metadatastore this should be true so that the application fetches the database usre password from aws secret manager |true| |                                                                                                                                                      
|S3_BUCKET|The bucket containing the objects of previously streamed records|corporatestorage|Will need to be set explicitly for aws deployed instances.  |                                                                                                                                                               
|S3_MAX_CONNECTIONS|How many concurrent s3 connections to allow|1000|Default probably ok.|                                                                                                                                                                  
|S3_PREFIX|The path to the files to be restored |data|Should be one tables worth of data, and must not include objects for other tables.|                           
