# corporate-data-loader

## corporate_data_loader

This repo contains Makefile to fit the standard pattern.
This repo is a base to create new non-Terraform repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`

    export HADOOP_CLASSPATH=$(find /usr/lib/hbase/ -type f -name '*.jar' \! -name 'gson*' | xargs | tr ' ' ':'):/etc/hbase/conf:./corporate-data-loader-1.0-SNAPSHOT-all.jar
    export HBASE_TABLE='agent_core:agentToDo'
    export S3_BUCKET=danc-nifi-stub
    export S3_PREFIX=data/agent-core
    output_directory_number() {
        local pattern='[0-9]+$'
        if [[ -n $MAP_REDUCE_OUTPUT_DIRECTORY ]]; then
                if [[ $MAP_REDUCE_OUTPUT_DIRECTORY =~ $pattern ]]; then
                        local last=${BASH_REMATCH[0]}
                        ((last++))
                        echo $last
                fi
        else
                echo 1
        fi
    }
    export MAP_REDUCE_OUTPUT_DIRECTORY=/user/hadoop/import/$(output_directory_number)
    hadoop jar ./corporate-data-loader-1.0-SNAPSHOT-all.jar
