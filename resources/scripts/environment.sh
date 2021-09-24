#!/usr/bin/env bash

# Generates a list of hbase table names from a set of CDL inputs in s3.
cdl_tables() {
    local -r aws_profile=${1:?Usage: ${FUNCNAME[0]} profile aws-profile}
    local -r s3_base_path=${2:?Usage: ${FUNCNAME[0]} profile s3-base-path}

    aws --profile "$aws_profile" s3 ls --recursive "$s3_base_path" \
        | awk '{ FS = "/"; print $NF }' \
        | perl -npe 's/_\d+_\d+[-_]\d+\.jsonl.gz$//' \
        | sort | uniq | sed -e 's/^db\.//' | tr -- '-.' '_:'
}
