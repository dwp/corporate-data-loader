#!/usr/bin/env bash

source ./environment.sh

aws_configure
aws_s3_mb corporatestorage
aws_s3_ls
add_objects
