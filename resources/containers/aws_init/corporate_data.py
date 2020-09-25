#!/bin/env python

import argparse
import gzip
import json
from typing import Tuple, List

import boto3
import regex


def main():
    args = arguments()
    s3 = s3_client()
    for batch_index in range(args.number_of_files):
        key = s3_key(args.prefix, args.topic, batch_index, args.records_per_file)
        batch = batch_contents(args.topic, args.records_per_file, batch_index)
        body = gzip.compress(batch.encode())
        s3.put_object(Body=body, Bucket=args.bucket, Key=key)
        print(f"Put object '{key}' into bucket '{args.bucket}'.")


def s3_key(prefix: str, topic: str, batch_index: int, records_per_file: int) -> str:
    database, collection = database_and_collection(topic)
    return f"{prefix}/{database}/{collection}/{topic}_0_{batch_index * records_per_file}-{batch_index * records_per_file + records_per_file}.jsonl.gz"


def batch_contents(topic: str, number_of_records: int, batch_index: int) -> str:
    database, collection = database_and_collection(topic)
    return "\n".join(batch_list(batch_index, collection, database, number_of_records))


def batch_list(batch_index: int, collection: str, database: str, number_of_records: int) -> List[str]:
    return [record_contents(database, collection, batch_index, record_index) for record_index in
            range(number_of_records)]


def record_contents(database: str, collection: str, batch_index: int, record_index: int) -> str:
    dictionary = {
        "traceId": f"{batch_index}-{record_index}",
        "unitOfWorkId": f"{batch_index}-{record_index}",
        "@type": "V4",
        "message": {
            "dbObject": f"{batch_index}/{record_index}",
            "encryption": {
                "keyEncryptionKeyId": "cloudhsm:1,2",
                "encryptedEncryptionKey": "ENCRYPTED_ENCRYPTION_KEY",
                "encryptionKeyId": "ENCRYPTION_KEY_ID",
                "initialisationVector": "INITIALISATION_VECTOR"
            },
            "_lastModifiedDateTime": "2020-09-22T23:16:34.260+0000",
            "@type": "MONGO_INSERT",
            "_timeBasedHash": "f5b4848e4f3c2d890f5d16381a0d43175281441d75e1347d500ac5cd3d32815c",
            "_id": {
                "id": f"{database}-{collection}-{batch_index}-{record_index}"
            },
            "db": f"{database}",
            "collection": f"{collection}",
            "timestamp_created_from": "_lastModifiedDateTime"
        },
        "version": f"{database}-1.release_{batch_index}_hotfix.{record_index}",
        "timestamp": "2020-09-22T23:16:34.267+0000"
    }

    return json.dumps(dictionary)


def database_and_collection(topic: str) -> Tuple[str, str]:
    topic_regex = regex.compile(r"^db\.((?:-|\w)+)\.((?:-|\w)+)")
    match = topic_regex.match(topic)
    if not match:
        raise ValueError(f"'{topic}' does not match '{topic_regex}'.")
    return match[1], match[2]


def s3_client():
    return boto3.client(service_name="s3",
                        endpoint_url="http://aws:4566",
                        use_ssl=False,
                        aws_access_key_id="ACCESS_KEY",
                        aws_secret_access_key="SECRET_KEY")


def arguments():
    parser = argparse.ArgumentParser(description="Create synthetic corporate data in the style persisted by k2hb.")
    parser.add_argument('-n', '--number-of-files', default=10, type=int)
    parser.add_argument('-r', '--records-per-file', default=100, type=int)
    parser.add_argument('topic')
    parser.add_argument('bucket')
    parser.add_argument('prefix')
    return parser.parse_args()


if __name__ == '__main__':
    main()
