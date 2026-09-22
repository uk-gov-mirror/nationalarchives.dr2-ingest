import hashlib
import io
import itertools
import json
import os
import re
import sqlite3
import sys
import uuid
import math
from collections import defaultdict
from contextlib import closing
from os import listdir
from urllib import parse
from concurrent.futures import ThreadPoolExecutor

import boto3
import oracledb
from botocore.config import Config
from botocore.exceptions import ClientError
from pathlib import PureWindowsPath, PurePosixPath
from dotenv import load_dotenv

load_dotenv()

def create_skeleton_suite_lookup(prefixes, droid_path):
    puid_lookup = {}
    for prefix in prefixes:
        path = os.path.join(droid_path, prefix)

        pattern = re.compile(r'((x-)?fmt-\d{1,5})-.*')

        directory_list = listdir(path)

        for name in directory_list:
            match = pattern.search(name)
            if match:
                puid = match.group(1).replace(f'{prefix}-', f'{prefix}/')
                puid_lookup[puid] = {'file_path': os.path.join(path, name)}

    return puid_lookup


page_size = 100

config = Config(region_name="eu-west-2")

sts_client = boto3.client("sts")
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")

def calculate_checksum(file_path: str, algorithm: str) -> str:
    try:
        hasher = getattr(hashlib, algorithm)()
    except AttributeError:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hasher.update(chunk)

    return hasher.hexdigest()


def group_assets(assets_list):
    grouped = defaultdict(list)
    for asset in assets_list:
        grouped[asset['metadata']['UUID']].append(asset)
    return dict(grouped)


def process_redacted(assets_to_process):
    for asset in assets_to_process:
        if asset['type_ref'] == 100:
            file_reference = asset['metadata']['FileReference']
            iaid = asset['metadata']['IAID']
            redacted_id = asset['rel_ref'] - 1
            asset['metadata'].update({'FileReference': f"{file_reference}/{redacted_id}"})
            asset['metadata'].update({'IAID': f"{iaid}_{redacted_id}"})
    return assets_to_process

def migrate(ic_db_path):
    database_host = os.environ.get("DATABASE_HOST", "localhost")
    account_number = os.environ["ACCOUNT_NUMBER"]
    environment = os.environ["ENVIRONMENT"]
    network_location = os.environ["NETWORK_LOCATION"]
    test_run = os.getenv("TEST_RUN", "true") == "true"
    assets = []
    raw_cache_bucket = f"{environment}-dr2-ingest-dri-migration-cache"
    object_store_bucket = os.environ["OBJECT_STORE_BUCKET"]
    queue_url = f"https://sqs.eu-west-2.amazonaws.com/{account_number}/{environment}-dr2-preingest-dri-importer"
    puid_lookup = create_skeleton_suite_lookup(['fmt', 'x-fmt'], os.environ["DROID_PATH"]) if test_run else {}
    oracledb.defaults.fetch_lobs = False
    oracledb.init_oracle_client(lib_dir=os.environ['CLIENT_LOCATION'])
    conn = oracledb.connect(dsn=f'{database_host}/SDB4', user="STORE", password=os.environ['STORE_PASSWORD'])
    cur = conn.cursor()
    cur.execute("SET TRANSACTION READ ONLY")

    print("\nExecuting SQL query\n")
    with open("ingest_query.sql") as query:
        sql = query.read()
        cur.execute(sql)
    column_indexes = {keys[0]: idx for idx, keys in enumerate(cur.description)}

    while True:
        rows = cur.fetchmany(page_size)
        if not rows:
            break
        for row in rows:
            puid = row[column_indexes["PUID"]]
            asset_uuid = row[column_indexes["UUID"]]
            file_id = row[column_indexes["FILEID"]]
            file_path = row[column_indexes["FILE_PATH"]]
            full_path = row[column_indexes["FULLPATH"]]
            checksums = json.loads(row[column_indexes["FIXITIES"]])
            consignment_reference = row[column_indexes["CONSIGNMENTREFERENCE"]]
            dri_batch_reference = row[column_indexes["DRIBATCHREFERENCE"]]
            rel_ref = row[column_indexes["MANIFESTATIONRELREF"]]
            type_ref = row[column_indexes["TYPEREF"]]
            description_one = row[column_indexes["DESC1"]]
            description_two = row[column_indexes["DESC2"]]
            sort_order = row[column_indexes["SORTORDER"]]
            security_tag = row[column_indexes["SECURITYTAG"]]
            unit_ref = row[column_indexes["UNITREF"]]
            description = description_one if description_one else description_two
            metadata = {
                "Series": row[column_indexes["SERIES"]],
                "UUID": asset_uuid,
                "fileId": file_id,
                "description": description,
                "TransferInitiatedDatetime": str(row[column_indexes["TRANSFERINITIATEDDATETIME"]]),
                "Filename": row[column_indexes["FILENAME"]],
                "FileReference": row[column_indexes["FILEREFERENCE"]],
                "preservicaMetadata": str(row[column_indexes["METADATA"]]),
                "ClientSideOriginalFilepath": file_path,
                "digitalAssetSource": security_tag,
                "sortOrder": sort_order,
                "IAID": unit_ref.replace("-","")
            }
            if consignment_reference:
                metadata["ConsignmentReference"] = consignment_reference
            if dri_batch_reference:
                metadata["driBatchReference"] = dri_batch_reference

            if not consignment_reference and not dri_batch_reference:
                raise ValueError("We need either a consignment reference or a dri batch reference")

            for each_checksum in checksums:
                for algorithm in each_checksum:
                    algorithm_lower = algorithm.lower().replace("-", "")
                    if test_run:
                        file_path = puid_lookup[puid]['file_path']
                        fingerprint = calculate_checksum(file_path, algorithm_lower)
                    else:
                        file_path = full_path
                        fingerprint = each_checksum[algorithm]
                    metadata[f"checksum_{algorithm_lower}"] = fingerprint

            assets.append({'file_path': file_path, 'metadata': metadata, 'rel_ref': rel_ref, 'type_ref': type_ref})

    assets_with_redacted = process_redacted(assets)

    grouped_assets = group_assets(assets_with_redacted)

    def migrate_asset(asset_id):
        assets_list = grouped_assets[asset_id]
        all_metadata = []
        local_assets = []
        for asset in assets_list:
            asset_file_path = asset['file_path']
            asset_metadata = asset['metadata']
            asset_file_id = asset_metadata['fileId']
            all_metadata.append(asset_metadata)
            if test_run:
                base_file_path = asset_file_path
                upload_file_path = asset_file_path
            elif os.name == "nt":
                base_file_path = asset_file_path[1:]
                upload_file_path = PureWindowsPath(network_location, base_file_path)
            else:
                base_file_path = asset_file_path[1:]
                upload_file_path = PurePosixPath(network_location, base_file_path)

            with open(upload_file_path, "rb") as upload_file:
                prefix = f"v1/{asset_id}"
                tags = parse.urlencode([("Series", asset_metadata["Series"])], )
                chunk_size = 5 * 1024 * 1024
                file_size = os.path.getsize(upload_file_path)
                total_parts = math.ceil(file_size / chunk_size)

                init_response = s3_client.create_multipart_upload(
                    Bucket=object_store_bucket,
                    Key=f"{prefix}/{asset_file_id}",
                    Tagging=tags
                )
                upload_id = init_response["UploadId"]
                parts = []

                for part_number in range(1, total_parts + 1):
                    file_chunk = upload_file.read(chunk_size)

                    part_response = s3_client.upload_part(
                        Bucket=object_store_bucket,
                        Key=f"{prefix}/{asset_file_id}",
                        UploadId=upload_id,
                        PartNumber=part_number,
                        Body=file_chunk
                    )

                    parts.append({
                        "ETag": part_response["ETag"],
                        "PartNumber": part_number
                    })
                try:
                    s3_client.complete_multipart_upload(
                        Bucket=object_store_bucket,
                        Key=f"{prefix}/{asset_file_id}",
                        UploadId=upload_id,
                        MultipartUpload={'Parts': parts},
                        IfNoneMatch="*"

                    )
                except ClientError as e:
                    error = e.response["Error"]
                    if error["Code"] == "PreconditionFailed" and error.get("Condition") == "If-None-Match":
                        print(f"Skipping asset {asset_id} as it already exists in {object_store_bucket}")
                    else:
                        print(f"An error occurred: {e}")
                    s3_client.abort_multipart_upload(
                        Bucket=object_store_bucket,
                        Key=f"{prefix}/{asset_file_id}",
                        UploadId=upload_id
                    )

            local_assets.append((asset_file_id, str(base_file_path), asset_id))
        json_bytes = io.BytesIO(json.dumps(all_metadata).encode("utf-8"))
        s3_client.upload_fileobj(json_bytes, raw_cache_bucket, f"{asset_id}.metadata")
        asset_sqs_message = {
            'assetId': asset_id,
            'bucket': object_store_bucket,
            'metadataLocation': f's3://{raw_cache_bucket}/{asset_id}.metadata',
            'filesPrefix': prefix
        }
        return local_assets, json.dumps(asset_sqs_message)

    all_sqs_messages = []
    db_assets = []
    grouped_asset_ids = list(grouped_assets.keys())
    print("Processing Assets...\n")
    with ThreadPoolExecutor(max_workers=20) as executor:
        count = 0
        for migrated_assets, sqs_message in executor.map(migrate_asset, grouped_asset_ids):
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} assets")
            db_assets.extend(migrated_assets)
            all_sqs_messages.append(sqs_message)

    with closing(sqlite3.connect(ic_db_path)) as connection:
        with connection:
            write_to_ic_db(db_assets, connection)

    print("\nSending messages to SQS")
    for batch in itertools.batched(all_sqs_messages, 10):
        entries = [{'MessageBody': msg, 'Id': str(uuid.uuid4())} for msg in batch]
        sqs_client.send_message_batch(QueueUrl=queue_url, Entries=entries)


def write_to_ic_db(assets, connection: sqlite3.Connection):
    print("\nWriting file ids to IC DB")
    for (file_id, path, asset_id) in assets:
        blob_cursor = connection.cursor()
        # If exact row exists (either because there are duplicates in DRI or script has been re-run) then skip,
        # otherwise attempt insert
        blob_cursor.execute("""
            INSERT INTO dri_files (file_id, file_path, asset_id)
            SELECT ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM dri_files
                WHERE file_id = ? AND file_path = ? AND asset_id = ?
            );""", (file_id, path, asset_id) * 2
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        intelligent_caching_db_path = sys.argv[1]
        print("Starting...")
        migrate(intelligent_caching_db_path)
        print("\nCompleted.")
    else:
        raise Exception("Missing arg: Path to SQLite database.")
