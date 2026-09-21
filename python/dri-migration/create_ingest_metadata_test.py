import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import PureWindowsPath, PurePosixPath
from sqlite3 import Connection
from unittest.mock import patch, MagicMock, mock_open, call, Mock, ANY
from parameterized import parameterized
from botocore.exceptions import ClientError
from migrate import create_ingest_metadata


def setup_test(mock_checksum, mock_connect, mock_create_skeleton, rows, mock_write_to_ic_db, test_run):
    os.environ['CLIENT_LOCATION'] = '/test/client'
    os.environ['STORE_PASSWORD'] = 'password'
    os.environ['DROID_PATH'] = '/test/droid'
    os.environ['ACCOUNT_NUMBER'] = '123456789'
    os.environ['ENVIRONMENT'] = 'testenv'
    os.environ['NETWORK_LOCATION'] = '/network-location'
    os.environ['OBJECT_STORE_BUCKET'] = 'testenv-da-object-store'
    os.environ["OBJECT_STORE_ACCOUNT_NUMBER"] = '56789'

    if test_run:
        os.environ['TEST_RUN'] = test_run
    elif 'TEST_RUN' in os.environ:
        os.environ.pop('TEST_RUN')


    mock_create_skeleton.return_value = {
        "fmt/123": {"file_path": "/test/file1"},
        "x-fmt/123": {"file_path": "/test/file2"}
    }
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("PUID",), ("UUID",), ("UNITREF",), ("FILEID",), ("FILE_PATH",), ("FULLPATH",), ("FIXITIES",),
        ("SERIES",), ("DESC1",), ("DESC2",), ("TRANSFERINITIATEDDATETIME",),
        ("CONSIGNMENTREFERENCE",), ("DRIBATCHREFERENCE",), ("FILENAME",),
        ("FILEREFERENCE",), ("METADATA",), ("MANIFESTATIONRELREF",), ("TYPEREF",), ('SORTORDER',), ('SECURITYTAG',)
    ]

    mock_cursor.fetchmany.side_effect = [rows, []]
    mock_connect.return_value.cursor.return_value = mock_cursor
    mock_checksum.return_value = "abc123"
    mock_write_to_ic_db.return_value = True


def verify_function_calls(test: "TestMigrate", is_test_run, mock_connect: Mock,
                          mock_create_skeleton: Mock, mock_checksum: Mock, write_to_ic_db: Mock, ref_error_thrown=False):

    mock_connect.assert_called_with(dsn="localhost/SDB4", user="STORE", password="password")
    if ref_error_thrown:
       mock_create_skeleton.assert_called_with(["fmt", "x-fmt"], "/test/droid")
       mock_checksum.assert_not_called()
       write_to_ic_db.assert_not_called()
    else:
        if is_test_run:
            mock_create_skeleton.assert_called_with(["fmt", "x-fmt"], "/test/droid")
            mock_checksum.assert_called_with("/test/file2", "sha256")
        else:
            mock_create_skeleton.assert_not_called()
            mock_checksum.assert_not_called()

        write_to_ic_args = write_to_ic_db.call_args_list[0][0]
        path = "/test/" if is_test_run else "dri/a/1/test/"
        test.assertEqual([("fileid-xyz", f"{path}file1", "uuid-abc"), ("fileid-xyz", f"{path}file2", "uuid-def")],
                         write_to_ic_args[0])
        test.assertEqual(True, isinstance(write_to_ic_args[1], sqlite3.Connection))


class TestMigrate(unittest.TestCase):
    ic_db_name = "intelligent_caching_test_db.db"
    @parameterized.expand([
        ('tru','test'),
        ('true','abc123'),
        ('false','test'),
        ('test','test'),
        (None,'abc123'),
    ])
    @patch("migrate.create_ingest_metadata.write_to_ic_db")
    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.sqs_client')
    @patch('migrate.create_ingest_metadata.s3_client')
    @patch('migrate.create_ingest_metadata.os.path.getsize', return_value=10 * 1024 * 1024 + 1)
    def test_migrate_s3_sqs(
            self, test_run, checksum, mock_getsize, mock_s3, mock_sqs, mock_checksum,
            mock_create_skeleton, mock_open_file, __, mock_connect, write_to_ic_db
    ):
        row_fmt = [
            "fmt/123", "uuid-abc", "unitref-abc", "fileid-xyz", "/test/file1", "/dri/a/1/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series 1", "desc1", "desc2", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta", "1", "1", 1, "BornDigital"
        ]
        row_x_fmt = [
            "x-fmt/123", "uuid-def", "unitref-def", "fileid-xyz", "/test/file2", "/dri/a/1/test/file2",
            json.dumps([{"SHA256": "test"}]),
            "series 1", "desc1", "desc2", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta", "1", "1", 1, "Surrogate"
        ]
        rows = [row_fmt, row_x_fmt]

        setup_test(mock_checksum, mock_connect, mock_create_skeleton, rows, write_to_ic_db, test_run)

        create_ingest_metadata.migrate(self.ic_db_name)

        if test_run == "true" or test_run is None:
            call_paths = ("/test/file1", "/test/file2")
        else:
            if os.name == "posix":
                call_paths = (PurePosixPath("/network-location/dri/a/1/test/file1"), PurePosixPath("/network-location/dri/a/1/test/file2"))
            else:
                call_paths = (PureWindowsPath("/network-location/dri/a/1/test/file1"), PureWindowsPath("/network-location/dri/a/1/test/file2"))

        self.assertEqual(call("ingest_query.sql"), mock_open_file.call_args_list[0])
        self.assertCountEqual([
            call(call_paths[0], "rb"),
            call(call_paths[1], "rb"),
        ], mock_open_file.call_args_list[1:])

        calls = [
            call(
                Key="v1/uuid-abc/fileid-xyz",
                Bucket="testenv-da-object-store",
                Tagging="Series=series+1",
            ),
            call(
                Key="v1/uuid-def/fileid-xyz",
                Bucket="testenv-da-object-store",
                Tagging="Series=series+1",
            ),
        ]

        self.assertCountEqual(calls, mock_s3.create_multipart_upload.call_args_list)
        upload_part_calls = mock_s3.upload_part.call_args_list
        self.assertEqual(6, len(upload_part_calls))
        self.assertEqual(sorted([1, 2, 3, 1, 2, 3]),
                         sorted([upload[1]["PartNumber"] for upload in upload_part_calls]))
        self.assertEqual(2, mock_s3.complete_multipart_upload.call_count)
        s3_args = mock_s3.upload_fileobj.call_args_list
        sqs_args = mock_sqs.send_message_batch.call_args_list

        sent_ids = {x['Id'] for x in sqs_args[0][1]["Entries"]}
        self.assertEqual(2, len(sent_ids))

        expected_by_uuid = {
            row[1]: {
                "unit_ref": row[2],
                "digital_asset_source": row[19],
            }
            for row in rows
        }
        self.assertEqual(2, len(s3_args))
        self.assertEqual(1, len(sqs_args))

        sent_entries = [json.loads(x['MessageBody']) for x in sqs_args[0][1]["Entries"]]
        sent_entries_by_asset_id = {entry["assetId"]: entry for entry in sent_entries}
        self.assertEqual(set(expected_by_uuid), set(sent_entries_by_asset_id))

        for s3_arg in s3_args:
            bytes_request, bucket, object_key = s3_arg[0]
            metadata_bytes = bytes_request.getvalue()
            metadata = json.loads(metadata_bytes.decode("utf-8"))[0]
            metadata_uuid = metadata["UUID"]
            expected = expected_by_uuid[metadata_uuid]
            unit_ref = expected["unit_ref"]

            self.assertEqual(metadata_uuid, metadata["UUID"])
            self.assertEqual(unit_ref.replace("-", ""), metadata["IAID"])
            self.assertEqual("series 1", metadata["Series"])
            self.assertEqual(checksum, metadata["checksum_sha256"])
            self.assertEqual("meta", metadata["preservicaMetadata"])
            self.assertEqual(expected["digital_asset_source"], metadata["digitalAssetSource"])
            self.assertEqual(1, metadata["sortOrder"])
            self.assertEqual("testenv-dr2-ingest-dri-migration-cache", bucket)
            self.assertEqual(f"{metadata_uuid}.metadata", object_key)

            self.assertEqual("https://sqs.eu-west-2.amazonaws.com/123456789/testenv-dr2-preingest-dri-importer",
                             sqs_args[0][1]["QueueUrl"])
            sent_body = sent_entries_by_asset_id[metadata_uuid]
            self.assertEqual(metadata_uuid, sent_body["assetId"])
            self.assertEqual("testenv-da-object-store", sent_body["bucket"])
            self.assertEqual(f"s3://testenv-dr2-ingest-dri-migration-cache/{metadata_uuid}.metadata",
                             sent_body["metadataLocation"])
            self.assertEqual(f"v1/{metadata_uuid}", sent_body["filesPrefix"])

        is_test_run = test_run == "true" or test_run is None
        verify_function_calls(self, is_test_run, mock_connect, mock_create_skeleton, mock_checksum,
                              write_to_ic_db)

    @patch("migrate.create_ingest_metadata.write_to_ic_db")
    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.sqs_client')
    @patch('migrate.create_ingest_metadata.s3_client')
    @patch('migrate.create_ingest_metadata.os.path.getsize', return_value=1)
    def test_migrate_raises_error_if_consignment_ref_and_batch_ref_are_missing(
            self, mock_getsize, mock_s3, mock_sqs, mock_checksum,
            mock_create_skeleton, ___, ____, mock_connect, write_to_ic_db
    ):
        row = [
            "fmt/123", "uuid-abc", "unitref-abc", "fileid-xyz", "/test/file1", "/dri/a/1/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series 1", "desc1", "desc2", "2021-01-01", None, None,
            "filename.txt", "fileref", "meta", "1", "1", 1, "BornDigital"
        ]
        setup_test(mock_checksum, mock_connect, mock_create_skeleton, [row], write_to_ic_db, None)
        with self.assertRaises(ValueError) as cm:
            create_ingest_metadata.migrate(self.ic_db_name)

        self.assertEqual("We need either a consignment reference or a dri batch reference", str(cm.exception))

        verify_function_calls(self, True, mock_connect, mock_create_skeleton, write_to_ic_db,
                              mock_checksum, ref_error_thrown=True)

    @patch("migrate.create_ingest_metadata.write_to_ic_db")
    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.sqs_client')
    @patch('migrate.create_ingest_metadata.s3_client')
    @patch('migrate.create_ingest_metadata.os.path.getsize', return_value=1)
    def test_migrate_continues_if_completion_raises_precondition_failed_if_none_match(
            self, mock_getsize, mock_s3, mock_sqs, mock_checksum,
            mock_create_skeleton, ___, ____, mock_connect, write_to_ic_db
    ):
        row = [
            "fmt/123", "uuid-abc", "unitref-abc", "fileid-xyz", "/test/file1", "/dri/a/1/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series 1", "desc1", "desc2", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta", "1", "1", 1, "BornDigital"
        ]
        setup_test(mock_checksum, mock_connect, mock_create_skeleton, [row], write_to_ic_db, None)

        precondition_error = ClientError(
            {"Error": {"Code": "PreconditionFailed", "Condition": "If-None-Match", "Message": "At least one of the pre-conditions you specified did not hold"}},
            "PutObject"
        )
        mock_s3.complete_multipart_upload.side_effect = precondition_error

        # Should not raise; the PreconditionFailed/If-None-Match error is expected when the object already exists
        create_ingest_metadata.migrate(self.ic_db_name)

        mock_s3.create_multipart_upload.assert_called_once()
        mock_s3.upload_part.assert_called_once()
        mock_s3.complete_multipart_upload.assert_called_once()
        mock_s3.abort_multipart_upload.assert_called_once_with(
            Bucket="testenv-da-object-store",
            Key="v1/uuid-abc/fileid-xyz",
            UploadId=ANY
        )
        mock_s3.upload_fileobj.assert_called_once()

    @patch("migrate.create_ingest_metadata.write_to_ic_db")
    @patch('oracledb.connect')
    @patch('oracledb.init_oracle_client')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM TEST")
    @patch('migrate.create_ingest_metadata.create_skeleton_suite_lookup')
    @patch('migrate.create_ingest_metadata.calculate_checksum')
    @patch('migrate.create_ingest_metadata.sqs_client')
    @patch('migrate.create_ingest_metadata.s3_client')
    @patch('migrate.create_ingest_metadata.os.path.getsize', return_value=1)
    def test_migrate_aborts_and_continues_after_other_completion_client_errors(
            self, mock_getsize, mock_s3, mock_sqs, mock_checksum,
            mock_create_skeleton, ___, ____, mock_connect, write_to_ic_db
    ):
        row = [
            "fmt/123", "uuid-abc", "unitref-abc", "fileid-xyz", "/test/file1", "/dri/a/1/test/file1",
            json.dumps([{"SHA256": "test"}]),
            "series 1", "desc1", "desc2", "2021-01-01", "consignment", "batch-ref",
            "filename.txt", "fileref", "meta", "1", "1", 1, "BornDigital"
        ]
        setup_test(mock_checksum, mock_connect, mock_create_skeleton, [row], write_to_ic_db, None)

        access_denied_error = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "PutObject"
        )
        mock_s3.complete_multipart_upload.side_effect = access_denied_error

        create_ingest_metadata.migrate(self.ic_db_name)
        mock_s3.complete_multipart_upload.assert_called_once()
        mock_s3.abort_multipart_upload.assert_called_once_with(
            Bucket="testenv-da-object-store",
            Key="v1/uuid-abc/fileid-xyz",
            UploadId=ANY
        )

    def test_skeleton_suite_lookup(self):
        self.test_dir = tempfile.mkdtemp()
        os.environ['DROID_PATH'] = self.test_dir

        self.prefixes = ['fmt', 'x-fmt']
        for prefix in self.prefixes:
            os.mkdir(os.path.join(self.test_dir, prefix))

        self.files = [
            ('fmt', 'fmt-123-signature-id-1.txt'),
            ('fmt', 'fmt-234-signature-id-2.bin'),
            ('fmt', 'other-file.txt'),
            ('x-fmt', 'x-fmt-345-signature-id-5.xml'),
            ('x-fmt', 'not-a-match.doc'),
        ]
        for prefix, filename in self.files:
            with open(os.path.join(self.test_dir, prefix, filename), 'w') as f:
                f.write('test')

        result = create_ingest_metadata.create_skeleton_suite_lookup(self.prefixes, self.test_dir)

        expected_keys = [
            'fmt/123',
            'fmt/234',
            'x-fmt/345',
        ]

        for key in expected_keys:
            self.assertIn(key, result)
            file_path = result[key]['file_path']
            self.assertTrue(os.path.exists(file_path))

        self.assertNotIn('fmt/other', result)
        self.assertNotIn('x-fmt/not-a-match', result)

        self.assertTrue(result['fmt/123']['file_path'].endswith('fmt-123-signature-id-1.txt'))
        self.assertTrue(result['fmt/234']['file_path'].endswith('fmt-234-signature-id-2.bin'))
        self.assertTrue(result['x-fmt/345']['file_path'].endswith('x-fmt-345-signature-id-5.xml'))

    def test_calculate_checksum(self):
        self.test_file = tempfile.NamedTemporaryFile(delete=False)
        self.test_file.write(b'Test data for checksum\n')
        self.test_file.close()
        self.file_path = self.test_file.name

        self.expected_md5 = hashlib.md5(b'Test data for checksum\n').hexdigest()
        self.expected_sha256 = hashlib.sha256(b'Test data for checksum\n').hexdigest()

        result = create_ingest_metadata.calculate_checksum(self.file_path, 'md5')
        self.assertEqual(self.expected_md5, result)

        result = create_ingest_metadata.calculate_checksum(self.file_path, 'sha256')
        self.assertEqual(self.expected_sha256, result)

        with self.assertRaises(ValueError) as cm:
            create_ingest_metadata.calculate_checksum(self.file_path, 'notahash')
        self.assertEqual("Unsupported hash algorithm: notahash", str(cm.exception))

    def test_redacted_processing(self):
        metadata_redacted_one = {'UUID': '72918742-af2e-4007-b630-0785c94a7526', 'FileReference': 'ABC/Z', 'IAID': '556a5d2bcf4e4383aaeb1897b180a295'}
        metadata_redacted_two = {'UUID': '059a3c12-812d-45ce-afa0-d3adffb9c8b7', 'FileReference': 'ABC/Z', 'IAID': '28618217be0541119a8d493063b57ba8'}
        metadata_standard = {'UUID': '00117826-c0b7-4485-b16f-6c996f0e331c', 'FileReference': 'DEF/Z', 'IAID': 'f8185e53ac554715ae1f1b7bdee0c3b2'}
        assets = [
            {'type_ref': 100, 'rel_ref': 2, 'metadata': metadata_redacted_one},
            {'type_ref': 1, 'rel_ref': 1, 'metadata': metadata_redacted_two},
            {'type_ref': 1, 'rel_ref': 1, 'metadata': metadata_standard}
        ]
        processed_assets = create_ingest_metadata.process_redacted(assets)

        def count(key, value):
            return sum(1 for asset in processed_assets if asset['metadata'][key] == value)

        self.assertEqual(3, len(processed_assets))
        self.assertEqual(1, count('UUID', '72918742-af2e-4007-b630-0785c94a7526'))
        self.assertEqual(1, count('UUID', '059a3c12-812d-45ce-afa0-d3adffb9c8b7'))
        self.assertEqual(1, count('UUID', '00117826-c0b7-4485-b16f-6c996f0e331c'))
        self.assertEqual(1, count('IAID', '556a5d2bcf4e4383aaeb1897b180a295_1'))
        self.assertEqual(1, count('IAID', '28618217be0541119a8d493063b57ba8'))
        self.assertEqual(1, count('IAID', 'f8185e53ac554715ae1f1b7bdee0c3b2'))
        self.assertEqual(1, count('FileReference', 'ABC/Z'))
        self.assertEqual(1, count('FileReference', 'ABC/Z/1'))
        self.assertEqual(1, count('FileReference', 'DEF/Z'))

    ic_table_name = "dri_files"
    if os.path.exists(ic_db_name):
        os.remove(ic_db_name)

    connection: Connection = sqlite3.connect(ic_db_name)
    blob_cursor = connection.cursor()
    blob_cursor.execute(f"""
        CREATE TABLE "{ic_table_name}" (
            "file_id"    TEXT NOT NULL UNIQUE,
            "file_path"  TEXT NOT NULL UNIQUE,
            "asset_id"   TEXT NOT NULL,
            PRIMARY KEY("file_id")
        )"""
    )

    assets = [
        ("fileId-abc", "/dri/a/1/test/file1", "assetId-abc"),
        ("fileId-def", "/dri/a/1/test/file2", "assetId-abc"),
        ("fileId-ghi", "/dri/a/1/test/file3", "assetId-abc")
    ]

    def empty_table(self):
        self.blob_cursor.execute(f"DELETE FROM {self.ic_table_name};")

    def test_writing_to_ic_db(self):
        self.empty_table()
        create_ingest_metadata.write_to_ic_db(self.assets, self.connection)

        self.blob_cursor.execute(f"SELECT * FROM {self.ic_table_name};")
        blob_info = self.blob_cursor.fetchall()
        self.assertEqual([
            ("fileId-abc", "/dri/a/1/test/file1", "assetId-abc"),
            ("fileId-def", "/dri/a/1/test/file2", "assetId-abc"),
            ("fileId-ghi", "/dri/a/1/test/file3", "assetId-abc")
        ], blob_info)

    def test_an_exact_duplicate_row_should_not_throw_error_but_just_skip_inserting_into_ic_db(self):
        self.empty_table()
        self.blob_cursor.execute(f"INSERT INTO {self.ic_table_name} VALUES ('fileId-abc', '/dri/a/1/test/file1', "
                                 f"'assetId-abc');")

        create_ingest_metadata.write_to_ic_db(self.assets, self.connection)

        self.blob_cursor.execute(f"SELECT * FROM {self.ic_table_name};")
        blob_info = self.blob_cursor.fetchall()
        self.assertEqual([
            ("fileId-abc", "/dri/a/1/test/file1", "assetId-abc"),
            ("fileId-def", "/dri/a/1/test/file2", "assetId-abc"),
            ("fileId-ghi", "/dri/a/1/test/file3", "assetId-abc")
        ], blob_info)

    def test_a_row_with_non_unique_id_should_fail_to_insert_into_ic_db(self):
        self.empty_table()
        self.blob_cursor.execute(f"INSERT INTO {self.ic_table_name} VALUES ('fileId-abc', '/dri/a/1/test/file1', 'assetId-zzz');")

        with self.assertRaises(Exception) as context:
            create_ingest_metadata.write_to_ic_db(self.assets, self.connection)

        self.assertEqual(f"UNIQUE constraint failed: {self.ic_table_name}.file_path", str(context.exception))


    @classmethod
    def tearDownClass(cls):
        cls.connection.commit()
        cls.connection.close()
        if os.path.exists(cls.ic_db_name):
            os.remove(cls.ic_db_name)

if __name__ == "__main__":
    unittest.main()
