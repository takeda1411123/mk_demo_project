#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Export data from Google Cloud Storage to BigQuery."""
from google.cloud import bigquery
import os
import sys
import unittest
from unittest import mock

try:
    import main
except ImportError:
    sys.path.append(os.path.abspath(os.path.dirname(__file__)) + '/../')
    import main

class DummyRecord(object):
    """dummy data."""
    def __init__(
        self,
        name: str,
    ):
        """init."""
        self.name = name

class ExportDataToBQTests(unittest.TestCase):
    """Export data from Google Cloud Storage to BigQuery."""
    def setUp(self):
        """set up."""
        self.bq_project_id = 'test_bq_project_id'
        self.bq_dataset_id = 'test_bq_dataset_id'
        self.gcs_bucket_name = 'test_gcs_bucket_name'
        # create model
        self.egtb = main.ExportDataToBQ(
            self.bq_project_id,
            self.bq_dataset_id,
            self.gcs_bucket_name,
        )
        self.egtb.bigquery_client = ''
        self.egtb.bq_dataset = ''
        self.egtb.gcs_client = ''
        self.egtb.gcs_bucket = ''

    def test_init(self):
        """init."""
        self.assertEqual(self.egtb.bq_project_id, self.bq_project_id)
        self.assertEqual(self.egtb.bq_dataset_id, self.bq_dataset_id)
        self.assertEqual(self.egtb.gcs_bucket_name, self.gcs_bucket_name)

    def test_get_conf_data_from_gcs(self):
        """get conf data from GCS."""
        conf_file_name = 'test_conf_file_name'
        return_value = {'test_key': 'test_value'}
        # positive
        with mock.patch.object(
            self.egtb,
            '_ExportDataToBQ__get_conf_data_from_gcs',
            return_value=return_value,
        ):
            self.assertEqual(
                self.egtb.get_conf_data_from_gcs(conf_file_name),
                return_value,
            )
        # negative
        with mock.patch.object(
            self.egtb,
            '_ExportDataToBQ__get_conf_data_from_gcs',
            return_value=None,
            side_effect=ValueError,
        ):
            with self.assertRaises(main.CommonError):
                self.egtb.get_conf_data_from_gcs(conf_file_name)

    def test_get_file_list_from_gcs(self):
        """get file list from GCS."""
        return_value = [
            DummyRecord('20200101/test_20200101_2.tsv'),
            DummyRecord('20200101/test_20200101.tsv'),
            DummyRecord('20200101/test_20200101_1.tsv'),
        ]
        prefix = 'test_prefix'
        # positive
        with mock.patch.object(
            self.egtb,
            '_ExportDataToBQ__get_file_list_from_gcs',
            return_value=return_value,
        ):
            self.assertEqual(
                self.egtb.get_file_list_from_gcs(prefix),
                [
                    '20200101/test_20200101.tsv',
                    '20200101/test_20200101_1.tsv',
                    '20200101/test_20200101_2.tsv',
                ],
            )
        # negative
        with mock.patch.object(
            self.egtb,
            '_ExportDataToBQ__get_file_list_from_gcs',
            return_value=None,
            side_effect=ValueError,
        ):
            with self.assertRaises(main.CommonError):
                self.egtb.get_file_list_from_gcs(prefix)

    def test_make_bq_schema(self):
        """make BigQuery schema."""
        conf_data = [
            'name_1',
            'name_2',
            'name_3',
        ]
        return_value = [
            bigquery.SchemaField(
                'name_1',
                'STRING',
                mode='NULLABLE',
            ),
            bigquery.SchemaField(
                'name_2',
                'STRING',
                mode='NULLABLE',
            ),
            bigquery.SchemaField(
                'name_3',
                'STRING',
                mode='NULLABLE',
            ),
        ]
        # positive
        self.assertEqual(self.egtb.make_bq_schema(conf_data), return_value)
        self.assertEqual(self.egtb.make_bq_schema([]), [])

    def test_make_bq_table(self):
        """make bq table."""
        bq_table_name = 'test_bq_table_name'
        bq_schema = [
            bigquery.SchemaField(
                'schema_1',
                'INTEGER',
                mode='REQUIRED',
            ),
            bigquery.SchemaField(
                'schema_2',
                'STRING',
                mode='REQUIRED',
            ),
            bigquery.SchemaField(
                'schema_3',
                'STRING',
                mode='NULLABLE',
            ),
        ]
        gcs_uri = 'test_gcs_uri'
        write_disposition = 'WRITE_TRUNCATE'
        skip_leading_rows = 1
        # positive
        with mock.patch.object(
            self.egtb,
            '_ExportDataToBQ__make_bq_table',
            return_value=None,
        ):
            self.assertTrue(self.egtb.make_bq_table(
                bq_table_name,
                bq_schema,
                gcs_uri,
                write_disposition,
                skip_leading_rows,
            ))
        # negative
        with mock.patch.object(
            self.egtb,
            '_ExportDataToBQ__make_bq_table',
            return_value=None,
            side_effect=ValueError,
        ):
            with self.assertRaises(main.CommonError):
                self.egtb.make_bq_table(
                    bq_table_name,
                    bq_schema,
                    gcs_uri,
                    write_disposition,
                    skip_leading_rows,
                )