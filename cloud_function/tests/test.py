#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Export data from BigQuery to Yahoo."""
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


class ExportBqDataToYTests(unittest.TestCase):
    """Export data from Google Cloud Storage to BigQuery."""
    def setUp(self):
        """set up."""
        self.bq_project_id = 'test_bq_project_id'
        self.bq_dataset_id = 'test_bq_dataset_id'

        # create model
        self.ebty = main.ExportDataToBQ(
            self.bq_project_id,
            self.bq_dataset_id,
        )
        self.ebty.bigquery_client = ''
        self.ebty.bq_dataset = ''
        self.ebty.api_url_fmt = 'https://s.tgm.yahoo-net.jp/api?site=cdiLM0x&referrer=%s&%s=%s&flag=%s'


    def test_init(self):
        """init."""
        self.assertEqual(self.ebty.bq_project_id, self.bq_project_id)
        self.assertEqual(self.ebty.bq_dataset_id, self.bq_dataset_id)

    def test_get_data_from_bq(self):
        """get data from BigQuery."""
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