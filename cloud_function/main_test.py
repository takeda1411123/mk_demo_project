#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Export data from BigQuery to Yahoo!."""

from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob
import os
import sys
import urllib.parse
import urllib.request
from urllib.request import urlopen
from google.cloud import error_reporting
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import logging
from time import sleep


class CommonError(Exception):
    """common error class."""
    pass


class ExportBqDataToY(object):
    """Export data from BigQuery to Yahoo!."""
    def __init__(
        self,
        bq_project_id: str,
        bq_dataset_id: str,
    ):
        """init."""
        self.bq_project_id = bq_project_id
        self.bq_dataset_id = bq_dataset_id
        self.bigquery_client = None
        self.bq_dataset = None
        self.api_url_fmt = 'https://s.tgm.yahoo-net.jp/api?site=cdiLM0x&referrer=%s&%s=%s&flag=%s'

    def __connect_bq(func):
        """connect BigQuery."""
        def wrapper(self, *args, **kwargs):
            """wrapper."""
            if self.bigquery_client is None or self.bq_dataset is None:
                self.bigquery_client = bigquery.Client(project=self.bq_project_id)
                self.bq_dataset = self.bigquery_client.dataset(self.bq_dataset_id)
            return func(self, *args, **kwargs)
        return wrapper

    def __bq_query(
        self,
        query: str,
    ) -> QueryJob:
        """get data from BigQuery."""
        # job
        job_config = bigquery.QueryJobConfig()
        return self.bigquery_client.query(
            query,
            job_config=job_config,
        )

    def __get_data_from_bq(
            self,
            query: str,
    ) -> list:
        """get data from BigQuery."""
        try:
            # get
            query_job = self.__bq_query(query)
            # check job
            if len(list(query_job)) == 0:
                raise CommonError('no data in BigQuery.')
        except Exception as e:
            raise CommonError(e)
        return query_job

    @__connect_bq
    def get_data_from_bq(
        self,
        query: str,
    ) -> list:
        """get data from BigQuery."""
        try:
            return self.__get_data_from_bq(query)
        except Exception as e:
            raise CommonError(e)

    def send_api(
        self,
        url_param: dict,
    ) -> bool:
        """post measurement url."""
        try:
            # request
            api_url = self.api_url_fmt % (
                url_param['referrer'],
                url_param['key'],
                urllib.parse.quote(url_param['value']),
                urllib.parse.quote(url_param['flag']),
            )
            '''
            request = urllib.request.Request(api_url)
            response = urlopen(request)
            # check response code
            response_code = response.getcode()
            if response_code != 200:
                msg = 'response code:%d, url:%s.' % (
                    response_code, api_url,
                )
                raise CommonError(msg)
            '''
        except Exception as e:
            msg = 'msg:%s, url param:%s.' % (e, api_url)
            raise CommonError(msg)
        return True


    def send_api(
        self,
        url_param: dict,
    ) -> bool:
        """post measurement url."""
        try:
            # request
            api_url = self.api_url_fmt % (
                url_param['referrer'],
                url_param['key'],
                urllib.parse.quote(url_param['value']),
                urllib.parse.quote(url_param['flag']),
            )
            '''
            request = urllib.request.Request(api_url)
            response = urlopen(request)
            # check response code
            response_code = response.getcode()
            if response_code != 200:
                msg = 'response code:%d, url:%s.' % (
                    response_code, api_url,
                )
                raise CommonError(msg)
            '''
        except Exception as e:
            msg = 'msg:%s, url param:%s.' % (e, api_url)
            raise CommonError(msg)
        return True


def bq_to_yahoo(
        event: dict,
        content,
) -> bool:
    # logging
    logging_client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(logging_client)
    cloud_logger = logging.getLogger('cloudLogger')
    cloud_logger.setLevel(logging.INFO)
    cloud_logger.addHandler(handler)
    # error reporting
    error_reporting_client = error_reporting.Client()

    # start
    func_name = sys._getframe().f_code.co_name
    cloud_logger.info('%s start.' % (func_name))

    # environmet variable
    project_id = os.environ.get('project_id', 'all-project-264506')
    bq_dataset_id = os.environ.get('bq_dataset_id', 'mk_demo_project')
    start_date = os.environ.get('start_date', '20200701')
    end_date = os.environ.get('end_date', '20200731')
    bq_query  = os.environ.get(
        'bq_query',
        'SELECT segmentId, clientId, idfa, adid ' +
        'FROM `{project_id}.{bq_dataset_id}.yahoo_demo1` ' +
        'WHERE created_at BETWEEN {start_date} AND {end_date}'
    ).format(
        project_id=project_id,
        bq_dataset_id=bq_dataset_id,
        start_date=start_date,
        end_date=end_date,
    )


    try:
        success_count = 0
        # create model
        ebty = ExportBqDataToY(
            project_id,
            bq_dataset_id,
        )
        # get data from BigQuery
        bq_data = ebty.get_data_from_bq(bq_query)

        bq_data_length = len(list(bq_data))

        # send data by API
        for row in bq_data:
            url_param_idfa = {}
            url_param_adid = {}
            url_param_gclid = {}
            try:
                if row.idfa != '':
                    # IDFA
                    url_param_idfa['referrer'] = 'idfa_referrer'
                    url_param_idfa['key'] = 'idfa'
                    url_param_idfa['value'] = row.idfa
                    url_param_idfa['flag'] = str(row.segmentId)
                    ebty.send_api(url_param_idfa)
                if row.adid != '':
                    # AAID
                    url_param_adid['referrer'] = 'adid_referrer'
                    url_param_adid['key'] = 'adid'
                    url_param_adid['value'] = row.adid
                    url_param_adid['flag'] = str(row.segmentId)
                    ebty.send_api(url_param_adid)
                # GA client ID
                url_param_gclid['referrer'] = 'gaid_referrer'
                url_param_gclid['key'] = 'ga_client_id'
                url_param_gclid['value'] = row.clientId
                url_param_gclid['flag'] = str(row.segmentId)
                ebty.send_api(url_param_gclid)
            except Exception as e:
                msg = '%s: %s.' % (__file__, e)
                cloud_logger.error(msg)
                error_reporting_client.report(msg)
                continue
            else:
                success_count += 1
            # wait
            sleep(0.01)

    except Exception as e:
        msg = '%s: %s.' % (__file__, e)
        cloud_logger.error(msg)
        error_reporting_client.report(msg)
        return False

    # end
    cloud_logger.info('total send count:%d, success send count:%d.' % (
        bq_data_length,
        success_count,
    ))
    cloud_logger.info('%s end.' % (func_name))
    return True




if __name__ == '__main__':
    event = {'data': 'hoge'}
    context = 'moge'
    bq_to_yahoo(event, context)
    sys.exit(0)