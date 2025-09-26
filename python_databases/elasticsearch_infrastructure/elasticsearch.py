import logging
import time
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Any
import pandas as pd
from custom_python_logger import get_logger
from elasticsearch import Elasticsearch, helpers
from retrying import retry


class UrlProtocol(Enum):
    HTTP = 'http'
    HTTPS = 'https'


class ElasticSearch(ABC):
    def __init__(
        self,
        elk_hostname: str,
        elasticsearch_port: Optional[int],
        kibana_port: Optional[int],
        protocol: UrlProtocol,
        username: Optional[str],
        password: Optional[str]
    ):
        self.logger = get_logger(self.__class__.__name__)
        self._change_elasticsearch_logger()

        self.protocol = protocol
        self.elk_hostname = elk_hostname
        self.username = username
        self.password = password

        self.elasticsearch_port = elasticsearch_port
        if not self.elasticsearch_port:
            self.elasticsearch_url = f'{self.protocol.value}://{self.elk_hostname}'
        else:
            self.elasticsearch_url = f'{self.protocol.value}://{self.elk_hostname}:{self.elasticsearch_port}'
        self.kibana_port = kibana_port

        self.elk_client = None

    @abstractmethod
    def connect_to_elasticsearch(self):
        pass

    @staticmethod
    def _change_elasticsearch_logger(debug_level: int = logging.CRITICAL) -> None:
        tracer = get_logger('elasticsearch')
        tracer.setLevel(debug_level)

    def _set_list_of_docs_quick(self, list_of_docs: list[dict], request_timeout: int = 600) -> None:
        if failed_response := helpers.streaming_bulk(
                self.elk_client,
                list_of_docs,
                raise_on_error=False,
                request_timeout=request_timeout,
                chunk_size=1000
        ):
            for item in failed_response:
                if item[1]['index']['status'] != 201 or item[1]['index']['_shards']['failed'] > 0:
                    self.logger.debug(f'Failed document: \n{item}')

    def _set_list_of_docs_safe(self, list_of_docs: list[dict]) -> None:
        success_response, failed_response = helpers.bulk(self.elk_client, list_of_docs)
        if success_response == len(list_of_docs):
            self.logger.info('All documents were reported successfully')
        else:
            self.logger.error(
                f'Not all documents were reported successfully.\n'
                f'The documents that were not reported successfully: {failed_response}'
            )

    def post_list_of_docs(self, list_of_docs: list[dict], request_timeout: int = 600, quick: bool = False) -> None:
        self.logger.info('Start to report the documents to ELK')

        if quick:
            self._set_list_of_docs_quick(list_of_docs=list_of_docs, request_timeout=request_timeout)
        else:
            self._set_list_of_docs_safe(list_of_docs=list_of_docs)
        self.logger.info('Finish to report the documents to ELK')

    @staticmethod
    def _get_doc_with_basic_info() -> dict:
        doc = {}

        timestamp = datetime.now(tz=timezone.utc)
        date_and_time_str = timestamp.isoformat()
        date = date_and_time_str.split('T')[0]
        time_part = date_and_time_str.split('T')[1].split('+')[0]

        doc.update(
            {
                'doc_id': timestamp.strftime('%Y%m%dT%H%M%S%f'),
                'timestamp': timestamp,
                'date_and_time_str': date_and_time_str,
                'date_str': date,
                'time_str': time_part,
            }
        )
        return doc

    @staticmethod
    def _add_list_values_as_str(doc: dict, column: str, value: Any) -> None:
        """Add list values as both list and string representation to the provided doc"""
        if type(value) is list and all(type(item) is str for item in value):
            filtered_value = list(filter(lambda item: item is not None, value))
            doc[f'{column}'] = filtered_value
            doc[f'{column}_str'] = ', '.join(filtered_value)

    def _build_document(self, row: dict) -> dict:
        doc = self._get_doc_with_basic_info()
        for custom_field, value in row.items():
            doc[custom_field] = value

            self._add_list_values_as_str(
                doc=doc,
                column=custom_field,
                value=value,
            )
        return doc

    @staticmethod
    def _fill_list_of_docs(index_name: str, doc: dict[str, Any]) -> dict:
        return {
                "_index": index_name,
                "_source": deepcopy(doc)
            }

    def _prepare_documents_for_bulk(self, data: list[dict], doc_index_name: str) -> list[dict]:
        list_of_docs = []
        for index, row in enumerate(data):
            self.logger.debug(f'index: {index + 1}/{len(data)}')
            doc = self._build_document(row=row)
            list_of_docs.append(
                self._fill_list_of_docs(index_name=doc_index_name, doc=doc)
            )
        return list_of_docs

    def post_list_of_docs_as_bulk_chunk(
        self,
        list_of_docs: list[dict],
        chunk_size=1000,
        time_sleep: int = 60,
        quick: bool = False
    ) -> None:
        for i in range(0, len(list_of_docs), chunk_size):
            self.logger.info(f'index: {i} - {i + chunk_size} / {len(list_of_docs)}')
            chunk = list_of_docs[i:i + chunk_size]
            self.post_list_of_docs(list_of_docs=chunk, quick=quick)
        time.sleep(time_sleep)

    def fill_elk_index_as_bulk(  # post_list_of_docs_as_bulk
        self,
        data: list[dict],
        doc_index_name: str,
        chunk_size=1000,
        time_sleep: int = 1,
        quick: bool = True
    ) -> None:  # use as the main function to send data to the elastic search
        list_of_docs = self._prepare_documents_for_bulk(
            data=data,
            doc_index_name=doc_index_name,
        )

        self.post_list_of_docs_as_bulk_chunk(
            list_of_docs=list_of_docs,
            chunk_size=chunk_size,
            time_sleep=time_sleep,
            quick=quick,
        )

    def check_if_index_exists(self, index: str) -> bool:
        if self.elk_client.indices.exists(index=index):
            self.logger.info(f"The index '{index}' exists.")
            return True
        else:
            self.logger.info(f"The index '{index}' does not exist.")
            return False

    def delete_index(self, index: str) -> None:
        self.elk_client.indices.delete(index=index)
        self.logger.info(f"The index '{index}' was deleted.")

    def check_if_index_exists_and_delete_if_exists(self, index: str) -> bool:
        if self.check_if_index_exists(index=index):
            self.delete_index(index=index)
            return True
        else:
            return False

    def get_documents(self, index: str, query: dict) -> list[dict]:
        try:
            res = self.elk_client.search(index=index, body=query)["hits"]["hits"]
            return [doc["_source"] for doc in res]
        except Exception as e:
            raise Exception(f"Failed to get documents from index '{index}': {str(e)}")

    def convert_dataframes_to_list_of_docs(self, dataframe: pd.DataFrame) -> list:
        data = []
        dataframe_values = dataframe.values.tolist()
        for index, value in enumerate(dataframe_values):
            self.logger.debug(f'index: {index}/{len(dataframe_values)}')
            data.append(value)
        return data


class ElasticSearchOnPrem(ElasticSearch):
    def __init__(
        self,
        elk_hostname: str,
        elasticsearch_port: Optional[int] = 9200,
        kibana_port: Optional[int] = 5602,
        protocol: UrlProtocol = UrlProtocol.HTTPS,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        super().__init__(
            elk_hostname=elk_hostname,
            elasticsearch_port=elasticsearch_port,
            kibana_port=kibana_port,
            protocol=protocol,
            username=username,
            password=password
        )

        self.logger = get_logger(self.__class__.__name__)

        self.connect_to_elasticsearch()

    @retry(stop_max_attempt_number=3, wait_fixed=180000)
    def connect_to_elasticsearch(self):
        if self.username and self.password:
            self.elk_client = Elasticsearch(
                hosts=[self.elasticsearch_url],
                http_auth=(self.username, self.password),
                # ca_certs="/etc/elasticsearch/certs/http_ca.crt",
                verify_certs=False
            )
        else:
            self.elk_client = Elasticsearch(hosts=[f'{self.elasticsearch_url}'])

        if self.elk_client.ping():
            self.logger.info("Elasticsearch on-prem Connection Successful")
        else:
            self.logger.error("Elasticsearch on-prem Connection Failed")
            raise Exception(
                f'Failed to connect to Elasticsearch on-prem on {self.elasticsearch_url}')


class ElasticSearchCloud(ElasticSearch):
    def __init__(
        self,
        elk_hostname: str,
        elasticsearch_port: Optional[int] = 9200,
        kibana_port: Optional[int] = 5602,
        protocol: UrlProtocol = UrlProtocol.HTTPS,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        super().__init__(
            elk_hostname=elk_hostname,
            elasticsearch_port=elasticsearch_port,
            kibana_port=kibana_port,
            protocol=protocol,
            username=username,
            password=password
        )

        self.logger = get_logger(self.__class__.__name__)

        self.connect_to_elasticsearch()

    @retry(stop_max_attempt_number=3, wait_fixed=180000)
    def connect_to_elasticsearch(self):
        self.elk_client = Elasticsearch(cloud_id=self.elk_hostname, http_auth=(self.username, self.password))

        if self.elk_client.ping():
            self.logger.info("Elasticsearch cloud Connection Successful")
        else:
            self.logger.error("Elasticsearch cloud Connection Failed")
            raise Exception(
                f'Failed to connect to Elasticsearch cloud on {self.elk_hostname}')
