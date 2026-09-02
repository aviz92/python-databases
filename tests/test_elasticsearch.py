# test_elasticsearch.py

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from python_databases.elasticsearch_infrastructure import (
    ElasticSearch,
    ElasticSearchCloud,
    ElasticSearchOnPrem,
    UrlProtocol,
)

MODULE = "python_databases.elasticsearch_infrastructure.elasticsearch"


class _FakeElasticSearch(ElasticSearch):
    """Concrete subclass that skips the real connection and the 180s retry."""

    def connect_to_elasticsearch(self) -> None:
        self.elk_client = MagicMock()


def _bulk_item(status: int, failed: int) -> tuple[bool, dict]:
    """One (ok, info) tuple shaped like elasticsearch.helpers.streaming_bulk output."""
    return status == 201, {"index": {"status": status, "_shards": {"failed": failed}}}


@pytest.fixture(autouse=True)
def no_sleep() -> Iterator[MagicMock]:
    with patch(f"{MODULE}.time.sleep") as mock_sleep:
        yield mock_sleep


@pytest.fixture
def es() -> _FakeElasticSearch:
    obj = _FakeElasticSearch(
        elk_hostname="localhost",
        elasticsearch_port=9200,
        kibana_port=5602,
        protocol=UrlProtocol.HTTP,
        username=None,
        password=None,
    )
    obj.connect_to_elasticsearch()
    obj.logger = MagicMock()
    return obj


# --------------------------------------------------------------------------- #
# URL construction / enum
# --------------------------------------------------------------------------- #
def test_url_protocol_values_are_http_schemes() -> None:
    assert UrlProtocol.HTTP.value == "http", f"Got {UrlProtocol.HTTP.value}"
    assert UrlProtocol.HTTPS.value == "https", f"Got {UrlProtocol.HTTPS.value}"


def test_elasticsearch_url_with_port_includes_port() -> None:
    obj = _FakeElasticSearch(
        elk_hostname="host",
        elasticsearch_port=9200,
        kibana_port=None,
        protocol=UrlProtocol.HTTPS,
        username=None,
        password=None,
    )
    assert obj.elasticsearch_url == "https://host:9200", f"Got {obj.elasticsearch_url}"


def test_elasticsearch_url_without_port_omits_port() -> None:
    obj = _FakeElasticSearch(
        elk_hostname="host",
        elasticsearch_port=None,
        kibana_port=None,
        protocol=UrlProtocol.HTTP,
        username=None,
        password=None,
    )
    assert obj.elasticsearch_url == "http://host", f"Got {obj.elasticsearch_url}"


# --------------------------------------------------------------------------- #
# fill_elk_index_as_bulk — the documented main entry point.
# Exercises _prepare_documents_for_bulk / _build_document / _get_doc_with_basic_info
# / _add_list_values_as_str / _fill_list_of_docs through the public surface.
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_fill_elk_index_as_bulk_builds_one_enriched_doc_per_row(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_bulk_item(201, 0)])

    es.fill_elk_index_as_bulk(
        data=[{"name": "svc", "tags": ["x", "y"]}],
        doc_index_name="idx",
        chunk_size=10,
    )

    sent_docs = mock_helpers.streaming_bulk.call_args[0][1]
    assert len(sent_docs) == 1, f"Got {len(sent_docs)}"
    assert sent_docs[0]["_index"] == "idx", f"Got {sent_docs[0]['_index']}"
    source = sent_docs[0]["_source"]
    assert source["name"] == "svc", f"Got {source['name']}"
    assert source["tags"] == ["x", "y"], f"Got {source['tags']}"
    assert source["tags_str"] == "x, y", f"Got {source.get('tags_str')}"
    assert "name_str" not in source, "scalar field must not get a _str variant"
    assert {"doc_id", "timestamp", "date_str", "time_str"} <= source.keys(), "basic info missing"


@patch(f"{MODULE}.helpers")
def test_fill_elk_index_as_bulk_splits_rows_into_chunks(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_bulk_item(201, 0)])

    es.fill_elk_index_as_bulk(data=[{"n": i} for i in range(5)], doc_index_name="idx", chunk_size=2)

    assert mock_helpers.streaming_bulk.call_count == 3, f"Got {mock_helpers.streaming_bulk.call_count}"


@patch(f"{MODULE}.helpers")
def test_fill_elk_index_as_bulk_sleeps_once_after_all_chunks(
    mock_helpers: MagicMock, es: _FakeElasticSearch, no_sleep: MagicMock  # pylint: disable=redefined-outer-name
) -> None:
    # Current behaviour: time.sleep sits outside the chunk loop -> one call total.
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_bulk_item(201, 0)])

    es.fill_elk_index_as_bulk(data=[{"n": i} for i in range(6)], doc_index_name="idx", chunk_size=2, time_sleep=30)

    no_sleep.assert_called_once_with(30)


# --------------------------------------------------------------------------- #
# post_list_of_docs — quick vs safe dispatch and raise_on_error semantics
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_quick_uses_streaming_bulk(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_bulk_item(201, 0)])
    es.post_list_of_docs(list_of_docs=[{}], quick=True)
    mock_helpers.streaming_bulk.assert_called_once()
    mock_helpers.bulk.assert_not_called()


@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_safe_uses_bulk(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.bulk.return_value = (1, [])
    es.post_list_of_docs(list_of_docs=[{}], quick=False)
    mock_helpers.bulk.assert_called_once()
    mock_helpers.streaming_bulk.assert_not_called()


@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_quick_raises_when_docs_fail_and_raise_on_error(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_bulk_item(201, 0), _bulk_item(400, 1)])
    with pytest.raises(Exception, match="Failed to report 1 documents"):
        es.post_list_of_docs(list_of_docs=[{}, {}], quick=True, raise_on_error=True)


@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_quick_silent_when_docs_fail_without_raise_on_error(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_bulk_item(400, 1)])
    es.post_list_of_docs(list_of_docs=[{}], quick=True, raise_on_error=False)


@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_safe_logs_info_when_all_reported(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.bulk.return_value = (2, [])
    es.post_list_of_docs(list_of_docs=[{}, {}], quick=False)
    es.logger.error.assert_not_called()


@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_safe_logs_error_on_partial_failure(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.bulk.return_value = (1, ["boom"])
    es.post_list_of_docs(list_of_docs=[{}, {}], quick=False)
    es.logger.error.assert_called_once()


@patch(f"{MODULE}.helpers")
def test_post_list_of_docs_safe_path_ignores_raise_on_error(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    # Known asymmetry: the safe path never honours raise_on_error, unlike the quick path.
    mock_helpers.bulk.return_value = (0, ["boom"])
    es.post_list_of_docs(list_of_docs=[{}], quick=False, raise_on_error=True)
    es.logger.error.assert_called_once()


# --------------------------------------------------------------------------- #
# post_list_of_docs_as_bulk_chunk
# --------------------------------------------------------------------------- #
def test_post_list_of_docs_as_bulk_chunk_splits_into_chunks(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    with patch.object(es, "post_list_of_docs") as mock_post:
        es.post_list_of_docs_as_bulk_chunk(list_of_docs=[{}] * 5, chunk_size=2)
    assert mock_post.call_count == 3, f"Got {mock_post.call_count}"


def test_post_list_of_docs_as_bulk_chunk_sleeps_once_after_all_chunks(
    es: _FakeElasticSearch, no_sleep: MagicMock  # pylint: disable=redefined-outer-name
) -> None:
    with patch.object(es, "post_list_of_docs"):
        es.post_list_of_docs_as_bulk_chunk(list_of_docs=[{}] * 6, chunk_size=2, time_sleep=60)
    no_sleep.assert_called_once_with(60)


# --------------------------------------------------------------------------- #
# index management
# --------------------------------------------------------------------------- #
def test_check_if_index_exists_true_when_client_reports_present(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    es.elk_client.indices.exists.return_value = True
    assert es.check_if_index_exists(index="idx") is True, "expected True"


def test_check_if_index_exists_false_when_client_reports_absent(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    es.elk_client.indices.exists.return_value = False
    assert es.check_if_index_exists(index="idx") is False, "expected False"


def test_delete_index_calls_client_delete(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.delete_index(index="idx")
    es.elk_client.indices.delete.assert_called_once_with(index="idx")


def test_check_and_delete_deletes_when_present(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.elk_client.indices.exists.return_value = True
    assert es.check_if_index_exists_and_delete_if_exists(index="idx") is True, "expected True"
    es.elk_client.indices.delete.assert_called_once_with(index="idx")


def test_check_and_delete_skips_when_absent(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.elk_client.indices.exists.return_value = False
    assert es.check_if_index_exists_and_delete_if_exists(index="idx") is False, "expected False"
    es.elk_client.indices.delete.assert_not_called()


# --------------------------------------------------------------------------- #
# get_documents
# --------------------------------------------------------------------------- #
def test_get_documents_returns_only_sources(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.elk_client.search.return_value = {"hits": {"hits": [{"_source": {"a": 1}}, {"_source": {"a": 2}}]}}
    result = es.get_documents(index="idx", query={"query": {"match_all": {}}})
    assert result == [{"a": 1}, {"a": 2}], f"Got {result}"


def test_get_documents_wraps_client_error_with_index_name(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    es.elk_client.search.side_effect = ValueError("kaboom")
    with pytest.raises(Exception, match="Failed to get documents from index 'idx'"):
        es.get_documents(index="idx", query={})


# --------------------------------------------------------------------------- #
# convert_dataframes_to_list_of_docs
# --------------------------------------------------------------------------- #
def test_convert_dataframes_to_list_of_docs_returns_row_lists(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = es.convert_dataframes_to_list_of_docs(dataframe=df)
    assert result == [[1, 3], [2, 4]], f"Got {result}"


# --------------------------------------------------------------------------- #
# concrete connect implementations
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.Elasticsearch")
def test_onprem_connect_without_auth_pings_and_stores_client(mock_es: MagicMock) -> None:
    mock_es.return_value.ping.return_value = True
    client = ElasticSearchOnPrem(elk_hostname="host")
    assert client.elk_client is mock_es.return_value, "client not stored"
    mock_es.assert_called_once_with(hosts=["https://host:9200"])


@patch(f"{MODULE}.Elasticsearch")
def test_onprem_connect_with_auth_passes_credentials(mock_es: MagicMock) -> None:
    mock_es.return_value.ping.return_value = True
    ElasticSearchOnPrem(elk_hostname="host", username="u", password="p")
    mock_es.assert_called_once_with(hosts=["https://host:9200"], http_auth=("u", "p"), verify_certs=False)


@patch("retrying.time.sleep")
@patch(f"{MODULE}.Elasticsearch")
def test_onprem_connect_failed_ping_raises(mock_es: MagicMock, _mock_sleep: MagicMock) -> None:
    mock_es.return_value.ping.return_value = False
    with pytest.raises(Exception, match="Failed to connect to Elasticsearch on-prem"):
        ElasticSearchOnPrem(elk_hostname="host")


@patch(f"{MODULE}.Elasticsearch")
def test_cloud_connect_uses_cloud_id_and_auth(mock_es: MagicMock) -> None:
    mock_es.return_value.ping.return_value = True
    ElasticSearchCloud(elk_hostname="cloud:abc123", username="u", password="p")
    mock_es.assert_called_once_with(cloud_id="cloud:abc123", http_auth=("u", "p"))


@patch("retrying.time.sleep")
@patch(f"{MODULE}.Elasticsearch")
def test_cloud_connect_failed_ping_raises(mock_es: MagicMock, _mock_sleep: MagicMock) -> None:
    mock_es.return_value.ping.return_value = False
    with pytest.raises(Exception, match="Failed to connect to Elasticsearch cloud"):
        ElasticSearchCloud(elk_hostname="cloud:abc123", username="u", password="p")
