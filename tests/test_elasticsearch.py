# test_elasticsearch.py
#
# Tests assert the *intended contract* of each unit, not merely what the current
# implementation happens to do. Where the code violates its contract the test is
# marked xfail(strict=True): it stays red-aware and will fail loudly if the bug
# is silently "fixed" or changes shape. See the module docstring of each xfail
# for the defect being tracked.

from collections.abc import Iterator
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from elasticsearch.helpers import BulkIndexError

from python_databases.elasticsearch_infrastructure import (
    ElasticSearch,
    ElasticSearchCloud,
    ElasticSearchOnPrem,
    UrlProtocol,
)

MODULE = "python_databases.elasticsearch_infrastructure.elasticsearch"


class _FakeElasticSearch(ElasticSearch):
    """Concrete subclass: no real connection, no 180s retry."""

    def connect_to_elasticsearch(self) -> None:
        self.elk_client = MagicMock()


def _ok_item(status: int = 201, shard_failures: int = 0) -> tuple[bool, dict]:
    """A realistic (ok, info) pair as yielded by helpers.streaming_bulk for a success."""
    return True, {
        "index": {
            "_index": "idx",
            "status": status,
            "_shards": {"total": 2, "successful": 2 - shard_failures, "failed": shard_failures},
        }
    }


def _failed_item(status: int = 400) -> tuple[bool, dict]:
    """A realistic failed (ok, info) pair -- note: no ``_shards`` key, like the real client."""
    return False, {
        "index": {
            "_index": "idx",
            "status": status,
            "error": {"type": "mapper_parsing_exception", "reason": "boom"},
        }
    }


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


def _sent_docs(mock_helpers: MagicMock) -> list[dict]:
    """The list of bulk actions handed to streaming_bulk (2nd positional arg)."""
    return mock_helpers.streaming_bulk.call_args[0][1]


# --------------------------------------------------------------------------- #
# URL construction
# --------------------------------------------------------------------------- #
def test_url_protocol_enum_maps_to_wire_schemes() -> None:
    assert (UrlProtocol.HTTP.value, UrlProtocol.HTTPS.value) == ("http", "https"), "enum drifted"


def test_url_includes_port_when_given() -> None:
    obj = _FakeElasticSearch("host", 9200, None, UrlProtocol.HTTPS, None, None)
    assert obj.elasticsearch_url == "https://host:9200", f"Got {obj.elasticsearch_url}"


def test_url_omits_port_when_none() -> None:
    obj = _FakeElasticSearch("host", None, None, UrlProtocol.HTTP, None, None)
    assert obj.elasticsearch_url == "http://host", f"Got {obj.elasticsearch_url}"


# --------------------------------------------------------------------------- #
# Document enrichment contract (fill_elk_index_as_bulk -> prepared bulk actions)
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_each_row_becomes_one_bulk_action_targeting_the_index(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(data=[{"n": 1}, {"n": 2}, {"n": 3}], doc_index_name="my-index", chunk_size=100)

    docs = _sent_docs(mock_helpers)
    assert len(docs) == 3, f"expected 3 actions, got {len(docs)}"
    assert [d["_index"] for d in docs] == ["my-index"] * 3, f"wrong index targets: {docs}"
    assert [d["_source"]["n"] for d in docs] == [1, 2, 3], "row payload not preserved"


@patch(f"{MODULE}.helpers")
def test_source_preserves_original_fields_and_adds_string_variant_for_str_lists(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(
        data=[{"name": "svc", "tags": ["a", "b"], "count": 7}], doc_index_name="idx", chunk_size=100
    )

    src = _sent_docs(mock_helpers)[0]["_source"]
    assert src["name"] == "svc", f"Got {src.get('name')}"
    assert src["count"] == 7, f"Got {src.get('count')}"
    assert src["tags"] == ["a", "b"], f"Got {src.get('tags')}"
    assert src["tags_str"] == "a, b", f"str list not joined: {src.get('tags_str')!r}"
    assert "name_str" not in src, "scalar field must not gain a _str variant"
    assert "count_str" not in src, "non-str list/scalar must not gain a _str variant"


@patch(f"{MODULE}.helpers")
def test_doc_id_is_derived_from_the_timestamp(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(data=[{"n": 1}], doc_index_name="idx", chunk_size=100)

    src = _sent_docs(mock_helpers)[0]["_source"]
    ts = src["timestamp"]
    assert isinstance(ts, datetime), f"timestamp must be a datetime, got {type(ts)}"
    assert src["doc_id"] == ts.strftime("%Y%m%dT%H%M%S%f"), "doc_id/timestamp derivation broken"
    assert src["date_and_time_str"].startswith(src["date_str"]), "date_str is not the date half"
    assert src["time_str"] in src["date_and_time_str"], "time_str is not the time half"


@patch(f"{MODULE}.helpers")
def test_bulk_actions_do_not_share_mutable_source_state(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(data=[{"n": 1}, {"n": 2}], doc_index_name="idx", chunk_size=100)

    docs = _sent_docs(mock_helpers)
    docs[0]["_source"]["injected"] = True
    assert "injected" not in docs[1]["_source"], "bulk actions alias the same _source dict"


# --------------------------------------------------------------------------- #
# log_progress (the HEAD commit under test)
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_log_progress_false_emits_no_progress_logging(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(data=[{"n": 1}, {"n": 2}], doc_index_name="idx", chunk_size=1)
    es.logger.debug.assert_not_called()


@patch(f"{MODULE}.helpers")
def test_log_progress_true_logs_each_row_at_debug_and_each_chunk_at_info(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(data=[{"n": i} for i in range(4)], doc_index_name="idx", chunk_size=2, log_progress=True)
    # 4 rows -> 4 per-row debug lines (from _prepare_documents_for_bulk).
    assert es.logger.debug.call_count == 4, f"expected one debug per row, got {es.logger.debug.call_count}"
    # 2 chunks -> at least 2 per-chunk info lines (from post_list_of_docs_as_bulk_chunk).
    assert es.logger.info.call_count >= 2, f"chunk progress not logged at info: {es.logger.info.call_count}"


# --------------------------------------------------------------------------- #
# _add_list_values_as_str behaviour boundaries (pinned via public surface)
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
@pytest.mark.parametrize(
    "value,expect_str_key",
    [
        (["a", "b"], True),
        ([], True),  # empty list: all() is vacuously true
        ([1, 2], False),  # non-str list -> no string variant
        (["a", 1], False),  # mixed list -> no string variant
        ("plain", False),  # scalar -> no string variant
    ],
)
def test_string_variant_only_for_all_string_lists(
    mock_helpers: MagicMock,
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
    value: object,
    expect_str_key: bool,
) -> None:
    mock_helpers.streaming_bulk.side_effect = lambda *a, **kw: iter([_ok_item()])
    es.fill_elk_index_as_bulk(data=[{"f": value}], doc_index_name="idx", chunk_size=100)

    src = _sent_docs(mock_helpers)[0]["_source"]
    assert ("f_str" in src) is expect_str_key, f"f_str presence wrong for {value!r}: {src.get('f_str')!r}"


# --------------------------------------------------------------------------- #
# Quick path: streaming_bulk result inspection
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_quick_all_success_never_raises_even_with_raise_on_error(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_ok_item(), _ok_item()])
    es.post_list_of_docs(list_of_docs=[{}, {}], quick=True, raise_on_error=True)


@patch(f"{MODULE}.helpers")
def test_quick_drives_streaming_bulk_in_result_inspection_mode(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    # These kwargs are load-bearing: raise_on_error=False is what keeps the
    # failure-inspection loop reachable instead of letting helpers raise.
    mock_helpers.streaming_bulk.return_value = iter([_ok_item()])
    es.post_list_of_docs(list_of_docs=[{}], quick=True, request_timeout=42)

    kwargs = mock_helpers.streaming_bulk.call_args.kwargs
    assert kwargs["raise_on_error"] is False, "must inspect results, not let helpers raise"
    assert kwargs["request_timeout"] == 42, f"request_timeout not forwarded: {kwargs.get('request_timeout')}"
    assert kwargs["chunk_size"] == 1000, f"Got {kwargs.get('chunk_size')}"


@patch(f"{MODULE}.helpers")
def test_quick_raises_with_exact_failure_count_when_raise_on_error(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_ok_item(), _failed_item(), _failed_item(503)])
    with pytest.raises(Exception, match=r"Failed to report 2 documents"):
        es.post_list_of_docs(list_of_docs=[{}, {}, {}], quick=True, raise_on_error=True)


@patch(f"{MODULE}.helpers")
def test_quick_does_not_raise_on_failures_when_raise_on_error_false(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_failed_item(), _failed_item()])
    es.post_list_of_docs(list_of_docs=[{}, {}], quick=True, raise_on_error=False)


@patch(f"{MODULE}.helpers")
def test_quick_treats_2xx_with_shard_failures_as_a_failed_document(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    # ok=True, HTTP 201, but a replica shard rejected the write -> must count as failure.
    mock_helpers.streaming_bulk.return_value = iter([_ok_item(status=201, shard_failures=1)])
    with pytest.raises(Exception, match=r"Failed to report 1 documents"):
        es.post_list_of_docs(list_of_docs=[{}], quick=True, raise_on_error=True)


@patch(f"{MODULE}.helpers")
def test_quick_handles_realistic_failure_item_without_shards_key(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    # Regression guard: a real failed item has no "_shards"; inspection must not KeyError.
    mock_helpers.streaming_bulk.return_value = iter([_failed_item()])
    es.post_list_of_docs(list_of_docs=[{}], quick=True, raise_on_error=False)


# --------------------------------------------------------------------------- #
# Safe path: helpers.bulk
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_safe_logs_success_and_no_error_when_all_reported(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.bulk.return_value = (2, [])
    es.post_list_of_docs(list_of_docs=[{}, {}], quick=False)
    es.logger.info.assert_called()
    es.logger.error.assert_not_called()


@patch(f"{MODULE}.helpers")
def test_safe_propagates_bulk_index_error(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    # helpers.bulk raises BulkIndexError on failure by default; the safe path has no
    # handler, so the caller sees it. (raise_on_error is not consulted here at all.)
    mock_helpers.bulk.side_effect = BulkIndexError("2 document(s) failed to index", [{"x": 1}])
    with pytest.raises(BulkIndexError):
        es.post_list_of_docs(list_of_docs=[{}, {}], quick=False, raise_on_error=False)


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.helpers")
def test_quick_flag_selects_streaming_bulk(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.streaming_bulk.return_value = iter([_ok_item()])
    es.post_list_of_docs(list_of_docs=[{}], quick=True)
    mock_helpers.streaming_bulk.assert_called_once()
    mock_helpers.bulk.assert_not_called()


@patch(f"{MODULE}.helpers")
def test_default_path_selects_bulk(
    mock_helpers: MagicMock, es: _FakeElasticSearch  # pylint: disable=redefined-outer-name
) -> None:
    mock_helpers.bulk.return_value = (1, [])
    es.post_list_of_docs(list_of_docs=[{}], quick=False)
    mock_helpers.bulk.assert_called_once()
    mock_helpers.streaming_bulk.assert_not_called()


# --------------------------------------------------------------------------- #
# Chunking
# --------------------------------------------------------------------------- #
def test_chunking_posts_every_document_exactly_once_in_order(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    docs = [{"n": i} for i in range(5)]
    with patch.object(es, "post_list_of_docs") as mock_post:
        es.post_list_of_docs_as_bulk_chunk(list_of_docs=docs, chunk_size=2)

    posted = [c.kwargs["list_of_docs"] for c in mock_post.call_args_list]
    assert posted == [docs[0:2], docs[2:4], docs[4:5]], f"chunk boundaries wrong: {posted}"


def test_chunking_forwards_quick_and_raise_on_error(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    with patch.object(es, "post_list_of_docs") as mock_post:
        es.post_list_of_docs_as_bulk_chunk(list_of_docs=[{}, {}], chunk_size=1, quick=True, raise_on_error=True)
    for c in mock_post.call_args_list:
        assert c.kwargs["quick"] is True and c.kwargs["raise_on_error"] is True, f"flags not forwarded: {c}"


@pytest.mark.xfail(
    strict=True,
    reason="BUG: time.sleep is dedented outside the chunk loop -> an empty input list still "
    "sleeps for time_sleep (60s by default) despite posting nothing.",
)
def test_chunking_empty_input_posts_nothing_and_does_not_sleep(
    es: _FakeElasticSearch, no_sleep: MagicMock  # pylint: disable=redefined-outer-name
) -> None:
    with patch.object(es, "post_list_of_docs") as mock_post:
        es.post_list_of_docs_as_bulk_chunk(list_of_docs=[], chunk_size=2)
    mock_post.assert_not_called()
    no_sleep.assert_not_called()


@pytest.mark.xfail(
    strict=True,
    reason="BUG: time.sleep is dedented outside the chunk loop -> it fires once after all "
    "chunks instead of pausing between them, so time_sleep cannot rate-limit chunked writes.",
)
def test_chunking_sleeps_between_chunks(
    es: _FakeElasticSearch, no_sleep: MagicMock  # pylint: disable=redefined-outer-name
) -> None:
    with patch.object(es, "post_list_of_docs"):
        es.post_list_of_docs_as_bulk_chunk(list_of_docs=[{}] * 6, chunk_size=2, time_sleep=30)
    assert no_sleep.call_args_list == [
        call(30),
        call(30),
    ], f"expected a sleep after each non-final chunk, got {no_sleep.call_args_list}"


# --------------------------------------------------------------------------- #
# Index management
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("present", [True, False])
def test_check_if_index_exists_mirrors_client(
    es: _FakeElasticSearch, present: bool  # pylint: disable=redefined-outer-name
) -> None:
    es.elk_client.indices.exists.return_value = present
    assert es.check_if_index_exists(index="idx") is present, f"expected {present}"
    es.elk_client.indices.exists.assert_called_once_with(index="idx")


def test_delete_index_delegates_to_client(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.delete_index(index="idx")
    es.elk_client.indices.delete.assert_called_once_with(index="idx")


def test_check_and_delete_removes_index_only_when_present(
    es: _FakeElasticSearch,  # pylint: disable=redefined-outer-name
) -> None:
    es.elk_client.indices.exists.return_value = True
    assert es.check_if_index_exists_and_delete_if_exists(index="idx") is True, "expected True"
    es.elk_client.indices.delete.assert_called_once_with(index="idx")


def test_check_and_delete_is_a_noop_when_absent(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.elk_client.indices.exists.return_value = False
    assert es.check_if_index_exists_and_delete_if_exists(index="idx") is False, "expected False"
    es.elk_client.indices.delete.assert_not_called()


# --------------------------------------------------------------------------- #
# get_documents
# --------------------------------------------------------------------------- #
def test_get_documents_returns_sources_in_order(es: _FakeElasticSearch) -> None:  # pylint: disable=redefined-outer-name
    es.elk_client.search.return_value = {"hits": {"hits": [{"_source": {"a": 1}}, {"_source": {"a": 2}}]}}
    query = {"query": {"match_all": {}}}
    assert es.get_documents(index="idx", query=query) == [{"a": 1}, {"a": 2}], "sources not unwrapped"
    es.elk_client.search.assert_called_once_with(index="idx", body=query)


def test_get_documents_wraps_and_chains_client_error(  # pylint: disable=redefined-outer-name
    es: _FakeElasticSearch,
) -> None:
    original = ValueError("kaboom")
    es.elk_client.search.side_effect = original
    with pytest.raises(Exception, match=r"Failed to get documents from index 'idx'") as exc_info:
        es.get_documents(index="idx", query={})
    assert exc_info.value.__cause__ is original, "original error not chained"


# --------------------------------------------------------------------------- #
# convert_dataframes_to_list_of_docs
# --------------------------------------------------------------------------- #
def test_convert_dataframes_returns_one_entry_per_row(  # pylint: disable=redefined-outer-name
    es: _FakeElasticSearch,
) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = es.convert_dataframes_to_list_of_docs(dataframe=df)
    assert len(result) == 2, f"expected one entry per row, got {result}"


@pytest.mark.xfail(
    strict=True,
    reason="BUG: convert_dataframes_to_list_of_docs returns bare value lists (df.values.tolist()), "
    "dropping column names. Its output is not consumable by fill_elk_index_as_bulk / _build_document, "
    "which iterate row.items().",
)
def test_convert_dataframes_output_is_keyed_by_column(  # pylint: disable=redefined-outer-name
    es: _FakeElasticSearch,
) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = es.convert_dataframes_to_list_of_docs(dataframe=df)
    assert result == [{"a": 1, "b": 3}, {"a": 2, "b": 4}], f"column names lost: {result}"


# --------------------------------------------------------------------------- #
# Concrete connect implementations
# --------------------------------------------------------------------------- #
@patch(f"{MODULE}.Elasticsearch")
def test_onprem_without_auth_connects_to_ported_url(mock_es: MagicMock) -> None:
    mock_es.return_value.ping.return_value = True
    client = ElasticSearchOnPrem(elk_hostname="host")
    mock_es.assert_called_once_with(hosts=["https://host:9200"])
    mock_es.return_value.ping.assert_called_once()
    assert client.elk_client is mock_es.return_value, "client not stored"


@patch(f"{MODULE}.Elasticsearch")
def test_onprem_with_auth_sends_credentials_and_disables_cert_check(mock_es: MagicMock) -> None:
    mock_es.return_value.ping.return_value = True
    ElasticSearchOnPrem(elk_hostname="host", username="u", password="p")
    mock_es.assert_called_once_with(hosts=["https://host:9200"], http_auth=("u", "p"), verify_certs=False)


@patch("retrying.time.sleep")
@patch(f"{MODULE}.Elasticsearch")
def test_onprem_raises_identifiable_error_when_ping_fails(mock_es: MagicMock, _sleep: MagicMock) -> None:
    mock_es.return_value.ping.return_value = False
    with pytest.raises(Exception, match=r"Failed to connect to Elasticsearch on-prem"):
        ElasticSearchOnPrem(elk_hostname="host")
    assert mock_es.return_value.ping.call_count == 3, "retry did not run the configured 3 attempts"


@patch(f"{MODULE}.Elasticsearch")
def test_cloud_connects_via_cloud_id(mock_es: MagicMock) -> None:
    mock_es.return_value.ping.return_value = True
    ElasticSearchCloud(elk_hostname="deployment:abc123", username="u", password="p")
    mock_es.assert_called_once_with(cloud_id="deployment:abc123", http_auth=("u", "p"))


@patch("retrying.time.sleep")
@patch(f"{MODULE}.Elasticsearch")
def test_cloud_raises_identifiable_error_when_ping_fails(mock_es: MagicMock, _sleep: MagicMock) -> None:
    mock_es.return_value.ping.return_value = False
    with pytest.raises(Exception, match=r"Failed to connect to Elasticsearch cloud"):
        ElasticSearchCloud(elk_hostname="deployment:abc123", username="u", password="p")
