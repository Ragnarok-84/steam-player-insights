"""Ghi DataFrame vào Elasticsearch bằng Python client (bulk theo partition).

Không cần connector JVM elasticsearch-spark; chỉ cần gói `elasticsearch` trên driver và executor.
Lỗi ghi được raise để job thất bại rõ ràng thay vì im lặng.
"""
import datetime
import decimal
import os


def es_client():
    from elasticsearch import Elasticsearch
    hosts = os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200").split(",")
    user, password = os.getenv("ELASTICSEARCH_USER"), os.getenv("ELASTICSEARCH_PASSWORD")
    auth = (user, password) if user and password else None
    return Elasticsearch(hosts=hosts, basic_auth=auth, verify_certs=False, request_timeout=60)


def _jsonable(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def doc_id(row: dict, id_cols):
    return "_".join(str(_jsonable(row[c])) for c in id_cols)


def bulk_rows(rows, index: str, id_cols=None, chunk_size: int = 1000):
    from elasticsearch import helpers
    client = es_client()

    def actions():
        for row in rows:
            doc = {k: _jsonable(v) for k, v in row.asDict(recursive=True).items()}
            action = {"_index": index, "_source": doc}
            if id_cols:
                action["_id"] = doc_id(doc, id_cols)
            yield action

    ok, errors = helpers.bulk(client, actions(), chunk_size=chunk_size, raise_on_error=False)
    if errors:
        raise RuntimeError(f"{len(errors)} Elasticsearch bulk errors on {index}; first: {errors[0]}")
    return ok


def write_es(df, index: str, id_cols=None):
    """Upsert theo id (id_cols ghép bằng '_'); không có id_cols thì ES tự sinh id."""
    df.foreachPartition(lambda rows: bulk_rows(rows, index, id_cols))


def es_enabled(es_nodes: str = None) -> bool:
    """Bật ghi ES khi truyền --es-nodes hoặc WRITE_ES=1 (dùng ELASTICSEARCH_HOSTS)."""
    if es_nodes:
        os.environ["ELASTICSEARCH_HOSTS"] = es_nodes
        return True
    return os.getenv("WRITE_ES", "0") == "1"
