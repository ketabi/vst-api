import json
import os

# from functools import reduce  # only in Python 3
import urllib3
from elasticsearch import Elasticsearch

from vst_api.indexer.constansts.constatnts import IndexingResult
from vst_api.utils.log import logger

es_user = os.environ.get("VST_ELASTIC_USER", "elastic")
es_pass = os.environ.get("VST_ELASTIC_PASS", "elastic_pass")


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


elastic_url = f"https://{es_user}:{es_pass}@localhost:9200/"
es = Elasticsearch([elastic_url], verify_certs=False, request_timeout=30)


def del_none(d):
    """
    Delete keys with the value ``None`` in a dictionary, recursively.

    This alters the input so you may wish to ``copy`` the dict first.
    """
    # For Python 3, write `list(d.items())`; `d.items()` won’t work
    # For Python 2, write `d.items()`; `d.iteritems()` won’t work
    for key, value in list(d.items()):
        if value is None:
            del d[key]
        elif isinstance(value, dict):
            del_none(value)
    return d  # For convenience


def create_index(index_name):
    request_body = {
        "settings": {},
        "mappings": {
            index_name: {
                "properties": {
                    "point": {"index": "not_analyzed", "type": "string"},
                }
            }
        },
    }
    print("creating 'example_index' index...")
    es.indices.create(index=index_name, body=request_body)


def index_in_elastic(loader, index_name):
    logger.info(
        f"{len(loader)} [index_name:<{index_name}>] will be indexed in elastic"
    )

    ignored = 0
    indexed = 0

    is_json_list = True if hasattr(loader, "json_list") else False

    if is_json_list:
        for row in loader.json_list:

            _id = int(row["IRPC"])
            del row['IRPC']

            try:
                es.index(index=index_name, id=_id, document=row)
                indexed += 1
            except Exception as e:
                ignored += 1
                logger.warning(f"<{index_name}> - {_id} - {e}")
    else:
        for idx, row in loader.df.iterrows():

            d = row.to_json()
            _id = int(idx)

            try:
                es.index(index=index_name, id=_id, document=d)
                indexed += 1
            except Exception as e:
                ignored += 1
                logger.warning(
                    f"{e.message} in index <{index_name}> for row {row['IRPC']}"
                )

    logger.info(
        f"<{index_name}>: {indexed} indexed and {ignored} rows failed to be indexed"
    )

    return indexed, ignored


def update_in_elastic(loader, index_name, sheet_name):
    logger.info(
        f"{len(loader)} [index_name:<{index_name}>] will be indexed in elastic"
    )

    ignored = 0
    indexed = 0
    updated = 0

    is_json_list = True if hasattr(loader, "json_list") else False

    if is_json_list:
        for json_obj in loader.json_list:

            _id = int(json_obj["IRPC"])
            del json_obj['IRPC']

            r = _update_a_row(_id, json_obj, index_name)

            if r == IndexingResult.CREATED:
                indexed += 1
            if r == IndexingResult.UPDATED:
                updated += 1
            if r == IndexingResult.IGNORED:
                ignored += 1

    else:
        for idx, json_obj in loader.df.iterrows():
            d = json.loads(json_obj.to_json())
            _id = int(idx)

            r = _update_a_row(_id, d, index_name)

            if r == IndexingResult.CREATED:
                indexed += 1
            if r == IndexingResult.UPDATED:
                updated += 1
            if r == IndexingResult.IGNORED:
                ignored += 1

    logger.info(
        f"<{index_name}>-<{sheet_name}>: {indexed} indexed and {ignored} rows failed to be indexed and {updated} updated "
    )

    return indexed, ignored, updated


def _update_a_row(_id, json_obj, index_name):
    try:
        if es.exists(index=index_name, id=_id):
            es.update(index=index_name, id=_id, doc=json_obj)
            return IndexingResult.UPDATED
        else:
            es.index(index=index_name, id=_id, document=json_obj)
            logger.debug(f"<{index_name}> - new row for id={_id} created")
            return IndexingResult.CREATED
    except Exception as e:
        logger.warning(
            f"{e.message} in index <{index_name}> for row {_id}"
        )
        return IndexingResult.IGNORED


def index_in_elastic_all(loader, index_name):
    logger.info(
        f"{len(loader)} [index_name:<{index_name}>] will be indexed in elastic"
    )

    ignored = 0
    indexed = 0

    is_json_list = True if hasattr(loader, "json_list") else False

    if is_json_list:
        for row in loader.json_list:

            _id = row["IRPC"]
            del row['IRPC']

            try:
                es.index(index=index_name, id=_id, document=row)
                indexed += 1
            except Exception as e:
                ignored += 1
                logger.warning(f"<{index_name}> - {row['IRPC']} - {e}")
    else:
        for idx, row in loader.df.iterrows():
            d = row.to_json()
            _id = int(idx)
            try:
                es.index(index=index_name, id=_id, document=d)
                indexed += 1
            except Exception as e:
                ignored += 1
                logger.warning(
                    f"{e.message} in index <{index_name}> for row {_id}"
                )

    logger.info(
        f"<{index_name}>: {indexed} indexed and {ignored} rows failed to be indexed"
    )

    return indexed, ignored


def index_json_in_elastic(obj, id, index_name):
    try:
        es.index(index=index_name, id=id, document=obj)
    except Exception as e:
        # ignored += 1
        logger.warning(
            f"<{index_name}> info failed to write in elastic: {e}, {id} "
        )
