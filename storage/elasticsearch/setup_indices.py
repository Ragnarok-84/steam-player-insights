import os
import json
import logging
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ES_Setup")

ES_HOST = os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200")
ES_USER = os.getenv("ELASTICSEARCH_USER")
ES_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

def init_indices(mapping_file: str = "storage/elasticsearch/indices_mapping.json"):
    auth = (ES_USER, ES_PASSWORD) if ES_USER and ES_PASSWORD else None
    es = Elasticsearch(hosts=[ES_HOST], basic_auth=auth, verify_certs=False)

    if not es.ping():
        logger.error(f"Cannot connect to Elasticsearch cluster at {ES_HOST}")
        return False

    logger.info(f"Connected to Elasticsearch cluster at {ES_HOST}")

    with open(mapping_file, "r", encoding="utf-8") as f:
        indices_config = json.load(f)

    for index_name, config in indices_config.items():
        if es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' already exists. Skipping.")
        else:
            logger.info(f"Creating index '{index_name}'...")
            es.indices.create(
                index=index_name,
                settings=config.get("settings", {}),
                mappings=config.get("mappings", {})
            )
            logger.info(f"Successfully created index '{index_name}'.")

    return True

if __name__ == "__main__":
    init_indices()
