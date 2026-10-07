"""Kafka-compatible local sink, with atomic, compressed output parts."""
import gzip
import json
import os
import re
import uuid
from crawlers.common.public_datasets import replace_file
from datetime import datetime, timezone
from pathlib import Path


class JsonlProducer:
    def __init__(self, output_dir='data/raw', batch_size=1000):
        if batch_size < 1:
            raise ValueError('batch_size must be positive')
        self.root = Path(output_dir)
        self.batch_size = batch_size
        self.buffers = {}
        self.record_count = 0

    def send(self, topic, key, value):
        if not re.fullmatch(r'[a-zA-Z0-9_.-]+', topic) or topic in ('.', '..'):
            raise ValueError('Invalid topic name')
        line = json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n'
        self.buffers.setdefault(topic, []).append(line)
        self.record_count += 1
        if sum(len(lines) for lines in self.buffers.values()) >= self.batch_size:
            self.flush()

    def flush(self):
        for topic, lines in self.buffers.items():
            if not lines:
                continue
            directory = self.root / topic.replace('.', '_') / ('dt=' + datetime.now(timezone.utc).date().isoformat())
            directory.mkdir(parents=True, exist_ok=True)
            final = directory / ('part-' + uuid.uuid4().hex + '.jsonl.gz')
            temp = final.with_name('.' + final.name + '.tmp')
            with temp.open('wb') as raw:
                with gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=5, mtime=0) as compressed:
                    for line in lines:
                        compressed.write(line.encode('utf-8'))
                raw.flush()
                os.fsync(raw.fileno())
            replace_file(temp, final)
            lines.clear()

    def close(self):
        self.flush()


def create_producer():
    from crawlers.config import CRAWLER_OUTPUT, OUTPUT_DIR
    if CRAWLER_OUTPUT == 'jsonl':
        return JsonlProducer(OUTPUT_DIR)
    if CRAWLER_OUTPUT != 'kafka':
        raise ValueError('CRAWLER_OUTPUT must be kafka or jsonl')
    from crawlers.common.kafka_producer import SteamKafkaProducer
    return SteamKafkaProducer()
