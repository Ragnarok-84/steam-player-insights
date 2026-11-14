"""Kafka-compatible local sink, with atomic, compressed output parts and memory buffering."""
import gzip
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from crawlers.common.files import replace_file

logger = logging.getLogger(__name__)


class _TopicBuffer:
    def __init__(self, topic_dir: str, dt: str):
        self.topic_dir = topic_dir
        self.dt = dt
        self.lines: list[str] = []
        self.first_record_time: float = 0.0
        self.callbacks: list[Callable[[], None]] = []

    @property
    def count(self) -> int:
        return len(self.lines)

    def is_empty(self) -> bool:
        return len(self.lines) == 0

    def add(self, line: str):
        if not self.lines:
            self.first_record_time = time.time()
        self.lines.append(line)

    def clear(self):
        self.lines.clear()
        self.first_record_time = 0.0
        self.callbacks.clear()


class JsonlProducer:
    def __init__(
        self,
        output_dir: str = 'data/raw',
        batch_size: Optional[int] = None,
        flush_max_records: Optional[int] = None,
        flush_interval_sec: Optional[float] = None,
    ):
        if flush_max_records is None:
            if batch_size is not None:
                flush_max_records = batch_size
            else:
                flush_max_records = int(os.getenv('CRAWLER_FLUSH_MAX_RECORDS', '500'))
        if flush_interval_sec is None:
            flush_interval_sec = float(os.getenv('CRAWLER_FLUSH_INTERVAL_SEC', '60'))

        if flush_max_records < 1:
            raise ValueError('flush_max_records must be positive')
        if flush_interval_sec <= 0:
            raise ValueError('flush_interval_sec must be positive')

        self.root = Path(output_dir)
        self.flush_max_records = flush_max_records
        self.flush_interval_sec = flush_interval_sec
        self.batch_size = flush_max_records  # For backward compatibility
        self.buffers: dict[tuple[str, str], _TopicBuffer] = {}
        self.record_count = 0

    def send(self, topic: str, key: str, value: dict):
        if not re.fullmatch(r'[a-zA-Z0-9_.-]+', topic) or topic in ('.', '..'):
            raise ValueError('Invalid topic name')
        line = json.dumps(value, ensure_ascii=False, allow_nan=False) + '\n'
        topic_dir = topic.replace('.', '_')
        dt = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        buf_key = (topic_dir, dt)

        if buf_key not in self.buffers:
            self.buffers[buf_key] = _TopicBuffer(topic_dir, dt)

        buf = self.buffers[buf_key]
        buf.add(line)
        self.record_count += 1

        self._check_and_flush()

    def _should_flush(self, buf: _TopicBuffer, now: float) -> bool:
        if buf.is_empty():
            return False
        if buf.count >= self.flush_max_records:
            return True
        if (now - buf.first_record_time) >= self.flush_interval_sec:
            return True
        return False

    def _check_and_flush(self):
        now = time.time()
        for buf_key, buf in list(self.buffers.items()):
            if self._should_flush(buf, now):
                self._flush_buffer(buf_key)

    def _flush_buffer(self, buf_key: tuple[str, str]):
        buf = self.buffers.get(buf_key)
        if not buf or buf.is_empty():
            return

        directory = self.root / buf.topic_dir / f"dt={buf.dt}"
        directory.mkdir(parents=True, exist_ok=True)

        now_ts = int(time.time())
        part_uuid = uuid.uuid4().hex
        final = directory / f"part-{now_ts}-{part_uuid}.jsonl.gz"
        temp = directory / f".{final.name}.tmp"

        with temp.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw, mode='wb', compresslevel=5, mtime=0) as compressed:
                for line in buf.lines:
                    compressed.write(line.encode('utf-8'))
            raw.flush()
            os.fsync(raw.fileno())

        replace_file(temp, final)

        callbacks = list(buf.callbacks)
        buf.clear()

        for cb in callbacks:
            try:
                cb()
            except Exception as e:
                logger.error("Error executing on_flush callback: %s", e)
                raise

    def flush_all(self):
        for buf_key in list(self.buffers.keys()):
            buf = self.buffers[buf_key]
            if not buf.is_empty():
                self._flush_buffer(buf_key)

    def flush(self, force: bool = False):
        if force:
            self.flush_all()
        else:
            self._check_and_flush()

    def close(self):
        self.flush_all()

    def has_pending(self, topic: Optional[str] = None) -> bool:
        if topic is None:
            return any(not buf.is_empty() for buf in self.buffers.values())
        topic_dir = topic.replace('.', '_')
        return any(k[0] == topic_dir and not buf.is_empty() for k, buf in self.buffers.items())

    def add_on_flush(self, topic: str, callback: Callable[[], None]):
        topic_dir = topic.replace('.', '_')
        matching = [buf for k, buf in self.buffers.items() if k[0] == topic_dir and not buf.is_empty()]
        if not matching:
            callback()
            return
        matching[-1].callbacks.append(callback)


def create_producer():
    from crawlers.config import CRAWLER_OUTPUT, OUTPUT_DIR
    if CRAWLER_OUTPUT == 'jsonl':
        return JsonlProducer(OUTPUT_DIR)
    if CRAWLER_OUTPUT != 'kafka':
        raise ValueError('CRAWLER_OUTPUT must be kafka or jsonl')
    from crawlers.common.kafka_producer import SteamKafkaProducer
    return SteamKafkaProducer()
