"""Resolve public Parquet shards and cache downloads within a byte budget."""
import hashlib
import json
import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    replace_file(temp, path)


def replace_file(temp, path):
    # Windows indexers/antivirus can briefly hold an otherwise writable file.
    for attempt in range(6):
        try:
            os.replace(temp, path)
            return
        except PermissionError as exc:
            if getattr(exc, 'winerror', None) not in (5, 32, 33) or attempt == 5:
                raise
            time.sleep(0.05 * 2**attempt)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def resolve_shards(dataset):
    response = requests.get('https://datasets-server.huggingface.co/parquet', params={'dataset': dataset}, timeout=30)
    response.raise_for_status()
    manifest = response.json()
    if manifest.get('partial') or manifest.get('pending') or manifest.get('failed'):
        raise ValueError('Public dataset Parquet conversion is incomplete')
    revision = requests.get(f'https://huggingface.co/api/datasets/{dataset}/revision/refs%2Fconvert%2Fparquet', timeout=30)
    revision.raise_for_status()
    commit = revision.json()['sha']
    files = [dict(item) for item in manifest['parquet_files'] if item['config'] == 'default' and item['split'] == 'train']
    if not files:
        raise ValueError('No default/train Parquet shards found')
    for item in files:
        item['url'] = item['url'].replace('/resolve/refs%2Fconvert%2Fparquet/', f'/resolve/{commit}/')
    return {'dataset': dataset, 'revision': commit, 'files': sorted(files, key=lambda item: item['filename'])}


def shard_cache_path(item, cache_dir):
    token = hashlib.sha256(item['url'].encode()).hexdigest()[:16]
    return Path(cache_dir) / (token + '-' + Path(item['filename']).name)


def download_shard(item, cache_dir, remaining_bytes):
    size = int(item['size'])
    if size <= 0 or size > remaining_bytes:
        raise ValueError('Shard would exceed the configured download budget')
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = shard_cache_path(item, cache_dir)
    if path.exists() and path.stat().st_size == size:
        return path
    temp = path.with_suffix('.part')
    logger.info('Downloading %s (%.1f MB)', item['filename'], size / 1024**2)
    received = 0
    with requests.get(item['url'], stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        with temp.open('wb') as stream:
            for block in response.iter_content(chunk_size=1024 * 1024):
                received += len(block)
                if received > size or received > remaining_bytes:
                    raise ValueError('Download exceeded its declared size or budget')
                stream.write(block)
    if received != size:
        raise ValueError(f'Incomplete download: {received}/{size} bytes')
    replace_file(temp, path)
    return path
