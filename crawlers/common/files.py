"""Atomic local JSON writes shared by the Steam crawlers."""
import json
import logging
import os
import time
from pathlib import Path


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

