"""A bounded target list shared by the Steam API crawlers."""
import json
from pathlib import Path


def is_assigned_to_worker(appid, worker_id, total_workers):
    if total_workers < 1 or not 0 <= worker_id < total_workers:
        raise ValueError('Invalid crawler worker configuration')
    return int(appid) % total_workers == worker_id


def load_target_apps(path, limit=2000):
    if not 1 <= limit <= 2000:
        raise ValueError('Target limit must be between 1 and 2000')
    manifest_path = Path(str(path) + '.manifest.json')
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        if manifest.get('source') != 'steam_api':
            raise ValueError('Targets must come from Steam API; run python -m crawlers.build_targets')
    entries = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if not isinstance(entries, list) or not entries:
        raise ValueError('Target file must contain a nonempty list')
    result = []
    seen = set()
    for entry in entries:
        value = entry.get('appid') if isinstance(entry, dict) else entry
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError('Each target must have a positive integer appid')
        appid = int(value)
        if appid <= 0:
            raise ValueError('Each target must have a positive integer appid')
        if appid not in seen:
            seen.add(appid)
            result.append(appid)
    return result[:limit]
