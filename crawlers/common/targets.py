"""A shared, bounded target list and reproducible popularity ranking."""
import json
from pathlib import Path


def is_assigned_to_worker(appid, worker_id, total_workers):
    if total_workers < 1 or not 0 <= worker_id < total_workers:
        raise ValueError('Invalid crawler worker configuration')
    return int(appid) % total_workers == worker_id


def load_target_apps(path, limit=2000):
    if not 1 <= limit <= 2000:
        raise ValueError('Target limit must be between 1 and 2000')
    entries = json.loads(Path(path).read_text(encoding='utf-8'))
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


def rank_games(rows, limit=1000):
    """Weighted reciprocal ranks: 70% review count, 30% catalog peak CCU."""
    if not 1 <= limit <= 2000:
        raise ValueError('Target limit must be between 1 and 2000')
    software_genres = {'Utilities', 'Animation & Modeling', 'Audio Production', 'Design & Illustration',
                       'Photo Editing', 'Software Training', 'Video Production', 'Web Publishing', 'Game Development'}
    games = {}
    for row in rows:
        if set(row.get('genres') or []).intersection(software_genres):
            continue
        appid = int(row.get('appID') or row.get('AppID') or row.get('appid') or 0)
        total = int(row.get('positive') or row.get('Positive') or 0) + int(row.get('negative') or row.get('Negative') or 0)
        peak = int(row.get('peak_ccu') or row.get('Peak CCU') or 0)
        name = row.get('name') or row.get('Name') or ''
        if appid <= 0 or not name or (total <= 0 and peak <= 0):
            continue
        game = {'appid': appid, 'name': name, 'total_reviews': total, 'catalog_peak_ccu': peak}
        if appid not in games or total > games[appid]['total_reviews']:
            games[appid] = game
    review_order = sorted(games, key=lambda a: (-games[a]['total_reviews'], a))
    ccu_order = sorted(games, key=lambda a: (-games[a]['catalog_peak_ccu'], a))
    review_rank = {a: i + 1 for i, a in enumerate(review_order)}
    ccu_rank = {a: i + 1 for i, a in enumerate(ccu_order)}
    for appid, game in games.items():
        game['review_rank'] = review_rank[appid]
        game['ccu_rank'] = ccu_rank[appid]
        game['selection_score'] = 0.7 / (60 + review_rank[appid]) + 0.3 / (60 + ccu_rank[appid])
    return sorted(games.values(), key=lambda game: (-game['selection_score'], game['appid']))[:limit]
