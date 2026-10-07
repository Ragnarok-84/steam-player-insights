"""Select 1,000-2,000 popular games from a public catalog snapshot."""
import argparse
import logging
import time

import pyarrow.parquet as pq

from crawlers.common.public_datasets import resolve_shards, download_shard, write_json, sha256_file
from crawlers.common.targets import rank_games

DATASET = 'FronkonGames/steam-games-dataset'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, choices=range(1000, 2001), default=1000, metavar='1000..2000')
    parser.add_argument('--output', default='data/target_apps.json')
    parser.add_argument('--cache-dir', default='data/cache/catalog')
    parser.add_argument('--max-download-mb', type=int, default=512)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    manifest = resolve_shards(DATASET)
    remaining = args.max_download_mb * 1024**2
    rows, artifacts = [], []
    for item in manifest['files']:
        path = download_shard(item, args.cache_dir, remaining)
        remaining -= item['size']
        parquet = pq.ParquetFile(path)
        columns = [column for column in ['appID', 'name', 'positive', 'negative', 'peak_ccu', 'genres']
                   if column in parquet.schema_arrow.names]
        if len(columns) != 6:
            raise ValueError('Catalog schema changed; required ranking columns are missing')
        for batch in parquet.iter_batches(batch_size=4096, columns=columns):
            rows.extend(batch.to_pylist())
        artifacts.append({'file': str(path), 'sha256': sha256_file(path), 'rows': parquet.metadata.num_rows})
    selected = rank_games(rows, args.limit)
    if len(selected) != args.limit:
        raise ValueError(f'Only {len(selected)} eligible catalog entries available')
    write_json(args.output, selected)
    write_json(args.output + '.manifest.json', {
        **manifest, 'artifacts': artifacts, 'retrieved_at': int(time.time()),
        'catalog_rows': len(rows), 'selected_games': len(selected),
        'ranking': '0.7/(60+review_rank) + 0.3/(60+catalog_peak_ccu_rank)',
        'excluded': 'Software genres (utilities, creative tools, software training and development tools)',
        'note': 'Catalog snapshot popularity, not a live CCU ranking or a historical CCU time series.',
    })
    print(f'Selected {len(selected)} games from {len(rows)} catalog rows: {args.output}')


if __name__ == '__main__':
    main()
