# Collection handoff

Baseline backup: `data/backups/baseline_100_20261117_223020/` (raw, state, targets, SHA256 manifest).
Copy the updated repository to the collection machine. Keep its existing `.env`; never send API keys in chat.
Only one machine may collect. Keep the same state directory. Disable trending target additions during the experiment.

## Expand to 500

From the repo root, with crawler dependencies installed and STEAM_API_KEY configured:

```powershell
python -B -m crawlers.build_targets --limit 500 --top-ccu 20 --seed 42 --output data/target_apps.json
```

Check length is 500 and the first 100 entries equal the baseline list before collecting. Do not use --fresh.

```powershell
python -u -B -m crawlers.collect_local --targets data/target_apps.json --limit 500 --output-dir data/raw --state-dir data/crawler_state --interval 1.5 --review-pages 10 --review-days 0 --max-requests 8000
```

Each pass keeps the latest report and writes a uniquely named report under `data/raw/reports/`.
Check status, completed_games, failed_sources, request_errors, and http_429. A finished pass can still contain failed sources.
Import each selected report once using `python tests/results_log.py import-crawl --member A --scale 500 --repeat 1 --dataset <dataset-name> <report-path>`.
Record initial expansion separately from updates. Run at least 3 comparable update passes with identical sources/configuration and repeat=1,2,3; never treat expansion and updates as interchangeable repeats.
`errors` means failed sources; `request_errors` counts transport errors and non-200 HTTP attempts, including retries. `request_error_rate` divides these by requests. HTTP 200 API-level failures remain source failures.
Old reports lack the new metrics; leave them unknown.

## Expand to 1000

Archive the 500 target list and manifest first. Repeat the build command with --limit 1000, seed 42, no --fresh. Confirm the first 500 entries match.
Run the collect command with --limit 1000 and --max-requests 16000. Actual retries may exhaust that budget; inspect status and next_game_index before resuming. Use --start-index only while the target list and worker configuration remain unchanged.
At 1000 games a CCU-only pass takes roughly 25 minutes at interval 1.5. Schedule the next pass after the preceding pass finishes, log start/end times, and report measured start-to-start cadence. Additional review/metadata work increases cadence. Do not retain a claimed 10-minute cadence.

## Freeze and filter

Stop collection after a completed pass. Copy raw and targets plus manifest to a new immutable snapshot directory. Resume collection only against the original directories.

```powershell
python tests/scalability/make_scale_subsets.py --snapshot data/snapshot_1000/raw --targets data/snapshot_1000/target_apps.json --output data/scale
```

Outputs raw_100, raw_500, raw_1000 share the source files/time window and preserve record contents. Each manifest records appids, source hashes, counts, compressed bytes and topic coverage. The script refuses existing output directories and fewer than 1000 targets. If input mutation is detected, discard that output and retry against a frozen snapshot.
Use the same dataset name for processing benchmarks across the three subsets. Check `python tests/results_log.py check` before submission.
