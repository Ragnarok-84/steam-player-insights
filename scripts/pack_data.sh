#!/usr/bin/env bash
# Đóng gói dữ liệu để chia sẻ qua Drive.
#   scripts/pack_data.sh raw     -> gói raw cho cả nhóm (chạy pipeline)
#   scripts/pack_data.sh crawl   -> gói crawl cho người giữ quyền crawl (raw + state + danh sách game)
#   scripts/pack_data.sh scale   -> gói data/scale/ (3 tập 100/500/1000 để benchmark)
# Kết quả nằm trong dist/ (đã gitignore), kèm file .sha256.
# Người nhận: giải nén ở thư mục gốc repo:  tar -xf <gói>.tar  &&  sha256sum -c <gói>.tar.sha256
set -euo pipefail
cd "$(dirname "$0")/.."

kind="${1:-}"
case "$kind" in
  raw)   paths=(data/raw) ;;
  crawl) paths=(data/raw data/crawler_state/reviews_v2 data/target_apps.json data/target_apps.json.manifest.json) ;;
  scale) paths=(data/scale) ;;
  *) echo "Dùng: $0 raw|crawl|scale" >&2; exit 2 ;;
esac

for p in "${paths[@]}"; do
  [ -e "$p" ] || { echo "Thiếu $p" >&2; exit 1; }
done

if pgrep -f "crawlers\.(collect_local|steam_)" >/dev/null; then
  echo "Crawler đang chạy. Dừng crawler trước khi đóng gói (raw và state phải cùng thời điểm)." >&2
  exit 1
fi

games=$(python3 -c "import json;print(len(json.load(open('data/target_apps.json'))))" 2>/dev/null || echo "x")
stamp=$(date +%Y%m%d-%H%M)
name="steam_${kind}_${games}g_${stamp}"
mkdir -p dist

info="dist/${name}.INFO.txt"
{
  echo "Gói: ${name}"
  echo "Loại: ${kind}"
  echo "Đóng gói lúc: $(date -Iseconds)"
  echo "Người đóng gói: $(git config user.name 2>/dev/null || whoami)"
  echo "Commit: $(git rev-parse --short HEAD)"
  echo "Số game trong danh sách: ${games}"
  echo "Ngày raw (dt):"
  for src in data/raw/*/; do
    [ -d "$src" ] || continue
    echo "  $(basename "$src"): $(ls "$src" | tr '\n' ' ')"
  done
  if [ "$kind" = crawl ]; then
    echo "File state review: $(ls data/crawler_state/reviews_v2 | wc -l)"
    for r in data/raw/collection-worker-*.json; do
      [ -f "$r" ] || continue
      python3 - "$r" <<'PY'
import json, sys, datetime
d = json.load(open(sys.argv[1]))
end = datetime.datetime.fromtimestamp(d.get("ended_at", 0)).isoformat(timespec="minutes")
print(f"  {sys.argv[1]}: sources={d.get('sources')} status={d.get('status')} "
      f"next_game_index={d.get('next_game_index')} ended_at={end}")
PY
    done
    echo "Lưu ý: chỉ một người crawl tại một thời điểm. Người gửi gói này phải dừng crawl."
  fi
  echo "Nội dung: ${paths[*]}"
} > "$info"

tar -cf "dist/${name}.tar" "${paths[@]}"
(cd dist && sha256sum "${name}.tar" > "${name}.tar.sha256")

echo "Đã tạo:"
ls -lh "dist/${name}".*
