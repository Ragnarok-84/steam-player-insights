.DEFAULT_GOAL := help

# Activate your venv first, or override: make b1 PYTHON=venv/Scripts/python.exe
# Hoặc dùng `python pipeline.py ...` (tự đặt đường dẫn local data/raw -> data/lake).
PYTHON ?= python
B1_ARGS ?=
B2_ARGS ?=
B3_ARGS ?=
B4_ARGS ?=
B5_ARGS ?=
B6_ARGS ?=
B7_ARGS ?=
B8_ARGS ?=
S1_ARGS ?=
S2_ARGS ?=
S3_ARGS ?=
MERGE_ARGS ?=

.PHONY: help batch b1 b2 b3 b4 b5 b6 b7 b8 s1 s2 s3 merge

help:
	@echo "make batch  - Run B1, B2, B5, B3, B4, B6, B7, B8 sequentially; stop on failure"
	@echo "make b1     - Ingest Kafka into raw storage"
	@echo "make b2     - Clean raw reviews, CCU and game metadata"
	@echo "make b3     - Compute daily/period/segment statistics (after B5)"
	@echo "make b4     - Analyze playtime buckets and retention"
	@echo "make b5     - Analyze aspects, sentiment and TF-IDF keywords"
	@echo "make b6     - Rank trending games and event impact"
	@echo "make b7     - Compute baselines for S2/S3"
	@echo "make b8     - Cluster games with K-Means"
	@echo "make s1     - Stream review metrics"
	@echo "make s2     - Stream review bombing alerts"
	@echo "make s3     - Stream CCU metrics and spike alerts"
	@echo "make merge  - Drop realtime docs covered by the latest batch view"
	@echo "Options: PYTHON=python, B1_ARGS=... through B8_ARGS=..., S1_ARGS=... through S3_ARGS=..., MERGE_ARGS=..."

# Separate recursive calls preserve ordering even with make -j.
# B5 trước B3 (B3 dùng cảm xúc từ B5); B7 sau B3 (baseline cho S2/S3).
batch:
	$(MAKE) b1
	$(MAKE) b2
	$(MAKE) b5
	$(MAKE) b3
	$(MAKE) b4
	$(MAKE) b6
	$(MAKE) b7
	$(MAKE) b8

b1:
	"$(PYTHON)" -m processing.batch.b1_ingest_raw $(B1_ARGS)

b2:
	"$(PYTHON)" -m processing.batch.b2_clean_raw $(B2_ARGS)

b3:
	"$(PYTHON)" -m processing.batch.b3_game_daily_stats $(B3_ARGS)

b4:
	"$(PYTHON)" -m processing.batch.b4_playtime_buckets $(B4_ARGS)

b5:
	"$(PYTHON)" -m processing.batch.b5_aspect_sentiment $(B5_ARGS)

b6:
	"$(PYTHON)" -m processing.batch.b6_trending_games $(B6_ARGS)

b7:
	"$(PYTHON)" -m processing.batch.b7_baselines $(B7_ARGS)

b8:
	"$(PYTHON)" -m processing.batch.b8_game_clusters $(B8_ARGS)

s1:
	"$(PYTHON)" -m processing.streaming.s1_review_metrics $(S1_ARGS)

s2:
	"$(PYTHON)" -m processing.streaming.s2_review_bombing_detector $(S2_ARGS)

s3:
	"$(PYTHON)" -m processing.streaming.s3_ccu_spike_detector $(S3_ARGS)

merge:
	"$(PYTHON)" -m processing.serving.merge_views $(MERGE_ARGS)
