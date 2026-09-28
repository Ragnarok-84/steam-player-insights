.DEFAULT_GOAL := help

# Activate your venv first, or override: make b1 PYTHON=venv/Scripts/python.exe
PYTHON ?= python
B1_ARGS ?=
B2_ARGS ?=
B3_ARGS ?=
B4_ARGS ?=
B5_ARGS ?=
B6_ARGS ?=

.PHONY: help batch b1 b2 b3 b4 b5 b6

help:
	@echo "make batch  - Run B1 through B6 sequentially; stop on failure"
	@echo "make b1     - Ingest Kafka into raw storage"
	@echo "make b2     - Clean raw reviews"
	@echo "make b3     - Compute daily game statistics"
	@echo "make b4     - Analyze playtime buckets"
	@echo "make b5     - Analyze aspects and sentiment"
	@echo "make b6     - Rank trending games"
	@echo "Options: PYTHON=python, B1_ARGS=..., through B6_ARGS=..."

# Separate recursive calls preserve ordering even with make -j.
batch:
	$(MAKE) b1
	$(MAKE) b2
	$(MAKE) b3
	$(MAKE) b4
	$(MAKE) b5
	$(MAKE) b6

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
