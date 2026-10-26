.DEFAULT_GOAL := help

# Mọi target gọi pipeline.py (chạy được cả khi không có make: python pipeline.py <lệnh>).
# Ví dụ: make batch RAW=data/raw ES=1        make s2 ARGS="--source file"
PYTHON ?= python
PROFILE ?= local
RAW ?= data/raw
ARGS ?=
ES ?=
IMAGE ?= steam-spark:dev

ES_FLAG := $(if $(ES),--es,)
RUN = "$(PYTHON)" pipeline.py --profile $(PROFILE) --raw $(RAW) $(ES_FLAG)
DOCKER = docker run --rm -it -v "$(CURDIR):/opt/spark/work-dir" $(IMAGE) python3 pipeline.py --profile $(PROFILE) --raw $(RAW) $(ES_FLAG)

JOBS := b1 b2 b3 b4 b5 b6 b7 b8 s1 s2 s3 merge es-setup kibana-setup replay feedback applist crawl
.PHONY: help batch sample image docker-batch test $(JOBS)

help:
	@echo "make batch          - B2 > B5 > B3 > B4 > B6 > B7 > B8 (ES=1: ghi ES + merge view)"
	@echo "make b1..b8         - Một job batch; s1 s2 s3 - streaming (ARGS=\"--source file\")"
	@echo "make sample         - Sinh dữ liệu mẫu vào data/sample_raw"
	@echo "make image          - Build image Spark ($(IMAGE))"
	@echo "make docker-batch   - Chạy batch trong Docker (dùng trên Windows)"
	@echo "make es-setup | merge | kibana-setup | replay | feedback | applist | crawl"
	@echo "make test           - Unit test (pytest)"
	@echo "Biến: PYTHON, PROFILE=local|cluster, RAW=data/raw, ES=1, ARGS=..."

batch:
	$(RUN) batch $(ARGS)

$(JOBS):
	$(RUN) $@ $(ARGS)

sample:
	"$(PYTHON)" tests/make_sample_data.py --out data/sample_raw

image:
	docker build -t $(IMAGE) -f containers/Dockerfile.spark .

docker-batch:
	$(DOCKER) batch $(ARGS)

test:
	"$(PYTHON)" -m pytest -q tests/unit
