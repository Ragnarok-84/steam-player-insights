import datetime as dt
import json
import math
import os
import runpy
import sys


MODULES = {
    's1': 'processing.streaming.s1_review_metrics',
    's2': 'processing.streaming.s2_review_bombing_detector',
    's3': 'processing.streaming.s3_ccu_spike_detector',
}
EXPECTED_QUERIES = {'s1': {'s1_60m', 's1_5m'}, 's2': {'s2_bombing'}, 's3': {'s3_ccu'}}


def inject():
    from crawlers.common.kafka_producer import SteamKafkaProducer
    from crawlers.demo_inject import bombing_records, ccu_records
    from processing.common.spark_utils import get_spark_session
    from pyspark.sql import functions as f

    spark = get_spark_session('Phase2_SelectDemoEvents')
    curated = os.environ['HDFS_CURATED_PATH']
    review = (spark.read.parquet(f'{curated}/review_baseline')
              .withColumn('threshold', f.col('neg_per_hour_mean') + 3 * f.col('neg_per_hour_std'))
              .where((f.col('baseline_positive_ratio') > 35) & (f.col('threshold') < 400))
              .orderBy('threshold', 'appid').select('appid', 'threshold').first())
    now = dt.datetime.now(dt.timezone.utc)
    ccu = (spark.read.parquet(f'{curated}/ccu_baseline').where(f.col('baseline_ccu') >= 100)
           .withColumn('hours_ago', f.pmod(f.lit(now.hour) - f.col('hour'), f.lit(24)))
           .orderBy('hours_ago', 'appid').select('appid', 'hour', 'baseline_ccu', 'hours_ago').first())
    spark.stop()
    if review is None or ccu is None:
        raise RuntimeError('The B7 baselines did not contain a game for both alert checks')
    review_appid = int(review.appid)
    ccu_appid = int(ccu.appid)
    count = max(80, math.ceil(review.threshold) + 20)
    event_hour = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=int(ccu.hours_ago))
    ccu_timestamp = int((event_hour + dt.timedelta(minutes=6)).timestamp())
    high_players = math.ceil(float(ccu.baseline_ccu) * 3)
    reviews = bombing_records(review_appid, count, 1, now=int(now.timestamp()))
    samples = ccu_records(ccu_appid, high_players, samples=3, now=ccu_timestamp)
    producer = SteamKafkaProducer()
    try:
        writes = [producer.send('steam.reviews.raw', str(review_appid), record) for record in reviews]
        writes.extend(producer.send('steam.players.ccu', str(ccu_appid), record) for record in samples)
        for write in writes:
            write.get(timeout=40)
    finally:
        producer.close()
    print(json.dumps({'source': 'demo_injection', 'review_appid': review_appid, 'negative_reviews': count,
                      'ccu_appid': ccu_appid, 'ccu_hour_utc': int(ccu.hour), 'ccu_samples': len(samples),
                      'ccu_baseline': float(ccu.baseline_ccu), 'injected_players': high_players}))


def stream(stage, arguments):
    from pyspark.sql.streaming import DataStreamWriter, StreamingQueryManager

    original_trigger = DataStreamWriter.trigger
    original_start = DataStreamWriter.start
    queries = []

    def available_now(writer, *args, **kwargs):
        return original_trigger(writer, availableNow=True)

    def remember_query(writer, *args, **kwargs):
        query = original_start(writer, *args, **kwargs)
        queries.append(query)
        return query

    def await_all(_manager, timeout=None):
        for query in queries:
            query.awaitTermination()

    DataStreamWriter.trigger = available_now
    DataStreamWriter.start = remember_query
    StreamingQueryManager.awaitAnyTermination = await_all
    os.environ['WRITE_ES'] = '1'
    metrics_path = f'/tmp/phase2-{stage}-metrics.jsonl'
    sys.argv = [MODULES[stage], '--source', 'kafka', '--metrics-file', metrics_path, *arguments]
    runpy.run_module(MODULES[stage], run_name='__main__')
    if {query.name for query in queries} != EXPECTED_QUERIES[stage]:
        raise RuntimeError(f'{stage} did not start its expected queries')
    totals = {name: 0 for name in EXPECTED_QUERIES[stage]}
    for query in queries:
        query.awaitTermination()
        for progress in query.recentProgress:
            totals[query.name] += int(progress['numInputRows'])
    if not all(totals.values()):
        raise RuntimeError(f'{stage} did not process Kafka input: {totals}')
    print(f'{stage.upper()} processed Kafka input and completed: {json.dumps(totals)}')


if __name__ == '__main__':
    if sys.argv[1] == 'inject':
        inject()
    else:
        stream(sys.argv[1], sys.argv[2:])
