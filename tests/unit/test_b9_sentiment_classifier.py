"""B9: chia train/test, trọng số lớp, giữ từ phủ định, và chạy trọn trên dữ liệu nhỏ (ghi Parquet tạm)."""
import pytest

pytest.importorskip("pyspark")

from processing.batch import b9_sentiment_classifier as b9  # noqa: E402

POS = ["great fun game love it", "amazing story and great music", "best game ever highly recommend",
       "really fun with friends love", "great value fun gameplay"]
NEG = ["boring broken game refund", "not fun crashes all the time", "worst purchase refund now",
       "unplayable bugs and crashes", "boring and not worth the money"]


def reviews(spark, n_per_text=6):
    rows, rid = [], 0
    for rep in range(n_per_text):
        for i, text in enumerate(POS + NEG):
            rid += 1
            rows.append((str(rid), 10 + i % 3, 1_600_000_000 + rid * 3600, text, i < len(POS), True, False))
    return spark.createDataFrame(rows, "recommendationid string, appid long, timestamp_created long, "
                                       "cleaned_review string, voted_up boolean, is_english boolean, "
                                       "is_suspicious boolean")


def test_stop_words_keep_negations(spark):  # danh sách mặc định đọc qua JVM
    words = set(b9.stop_words())
    assert {"not", "no", "don't"}.isdisjoint(words)
    assert "the" in words


def test_time_split_puts_newest_in_test(spark):
    df = b9.load_labeled_df(reviews(spark))
    train, test, cutoff = b9.split_train_test(df, "time", 0.2)
    assert cutoff is not None
    assert train.agg({"timestamp_created": "max"}).first()[0] <= cutoff < test.agg({"timestamp_created": "min"}).first()[0]
    assert train.count() + test.count() == df.count()


def test_class_weight_balances_classes(spark):
    df = spark.createDataFrame([(1.0,)] * 8 + [(0.0,)] * 2, "label double")
    w = {r["label"]: r["s"] for r in b9.with_class_weight(df).groupBy("label").agg({"weight": "sum"})
         .withColumnRenamed("sum(weight)", "s").collect()}
    assert w[1.0] == pytest.approx(w[0.0])


def test_end_to_end_writes_metrics_and_terms(spark, tmp_path):
    clean, curated = tmp_path / "clean", tmp_path / "curated"
    reviews(spark).write.parquet(str(clean / "reviews"))
    results = b9.train_sentiment_classifier(spark, clean.as_uri(), curated.as_uri(), split="random",
                                            min_df=1.0, top_n=5)
    models = {r["model"] for r in results}
    assert models == {"logreg", "vader"}  # vader tính lại vì không có review_sentiment
    logreg = next(r for r in results if r["model"] == "logreg")
    assert logreg["auc"] > 0.8 and logreg["train_s"] > 0
    assert spark.read.parquet(str(curated / "sentiment_model_metrics")).count() == 2
    terms = spark.read.parquet(str(curated / "sentiment_model_terms"))
    neg = {r["term"] for r in terms.filter("direction = 'negative'").collect()}
    assert neg & {"boring", "refund", "crashes", "worst", "not", "unplayable", "broken"}
