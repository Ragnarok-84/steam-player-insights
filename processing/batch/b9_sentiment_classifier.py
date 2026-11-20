"""B9: phân loại cảm xúc review bằng Logistic Regression (Spark MLlib), so sánh với VADER của B5.

- Nhãn: voted_up (có sẵn, không gán tay). Chỉ review tiếng Anh có nội dung, bỏ review bị B2 gắn cờ đáng ngờ.
- Đặc trưng: RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF như B5, với hai điểm khác có chủ ý:
  giữ từ phủ định (not, no, don't...) và token dài >= 2 ký tự, vì "not good" và "no" mang cảm xúc.
- Chia train/test 80/20: mặc định theo thời gian (review cũ train, 20% mới nhất test); --split random dùng seed 42.
- Mất cân bằng nhãn: weightCol = n / (2 * n_lớp); báo cáo F1 và AUC, không chỉ accuracy.
- VADER đánh giá trên CÙNG tập test: compound >= 0,05 là tích cực (điểm lấy từ curated/review_sentiment của B5).

Siêu tham số mặc định minDF=20, regParam=0.1: thử 3 cấu hình trên bộ 100 game (2026-11-19); minDF=5 cho
top từ toàn từ hiếm/mảnh từ ("ter", "william") còn minDF=20, regParam=0.1 cho AUC cao nhất và từ đọc được.
Cấu hình chọn trên chính tập test nên chỉ là chọn thô; không tinh chỉnh thêm.

Thời gian (giây): train_s = fit cả pipeline đặc trưng + mô hình trên tập train đã cache (không tính đọc dữ liệu);
predict_s = transform + materialize tập test.

Đầu ra (curated):
  sentiment_model_metrics  một dòng / (mô hình, tỷ lệ train): accuracy, f1, f1_negative, auc, train_s, predict_s
  sentiment_model_terms    top từ trọng số dương/âm của mô hình đầy đủ, đối chiếu từ khoá tiêu cực B5 (curated/keywords)
"""
import argparse
import datetime as dt
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.sql.functions import col, when, length, lit, udf, count as spark_count
from pyspark.sql.types import DoubleType

from processing.common.spark_utils import get_spark_session, path_exists, write_table, CLEAN_PATH, CURATED_PATH

SEED = 42
VADER_POSITIVE = 0.05  # cùng ngưỡng sentiment_label của B5
NEGATIONS = {
    "no", "not", "nor", "never", "cannot", "can't", "don't", "doesn't", "didn't", "isn't", "wasn't", "aren't",
    "weren't", "won't", "wouldn't", "couldn't", "shouldn't", "haven't", "hasn't", "hadn't", "mustn't", "needn't",
}
METRIC_COLUMNS = ["model", "split", "train_fraction", "train_rows", "test_rows", "test_positive_ratio",
                  "accuracy", "f1", "f1_negative", "auc", "train_s", "predict_s", "run_at"]


def stop_words():
    return [w for w in StopWordsRemover.loadDefaultStopWords("english") if w not in NEGATIONS]


def build_pipeline(vocab_size: int, min_df: float, reg_param: float, max_iter: int) -> Pipeline:
    return Pipeline(stages=[
        RegexTokenizer(inputCol="cleaned_review", outputCol="words", pattern="[^a-zA-Z']+", minTokenLength=2),
        StopWordsRemover(inputCol="words", outputCol="terms", stopWords=stop_words()),
        CountVectorizer(inputCol="terms", outputCol="tf", vocabSize=vocab_size, minDF=min_df),
        IDF(inputCol="tf", outputCol="features"),
        LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                           regParam=reg_param, elasticNetParam=0.0, maxIter=max_iter),
    ])


def load_labeled(spark, clean_root: str):
    return load_labeled_df(spark.read.parquet(f"{clean_root}/reviews"))


def load_labeled_df(df):
    """clean/reviews -> review tiếng Anh có nội dung, không bị gắn cờ đáng ngờ, nhãn label = voted_up (1.0/0.0)."""
    return (
        df.filter(col("is_english") & col("voted_up").isNotNull() & col("cleaned_review").isNotNull()
                  & (length(col("cleaned_review")) > 0) & ~col("is_suspicious"))
        .select("recommendationid", "appid", "timestamp_created", "cleaned_review",
                when(col("voted_up"), 1.0).otherwise(0.0).alias("label"))
    )


def split_train_test(df, split: str, test_ratio: float):
    if split == "random":
        train, test = df.randomSplit([1 - test_ratio, test_ratio], seed=SEED)
        return train, test, None
    # Theo thời gian: mốc là phân vị (1 - test_ratio) của timestamp_created
    cutoff = df.approxQuantile("timestamp_created", [1 - test_ratio], 0.0005)[0]
    return df.filter(col("timestamp_created") <= cutoff), df.filter(col("timestamp_created") > cutoff), int(cutoff)


def with_class_weight(train):
    """weight = n / (2 * n_lớp): hai lớp đóng góp như nhau vào hàm mất mát."""
    counts = {r["label"]: r["n"] for r in train.groupBy("label").agg(spark_count("*").alias("n")).collect()}
    n = sum(counts.values())
    w = {lbl: n / (2.0 * c) for lbl, c in counts.items()}
    return train.withColumn("weight", when(col("label") == 1.0, lit(w.get(1.0, 1.0))).otherwise(lit(w.get(0.0, 1.0))))


def evaluate(pred, score_col: str, prediction_col: str = "prediction") -> dict:
    """accuracy, F1 có trọng số theo lớp, F1 lớp tiêu cực, AUC (score_col: rawPrediction hoặc điểm VADER)."""
    mc = MulticlassClassificationEvaluator(labelCol="label", predictionCol=prediction_col)
    return {
        "accuracy": mc.setMetricName("accuracy").evaluate(pred),
        "f1": mc.setMetricName("f1").evaluate(pred),
        "f1_negative": mc.setMetricName("fMeasureByLabel").setMetricLabel(0.0).evaluate(pred),
        "auc": BinaryClassificationEvaluator(labelCol="label", rawPredictionCol=score_col,
                                             metricName="areaUnderROC").evaluate(pred),
    }


def vader_scores(spark, curated_root: str, test):
    """Điểm VADER của tập test: lấy từ review_sentiment (B5); thiếu thì tính lại bằng hàm của B5."""
    path = f"{curated_root}/review_sentiment"
    if path_exists(spark, path):
        s = spark.read.parquet(path).select("recommendationid", col("sentiment_score").cast("double").alias("vader_score"))
        scored = test.join(s, "recommendationid", "inner")
    else:
        from processing.batch.b5_aspect_sentiment import get_vader_compound
        print("B9: không có curated/review_sentiment (chưa chạy B5) -> tính VADER trên tập test")
        scored = test.withColumn("vader_score", udf(get_vader_compound, DoubleType())(col("cleaned_review")))
    return scored.withColumn("vader_prediction", when(col("vader_score") >= VADER_POSITIVE, 1.0).otherwise(0.0))


def top_terms(spark, model, curated_root: str, top_n: int):
    vocab = model.stages[2].vocabulary
    coef = model.stages[-1].coefficients.toArray().tolist()
    ranked = sorted(range(len(coef)), key=lambda i: coef[i])
    rows = [("negative", r + 1, vocab[i], float(coef[i])) for r, i in enumerate(ranked[:top_n])]
    rows += [("positive", r + 1, vocab[i], float(coef[i])) for r, i in enumerate(reversed(ranked[-top_n:]))]
    terms = spark.createDataFrame(rows, "direction string, rank int, term string, coefficient double")
    kw_path = f"{curated_root}/keywords"
    if path_exists(spark, kw_path):  # từ khoá tiêu cực TF-IDF của B5: term có nằm trong top của bao nhiêu game
        kw = (spark.read.parquet(kw_path).groupBy(col("keyword").alias("term"))
              .agg(spark_count("*").alias("b5_negative_keyword_games")))
        terms = terms.join(kw, "term", "left")
    else:
        terms = terms.withColumn("b5_negative_keyword_games", lit(None).cast("long"))
    return terms.fillna(0, subset=["b5_negative_keyword_games"]).select(
        "direction", "rank", "term", "coefficient", "b5_negative_keyword_games")


def train_sentiment_classifier(spark, clean_root: str, curated_root: str, split: str = "time",
                               test_ratio: float = 0.2, train_fractions=(1.0,), vocab_size: int = 20000,
                               min_df: float = 20.0, reg_param: float = 0.1, max_iter: int = 50, top_n: int = 30,
                               save_model: bool = False):
    """Job B9. Trả về danh sách dict chỉ số (một phần tử / mô hình / tỷ lệ train) để benchmark ghi mẫu chung."""
    data = load_labeled(spark, clean_root)
    train_all, test, cutoff = split_train_test(data, split, test_ratio)
    test = test.cache()
    test_rows = test.count()
    if test_rows == 0:
        print("B9: không có review tiếng Anh để đánh giá; bỏ qua.")
        return []
    test_pos = test.filter(col("label") == 1.0).count() / test_rows
    run_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    if cutoff is not None:
        print(f"B9: split=time, mốc {dt.datetime.fromtimestamp(cutoff, dt.timezone.utc):%Y-%m-%d %H:%M} UTC")

    results, full_model = [], None
    for frac in sorted(set(train_fractions)):
        train = train_all if frac >= 1.0 else train_all.sample(fraction=frac, seed=SEED)
        train = with_class_weight(train).cache()
        train_rows = train.count()
        if train_rows == 0 or train.select("label").distinct().count() < 2:
            print(f"B9: train_fraction={frac}: tập train thiếu một lớp; bỏ qua.")
            train.unpersist()
            continue
        t = time.perf_counter()
        model = build_pipeline(vocab_size, min_df, reg_param, max_iter).fit(train)
        train_s = time.perf_counter() - t
        train.unpersist()

        t = time.perf_counter()
        pred = model.transform(test).select("label", "rawPrediction", "prediction").cache()
        pred.count()
        predict_s = time.perf_counter() - t
        m = evaluate(pred, "rawPrediction")
        pred.unpersist()
        results.append(dict(model="logreg", split=split, train_fraction=float(frac), train_rows=train_rows,
                            test_rows=test_rows, test_positive_ratio=test_pos, train_s=train_s, predict_s=predict_s,
                            run_at=run_at, **m))
        print(f"B9 logreg frac={frac}: train={train_rows} test={test_rows} " +
              " ".join(f"{k}={v:.4f}" for k, v in m.items()) + f" train_s={train_s:.1f} predict_s={predict_s:.1f}")
        if frac >= max(train_fractions):
            full_model = model

    if full_model is None:
        test.unpersist()
        return results

    vader = vader_scores(spark, curated_root, test).cache()
    vader_rows = vader.count()
    if vader_rows:
        m = evaluate(vader.withColumnRenamed("vader_prediction", "prediction"), "vader_score")
        results.append(dict(model="vader", split=split, train_fraction=None, train_rows=None, test_rows=vader_rows,
                            test_positive_ratio=vader.filter(col("label") == 1.0).count() / vader_rows,
                            train_s=None, predict_s=None, run_at=run_at, **m))
        print(f"B9 vader (cùng tập test, {vader_rows} review): " + " ".join(f"{k}={v:.4f}" for k, v in m.items()))
    vader.unpersist()
    test.unpersist()

    metrics_df = spark.createDataFrame(
        [tuple(r.get(c) for c in METRIC_COLUMNS) for r in results],
        "model string, split string, train_fraction double, train_rows long, test_rows long, "
        "test_positive_ratio double, accuracy double, f1 double, f1_negative double, auc double, "
        "train_s double, predict_s double, run_at timestamp")
    write_table(spark, metrics_df.coalesce(1), f"{curated_root}/sentiment_model_metrics", "sentiment_model_metrics")
    terms = top_terms(spark, full_model, curated_root, top_n)
    write_table(spark, terms.coalesce(1), f"{curated_root}/sentiment_model_terms", "sentiment_model_terms")
    terms.orderBy("direction", "rank").show(10, truncate=False)
    if save_model:
        full_model.write().overwrite().save(f"{curated_root}/models/b9_sentiment_logreg")
    print("Job B9 completed successfully.")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="B9: Logistic Regression sentiment classifier vs VADER")
    parser.add_argument("--clean", default=CLEAN_PATH)
    parser.add_argument("--curated", default=CURATED_PATH)
    parser.add_argument("--split", choices=["time", "random"], default="time")
    parser.add_argument("--test-ratio", type=float, default=0.2)
    parser.add_argument("--train-fractions", type=float, nargs="+", default=[1.0],
                        help="Đường cong học trong một quy mô, vd. 0.1 0.25 0.5 1.0 (cùng tập test)")
    parser.add_argument("--vocab-size", type=int, default=20000)
    parser.add_argument("--min-df", type=float, default=20.0)
    parser.add_argument("--reg-param", type=float, default=0.1)
    parser.add_argument("--max-iter", type=int, default=50)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--save-model", action="store_true", help="Lưu PipelineModel vào curated/models/")
    args = parser.parse_args()

    spark = get_spark_session("B9_Sentiment_Classifier")
    train_sentiment_classifier(spark, args.clean, args.curated, args.split, args.test_ratio, args.train_fractions,
                               args.vocab_size, args.min_df, args.reg_param, args.max_iter, args.top_n,
                               args.save_model)
    spark.stop()
