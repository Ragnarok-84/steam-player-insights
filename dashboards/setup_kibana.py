"""Tạo data view, visualization (Vega) và dashboard D1–D4 trên Kibana qua Saved Objects API.

Chạy lại an toàn (overwrite theo id cố định). Cấu hình qua biến môi trường:
  KIBANA_HOST (mặc định http://localhost:5601), ELASTICSEARCH_USER / ELASTICSEARCH_PASSWORD.
Mỗi panel Vega nhận bộ lọc / ô tìm kiếm của dashboard (vd. gõ `appid:730` ở D2) và khoảng thời gian dashboard.
Xuất bản sao NDJSON vào dashboards/kibana/steam_dashboards.ndjson để import tay nếu cần.
"""
import json
import os
from pathlib import Path

import requests

KIBANA = os.getenv("KIBANA_HOST", "http://localhost:5601").rstrip("/")
AUTH = (os.getenv("ELASTICSEARCH_USER"), os.getenv("ELASTICSEARCH_PASSWORD")) \
    if os.getenv("ELASTICSEARCH_USER") and os.getenv("ELASTICSEARCH_PASSWORD") else None
OUT = Path(__file__).resolve().parent / "kibana" / "steam_dashboards.ndjson"

DATA_VIEWS = {
    "batch_game_daily": "dt", "batch_trending": "dt", "batch_aspect_stats": "month",
    "reviews_search": "dt", "realtime_game_metrics": "window_start", "alerts": "detected_at",
    "batch_event_impact": "event_dt", "batch_game_retention": None,
}


def ctx_query(*must):
    """Query nhận bộ lọc dashboard + điều kiện riêng của panel."""
    return {"bool": {"must": ["%dashboard_context-must_clause%", *must],
                     "filter": ["%dashboard_context-filter_clause%"],
                     "must_not": ["%dashboard_context-must_not_clause%"]}}


def time_range(field):
    return {"range": {field: {"%timefilter%": True}}}


def es_data(index, aggs, *must, time_field=None, fmt="aggregations.x.buckets"):
    q = ctx_query(*must, *([time_range(time_field)] if time_field else []))
    return {"url": {"index": index, "body": {"size": 0, "query": q, "aggs": {"x": aggs}}}, "format": {"property": fmt}}


def vl(title, data, mark, encoding, transform=None):
    spec = {"$schema": "https://vega.github.io/schema/vega-lite/v5.json", "title": title,
            "data": data, "mark": mark, "encoding": encoding,
            "config": {"axis": {"labelLimit": 160}}}
    if transform:
        spec["transform"] = transform
    return spec


def date_hist(field, interval, **sub):
    a = {"date_histogram": {"field": field, "calendar_interval": interval, "min_doc_count": 1}}
    if sub:
        a["aggs"] = sub
    return a


def terms(field, size=20, order=None, **sub):
    a = {"terms": {"field": field, "size": size}}
    if order:
        a["terms"]["order"] = order
    if sub:
        a["aggs"] = sub
    return a


def s(field):
    return {"sum": {"field": field}}


def m(field):
    return {"max": {"field": field}}


def a(field):
    return {"avg": {"field": field}}


T = {"type": "temporal"}
POS_RATIO = [{"calculate": "datum.pos.value * 100 / max(datum.tot.value, 1)", "as": "positive_pct"}]

# ---------------------------------------------------------------- panels
PANELS = {
    # D1 Tổng quan
    "d1_reviews_time": vl("Số review theo ngày", es_data("batch_game_daily", date_hist("dt", "day", r=s("total_reviews")), time_field="dt"),
                          {"type": "line", "point": False},
                          {"x": {"field": "key", **T, "title": "Ngày"}, "y": {"field": "r.value", "type": "quantitative", "title": "Review"}}),
    "d1_genre_positive": vl("% tích cực theo thể loại (tháng gần nhất trong khoảng)",
                            es_data("batch_segment_stats", terms("segment", 20, pos=s("positive_reviews"), tot=s("total_reviews")),
                                    {"term": {"dimension": "genre"}}, time_field="month"),
                            "bar", {"y": {"field": "key", "type": "nominal", "sort": "-x", "title": "Thể loại"},
                                    "x": {"field": "positive_pct", "type": "quantitative", "title": "% tích cực"}}, POS_RATIO),
    "d1_top_ccu": vl("Top game theo CCU đỉnh", es_data("batch_game_daily", terms("name", 15, {"peak": "desc"}, peak=m("peak_ccu")), time_field="dt"),
                     "bar", {"y": {"field": "key", "type": "nominal", "sort": "-x", "title": "Game"},
                             "x": {"field": "peak.value", "type": "quantitative", "title": "CCU đỉnh"}}),
    "d1_language": vl("Phân bố review theo ngôn ngữ", es_data("batch_language_stats", terms("language", 12, {"r": "desc"}, r=s("total_reviews")), time_field="dt"),
                      {"type": "arc", "innerRadius": 50},
                      {"theta": {"field": "r.value", "type": "quantitative"}, "color": {"field": "key", "type": "nominal", "title": "Ngôn ngữ"}}),
    "d1_price_heatmap": vl("Khoảng giá × % tích cực (số game)",
                           {"url": {"index": "batch_game_retention", "body": {"size": 5000, "query": ctx_query(),
                                                                                "_source": ["price_bucket", "positive_ratio_percent"]}},
                            "format": {"property": "hits.hits"}},
                           "rect", {"x": {"field": "_source.price_bucket", "type": "ordinal", "title": "Khoảng giá",
                                          "sort": ["free", "<10$", "10-20$", "20-40$", ">=40$"]},
                                    "y": {"field": "_source.positive_ratio_percent", "bin": {"step": 10}, "type": "quantitative", "title": "% tích cực"},
                                    "color": {"aggregate": "count", "type": "quantitative", "title": "Số game"}}),
    # D2 Chi tiết game (lọc bằng ô tìm kiếm: appid:<id>)
    "d2_weekly_positive": vl("% tích cực theo tuần", es_data("batch_game_period", date_hist("period_start", "week", pos=s("positive_reviews"), tot=s("total_reviews")),
                                                              {"term": {"period": "week"}}, time_field="period_start"),
                             {"type": "line", "point": True},
                             {"x": {"field": "key", **T, "title": "Tuần"}, "y": {"field": "positive_pct", "type": "quantitative", "title": "% tích cực"}}, POS_RATIO),
    "d2_ccu": vl("CCU trung bình / đỉnh theo ngày", es_data("batch_game_daily", date_hist("dt", "day", avg=a("avg_ccu"), peak=m("peak_ccu")), time_field="dt"),
                 {"type": "line"},
                 {"x": {"field": "key", **T, "title": "Ngày"}, "y": {"field": "value", "type": "quantitative", "title": "CCU"},
                  "color": {"field": "metric", "type": "nominal"}},
                 [{"fold": ["avg.value", "peak.value"], "as": ["metric", "value"]}]),
    "d2_playtime": vl("% tích cực theo nhóm giờ chơi", es_data("batch_playtime_buckets", terms("playtime_bucket", 10, pos=s("positive_reviews"), tot=s("total_reviews"))),
                      "bar", {"x": {"field": "key", "type": "ordinal", "sort": ["<2h", "2-10h", "10-50h", "50-200h", ">200h"], "title": "Giờ chơi khi review"},
                              "y": {"field": "positive_pct", "type": "quantitative", "title": "% tích cực"}}, POS_RATIO),
    "d2_aspects": vl("Khía cạnh bị nhắc trong review (tiêu cực / tích cực)",
                     es_data("batch_aspect_stats", terms("aspect", 10, {"neg": "desc"}, neg=s("negative_count"), pos=s("positive_count")), time_field="month"),
                     "bar", {"y": {"field": "key", "type": "nominal", "title": "Khía cạnh"}, "x": {"field": "value", "type": "quantitative", "stack": True, "title": "Số lần nhắc"},
                             "color": {"field": "loai", "type": "nominal", "scale": {"range": ["#d6604d", "#4393c3"]}}},
                     [{"fold": ["neg.value", "pos.value"], "as": ["loai", "value"]}]),
    # D3 Thời gian thực
    "d3_reviews_5m": vl("Review mới mỗi 5 phút", es_data("realtime_game_metrics", date_hist("window_start", "hour", r=s("review_count_window")),
                                                         {"term": {"metric_type": "reviews"}}, {"term": {"window_size": "5m"}}, time_field="window_start"),
                        "bar", {"x": {"field": "key", **T, "title": "Giờ"}, "y": {"field": "r.value", "type": "quantitative", "title": "Review"}}),
    "d3_positive_60m": vl("% tích cực cửa sổ 60 phút", es_data("realtime_game_metrics", date_hist("window_start", "hour", p=a("positive_ratio_percent")),
                                                               {"term": {"metric_type": "reviews"}}, {"term": {"window_size": "60m"}}, time_field="window_start"),
                          {"type": "line", "point": True},
                          {"x": {"field": "key", **T}, "y": {"field": "p.value", "type": "quantitative", "title": "% tích cực", "scale": {"domain": [0, 100]}}}),
    "d3_top_ccu_now": vl("CCU hiện tại — top game", es_data("realtime_game_metrics", terms("appid", 15, {"c": "desc"}, c=m("current_ccu")),
                                                           {"term": {"metric_type": "ccu"}}, time_field="window_start"),
                         "bar", {"y": {"field": "key", "type": "nominal", "sort": "-x", "title": "appid"}, "x": {"field": "c.value", "type": "quantitative", "title": "CCU"}}),
    # D4 Cảnh báo & xu hướng
    "d4_alerts_time": vl("Cảnh báo theo ngày và loại",
                         {"url": {"index": "alerts", "body": {"size": 5000, "query": ctx_query(time_range("window_start")),
                                                              "_source": ["window_start", "alert_type"]}},
                          "format": {"property": "hits.hits"}},
                         "bar", {"x": {"field": "_source.window_start", "timeUnit": "yearmonthdate", "type": "temporal", "title": "Ngày"},
                                 "y": {"aggregate": "count", "type": "quantitative", "title": "Cảnh báo"},
                                 "color": {"field": "_source.alert_type", "type": "nominal", "title": "Loại"}}),
    "d4_event_impact": vl("Thay đổi % tích cực quanh đợt giảm giá (7 ngày trước/sau)",
                          {"url": {"index": "batch_event_impact", "body": {"size": 50, "query": ctx_query(), "sort": [{"positive_delta": "desc"}]}},
                           "format": {"property": "hits.hits"}},
                          "bar", {"y": {"field": "_source.appid", "type": "nominal", "sort": "-x", "title": "appid"},
                                  "x": {"field": "_source.positive_delta", "type": "quantitative", "title": "Δ % tích cực"},
                                  "color": {"condition": {"test": "datum._source.positive_delta < 0", "value": "#d6604d"}, "value": "#4393c3"}}),
}

# D2 word cloud: Vega đầy đủ (Vega-Lite không có wordcloud)
PANELS["d2_wordcloud"] = {
    "$schema": "https://vega.github.io/schema/vega/v5.json", "title": "Từ khoá review tiêu cực (TF-IDF)",
    "width": 400, "height": 250, "autosize": "none",
    "data": [{"name": "k", "url": {"index": "game_keywords", "body": {"size": 0, "query": ctx_query(),
                                                                     "aggs": {"x": terms("keyword", 60, {"s": "desc"}, s=s("score"))}}},
              "format": {"property": "aggregations.x.buckets"},
              "transform": [{"type": "formula", "as": "w", "expr": "datum.s.value"}]}],
    "scales": [{"name": "c", "type": "ordinal", "domain": {"data": "k", "field": "key"}, "range": "category"}],
    "marks": [{"type": "text", "from": {"data": "k"},
               "encode": {"enter": {"text": {"field": "key"}, "align": {"value": "center"}, "baseline": {"value": "alphabetic"},
                                    "fill": {"scale": "c", "field": "key"}}},
               "transform": [{"type": "wordcloud", "size": [400, 250], "text": {"field": "key"},
                              "fontSize": {"field": "datum.w"}, "fontSizeRange": [10, 48], "padding": 2}]}],
}

SEARCHES = {
    "s_alerts": ("Danh sách cảnh báo", "alerts", ["appid", "alert_type", "rule", "window_start", "negative_reviews",
                                                  "positive_ratio", "change_ratio"], [["detected_at", "desc"]]),
    "s_trending": ("Game trending (xếp hạng mỗi ngày)", "batch_trending", ["trending_rank", "appid", "name", "reviews_7d",
                                                                        "review_growth_rate", "ccu_growth_rate", "trending_score"],
                   [["dt", "desc"], ["trending_rank", "asc"]]),
    "s_reviews": ("Tìm kiếm review", "reviews_search", ["appid", "voted_up", "sentiment_label", "aspects", "cleaned_review"], [["dt", "desc"]]),
}

DASHBOARDS = {
    "steam-d1-overview": ("D1. Tổng quan", "now-5y", ["d1_reviews_time", "d1_genre_positive", "d1_top_ccu", "d1_language", "d1_price_heatmap"]),
    "steam-d2-game": ("D2. Chi tiết game (lọc: appid:<id>)", "now-5y",
                      ["d2_weekly_positive", "d2_ccu", "d2_playtime", "d2_aspects", "d2_wordcloud", "s_reviews"]),
    "steam-d3-realtime": ("D3. Giám sát thời gian thực", "now-24h", ["d3_reviews_5m", "d3_positive_60m", "d3_top_ccu_now"]),
    "steam-d4-alerts": ("D4. Cảnh báo & xu hướng", "now-1y", ["d4_alerts_time", "s_alerts", "s_trending", "d4_event_impact"]),
}


def saved_objects():
    objs = []
    for idx, tf in DATA_VIEWS.items():
        attrs = {"title": idx, "name": idx}
        if tf:
            attrs["timeFieldName"] = tf
        objs.append({"type": "index-pattern", "id": f"dv-{idx}", "attributes": attrs, "references": []})
    for pid, spec in PANELS.items():
        objs.append({"type": "visualization", "id": pid, "references": [], "attributes": {
            "title": spec["title"], "uiStateJSON": "{}", "description": "",
            "visState": json.dumps({"title": spec["title"], "type": "vega", "aggs": [],
                                    "params": {"spec": json.dumps({k: v for k, v in spec.items() if k != "title"},
                                                                  ensure_ascii=False, indent=1)}}, ensure_ascii=False),
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}}})
    for sid, (title, idx, cols, sort) in SEARCHES.items():
        objs.append({"type": "search", "id": sid, "attributes": {
            "title": title, "columns": cols, "sort": sort,
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                "query": {"query": "", "language": "kuery"}, "filter": [],
                "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"})}},
            "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": f"dv-{idx}"}]})
    for did, (title, time_from, panels) in DASHBOARDS.items():
        panel_json, refs = [], []
        for i, pid in enumerate(panels):
            kind = "search" if pid in SEARCHES else "visualization"
            w = 48 if kind == "search" else 24
            panel_json.append({"panelIndex": str(i), "gridData": {"x": (i % 2) * 24 if w == 24 else 0, "y": (i // 2) * 15,
                                                                 "w": w, "h": 15, "i": str(i)},
                               "embeddableConfig": {}, "panelRefName": f"panel_{i}", "version": "8.12.1"})
            refs.append({"name": f"panel_{i}", "type": kind, "id": pid})
        objs.append({"type": "dashboard", "id": did, "references": refs, "attributes": {
            "title": title, "description": "", "timeRestore": True, "timeFrom": time_from, "timeTo": "now",
            "panelsJSON": json.dumps(panel_json), "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})}}})
    return objs


def main():
    objs = saved_objects()
    ndjson = "\n".join(json.dumps(o, ensure_ascii=False) for o in objs) + "\n"
    try:  # bản export để import tay; trong container chỉ đọc thì bỏ qua
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(ndjson, encoding="utf-8")
    except OSError as err:
        print(f"Skip writing {OUT}: {err}")
    resp = requests.post(f"{KIBANA}/api/saved_objects/_import?overwrite=true", headers={"kbn-xsrf": "true"},
                         files={"file": ("steam_dashboards.ndjson", ndjson.encode("utf-8"), "application/ndjson")},
                         auth=AUTH, verify=False, timeout=120)
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise SystemExit(f"Kibana import errors: {json.dumps(body.get('errors'), ensure_ascii=False)[:2000]}")
    print(f"Imported {body.get('successCount')} saved objects into {KIBANA}; NDJSON at {OUT}")


if __name__ == "__main__":
    main()
