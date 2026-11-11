"""Presentation styling and responsive layout for the Kibana overview."""
from copy import deepcopy


BLUE = "#2563eb"
TEAL = "#0d9488"
PURPLE = "#7c3aed"
PALETTE = [BLUE, TEAL, PURPLE, "#f59e0b", "#06b6d4", "#ec4899", "#64748b"]

# Kibana uses a 48-column dashboard grid. All positions are explicit so panels
# of different heights cannot overlap.
LAYOUT = {
    "d1_intro": (0, 0, 48, 5),
    "d1_kpi_reviews": (0, 5, 12, 7),
    "d1_kpi_positive": (12, 5, 12, 7),
    "d1_kpi_games": (24, 5, 12, 7),
    "d1_kpi_peak": (36, 5, 12, 7),
    "d1_reviews_time": (0, 12, 30, 15),
    "d1_genre_positive": (30, 12, 18, 15),
    "d1_top_ccu": (0, 27, 30, 16),
    "d1_language": (30, 27, 18, 16),
    "d1_price_heatmap": (0, 43, 48, 14),
}


def text_mark(text, x, y, size, color, **extra):
    return {"type": "text", "encode": {"update": {
        "x": x, "y": {"value": y}, "text": text,
        "font": {"value": "Inter, Segoe UI, sans-serif"},
        "fontSize": {"value": size}, "fill": {"value": color},
        "baseline": {"value": "top"}, **extra,
    }}}


def card(title, agg, expression, color, subtitle, query):
    """One ES aggregate per card; queries honor dashboard filters and time."""
    return {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "title": title, "padding": 14, "autosize": {"type": "fit", "contains": "padding"},
        "background": "#ffffff",
        "data": [{"name": "stats", "url": {"index": "batch_game_daily", "body": {
            "size": 0, "query": deepcopy(query), "aggs": agg,
        }}, "format": {"property": "aggregations"}}],
        "marks": [
            {"type": "rect", "encode": {"update": {
                "x": {"value": 0}, "y": {"value": 0},
                "width": {"value": 4}, "height": {"signal": "height"},
                "cornerRadius": {"value": 2}, "fill": {"value": color},
            }}},
            text_mark({"value": title.upper()}, {"value": 18}, 3, 11, "#64748b",
                      fontWeight={"value": 600}),
            text_mark({"signal": expression}, {"value": 18}, 27, 34, color,
                      fontWeight={"value": 700}),
            text_mark({"value": subtitle}, {"value": 18}, 72, 11, "#64748b"),
        ],
    }


def apply_overview(panels, ctx_query, time_range):
    query = ctx_query(time_range("dt"))
    panels["d1_intro"] = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "title": "Steam Player Insights", "padding": 16,
        "autosize": {"type": "fit", "contains": "padding"}, "background": "#10243a",
        "marks": [
            text_mark({"value": "STEAM  /  PLAYER INSIGHTS"}, {"value": 4}, 0, 20, "#ffffff",
                      fontWeight={"value": 700}),
            text_mark({"value": "TỔNG QUAN THỊ TRƯỜNG  ·  Review, cảm xúc và sức hút của game"},
                      {"value": 4}, 31, 12, "#a5c9e8"),
        ],
    }
    stats = "data('stats').length ? data('stats')[0] : null"
    panels["d1_kpi_reviews"] = card(
        "Tổng review", {"total": {"sum": {"field": "total_reviews"}}},
        f"({stats}) ? format(data('stats')[0].total.value, ',.0f') : '—'",
        BLUE, "Trong khoảng thời gian đã chọn", query)
    panels["d1_kpi_positive"] = card(
        "Review tích cực", {"pos": {"sum": {"field": "positive_reviews"}},
                           "total": {"sum": {"field": "total_reviews"}}},
        f"({stats}) && data('stats')[0].total.value > 0 ? "
        "format(data('stats')[0].pos.value / data('stats')[0].total.value, '.1%') : '—'",
        TEAL, "Tính theo tổng số review", query)
    panels["d1_kpi_games"] = card(
        "Game có dữ liệu", {"games": {"cardinality": {"field": "appid", "precision_threshold": 40000}}},
        f"({stats}) ? format(data('stats')[0].games.value, ',.0f') : '—'",
        PURPLE, "Số game duy nhất · ước lượng", query)
    panels["d1_kpi_peak"] = card(
        "CCU đỉnh cao nhất", {"peak": {"max": {"field": "peak_ccu"}}},
        f"({stats}) && isValid(data('stats')[0].peak.value) ? "
        "format(data('stats')[0].peak.value, ',.0f') : '—'",
        "#d97706", "Kỷ lục của một game trong kỳ", query)

    for pid in LAYOUT:
        if pid.startswith(("d1_kpi", "d1_intro")):
            continue
        spec = panels[pid]
        spec.update({"background": "#ffffff", "padding": 14,
                     "autosize": {"type": "fit", "contains": "padding"}})
        spec["config"] = {
            "font": "Inter, Segoe UI, sans-serif", "view": {"stroke": None},
            "axis": {"labelLimit": 190, "labelColor": "#64748b", "titleColor": "#475569",
                     "labelFontSize": 11, "titleFontSize": 11, "titlePadding": 14,
                     "gridColor": "#edf2f7", "domain": False, "ticks": False,
                     "labelPadding": 8},
            "legend": {"labelColor": "#475569", "titleColor": "#475569", "labelFontSize": 11,
                       "symbolType": "circle", "padding": 10},
            "range": {"category": PALETTE},
        }

    trend = panels["d1_reviews_time"]
    trend["title"] = "01  /  Nhịp độ review theo ngày"
    trend.pop("mark")
    trend["encoding"]["x"].update({"title": None, "axis": {"format": "%d/%m", "tickCount": 8, "grid": False}})
    trend["encoding"]["y"].update({"title": "Số review", "axis": {"format": "~s"}})
    trend["encoding"]["tooltip"] = [
        {"field": "key", "type": "temporal", "title": "Ngày", "format": "%d/%m/%Y"},
        {"field": "r.value", "type": "quantitative", "title": "Review", "format": ",.0f"},
    ]
    trend["layer"] = [
        {"mark": {"type": "area", "color": {"gradient": "linear", "x1": 0, "y1": 0, "x2": 0, "y2": 1,
                                               "stops": [{"offset": 0, "color": "#bfdbfe"}, {"offset": 1, "color": "#eff6ff"}]}}},
        {"mark": {"type": "line", "color": BLUE, "strokeWidth": 3,
                  "point": {"filled": True, "fill": "white", "stroke": BLUE, "strokeWidth": 2, "size": 35}}},
    ]
    for pid, field, color, title in [
        ("d1_genre_positive", "positive_pct", TEAL, "02  /  Mức hài lòng theo thể loại"),
        ("d1_top_ccu", "peak.value", BLUE, "03  /  Top 10 game · CCU đỉnh"),
    ]:
        spec = panels[pid]
        spec["title"] = title
        spec["mark"] = {"type": "bar", "color": color, "cornerRadiusEnd": 5, "size": 20}
        spec["encoding"]["y"].update({"title": None, "axis": {"grid": False}})
        spec["encoding"]["x"]["axis"] = {"format": ".0f" if field == "positive_pct" else "~s"}
        spec["encoding"]["tooltip"] = [
            {"field": "key", "type": "nominal", "title": "Thể loại" if field == "positive_pct" else "Game"},
            {"field": field, "type": "quantitative", "title": "% tích cực" if field == "positive_pct" else "CCU đỉnh", "format": ",.1f" if field == "positive_pct" else ",.0f"},
        ]
    panels["d1_genre_positive"]["encoding"]["x"]["scale"] = {"domain": [0, 100]}
    panels["d1_top_ccu"]["data"]["url"]["body"]["aggs"]["x"]["terms"]["size"] = 10
    language = panels["d1_language"]
    language["title"] = "04  /  Cộng đồng người chơi"
    language["mark"] = {"type": "arc", "innerRadius": 70, "outerRadius": 110,
                        "padAngle": 0.035, "cornerRadius": 5, "stroke": "#ffffff", "strokeWidth": 2}
    language["encoding"]["color"]["scale"] = {"range": PALETTE}
    language["encoding"]["color"]["legend"] = {"orient": "bottom", "columns": 3, "title": None}
    language["encoding"]["tooltip"] = [
        {"field": "key", "type": "nominal", "title": "Ngôn ngữ"},
        {"field": "r.value", "type": "quantitative", "title": "Review", "format": ",.0f"},
    ]
    heat = panels["d1_price_heatmap"]
    heat["title"] = "05  /  Giá bán & mức hài lòng · số game theo phân khúc"
    heat["mark"] = {"type": "rect", "cornerRadius": 4, "stroke": "#ffffff", "strokeWidth": 3}
    heat["encoding"]["color"]["scale"] = {"scheme": "blues", "zero": True}
    heat["encoding"]["tooltip"] = [
        {"field": "_source.price_bucket", "type": "ordinal", "title": "Khoảng giá"},
        {"field": "_source.positive_ratio_percent", "bin": {"step": 10}, "type": "quantitative", "title": "% tích cực"},
        {"aggregate": "count", "type": "quantitative", "title": "Số game"},
    ]
