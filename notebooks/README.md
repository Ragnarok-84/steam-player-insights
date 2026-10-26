# Notebooks

Các notebook đọc kết quả của pipeline (profile local: `data/lake`; đổi bằng biến `LAKE_DIR`) và `tests/results/`.

| Notebook | Nội dung | Cần chạy trước |
| --- | --- | --- |
| `01_eda_raw_reviews.ipynb` | Khối lượng, ngôn ngữ, giờ chơi, review đáng ngờ, lệch dữ liệu | `python pipeline.py batch` |
| `02_sentiment_aspect_exploration.ipynb` | Khía cạnh bị phàn nàn, VADER vs nhãn, giờ chơi, game bỏ sớm / đáng tiền, TF-IDF, cụm K-Means | `python pipeline.py batch` |
| `03_scalability_fault_tolerance_analysis.ipynb` | Biểu đồ SC1–SC4, bảng FT1–FT6 | Các script trong `tests/scalability`, `tests/fault_tolerance` |

Cần `pandas`, `pyarrow`, `matplotlib` và `jupyterlab` (có trong `requirements.txt`). Khởi động Jupyter tại gốc repo:

```bash
jupyter lab notebooks/
```
