import re

ASCII_ART_PATTERN = re.compile(r"[\u2580-\u259F\u2800-\u28FF]+")
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")
MULTIPLE_SPACES = re.compile(r"\s+")

def clean_review_text(text: str) -> str:
    """
    Làm sạch văn bản review cơ bản:
    - Bỏ URL
    - Bỏ ký tự ASCII art/block characters spam
    - Chuẩn hóa khoảng trắng
    """
    if not text:
        return ""
    # Xóa URL
    cleaned = URL_PATTERN.sub("", text)
    # Xóa ASCII art khối
    cleaned = ASCII_ART_PATTERN.sub("", cleaned)
    # Chuẩn hóa khoảng trắng và dòng
    cleaned = MULTIPLE_SPACES.sub(" ", cleaned).strip()
    return cleaned

def is_spam_or_empty(text: str, min_words: int = 2) -> bool:
    """Kiểm tra xem review có phải là chuỗi rỗng hoặc meme/spam vô nghĩa không"""
    if not text:
        return True
    cleaned = clean_review_text(text)
    words = cleaned.split()
    if len(words) < min_words:
        return True
    return False
