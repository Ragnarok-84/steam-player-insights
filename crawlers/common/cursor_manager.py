import os
import json
import logging
from crawlers.config import STATE_DIR

logger = logging.getLogger(__name__)

class CursorManager:
    """
    Quản lý lưu và đọc trạng thái phân trang cursor của từng appid.
    Hỗ trợ crawler phục hồi (fault-tolerance) tiếp tục crawl từ mốc cũ sau khi restart.
    """
    def __init__(self, state_dir: str = STATE_DIR):
        self.state_dir = state_dir
        os.makedirs(self.state_dir, exist_ok=True)

    def _get_path(self, appid: int) -> str:
        return os.path.join(self.state_dir, f"cursor_{appid}.json")

    def get_cursor(self, appid: int) -> str:
        path = self._get_path(appid)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("cursor", "*")
            except Exception as e:
                logger.error(f"Error reading cursor for appid {appid}: {e}")
        return "*"

    def save_cursor(self, appid: int, cursor: str, last_updated: int = None):
        path = self._get_path(appid)
        temp_path = f"{path}.tmp"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump({"appid": appid, "cursor": cursor, "last_updated": last_updated}, f)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"Error saving cursor for appid {appid}: {e}")
