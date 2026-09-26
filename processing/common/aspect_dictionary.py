"""
Từ điển từ khóa khía cạnh (Aspect Keywords Dictionary)
Dùng để phân loại phản ánh của người chơi trong bài đánh giá Steam thành các khía cạnh chính.
"""

ASPECT_KEYWORDS = {
    "performance": [
        "fps", "lag", "stutter", "freeze", "crash", "optimization", "unoptimized",
        "drop", "framerate", "gpu", "cpu", "memory leak", "overheat", "performance"
    ],
    "bug": [
        "bug", "glitch", "broken", "exploit", "corrupt", "freeze", "error",
        "crash to desktop", "softlock", "game breaking", "not working", "fix"
    ],
    "price": [
        "price", "expensive", "overpriced", "worth", "cost", "microtransaction",
        "microtransactions", "pay to win", "p2w", "battle pass", "greedy", "refund", "dlc"
    ],
    "server": [
        "server", "servers", "disconnect", "ping", "latency", "connection",
        "matchmaking", "queue", "desync", "offline", "kick", "netcode"
    ],
    "gameplay": [
        "gameplay", "mechanic", "mechanics", "boring", "repetitive", "fun",
        "combat", "control", "controls", "movement", "clunky", "balance", "physics"
    ],
    "story": [
        "story", "plot", "character", "characters", "dialogue", "writing",
        "voice acting", "lore", "ending", "campaign", "narrative"
    ],
    "graphics": [
        "graphics", "graphic", "visual", "visuals", "art style", "animation",
        "textures", "ray tracing", "resolution", "beautiful", "ugly"
    ],
    "cheating": [
        "cheat", "cheater", "cheaters", "hacker", "hackers", "aimbot",
        "wallhack", "anti-cheat", "anticheat", "vac", "easy anticheat"
    ],
}

def extract_aspects(text: str) -> list:
    """Trả về danh sách các khía cạnh xuất hiện trong nội dung review"""
    if not text:
        return []
    text_lower = text.lower()
    matched = []
    for aspect, keywords in ASPECT_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            matched.append(aspect)
    return matched
