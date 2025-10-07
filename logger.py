import json
from pathlib import Path
from datetime import datetime

LOG_FILE = Path("logs/log.json")
LOG_FILE.parent.mkdir(exist_ok=True)


def log_action(action: str, status: str, info: dict = None):
    """Логирует действия в формате JSON"""
    entry = {
        "time": datetime.now().isoformat(),
        "action": action,
        "status": status,
        "info": info or {}
    }

    logs = []
    if LOG_FILE.exists():
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            try:
                logs = json.load(f)
            except json.JSONDecodeError:
                logs = []

    logs.append(entry)

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)

    print(f"[LOG] {action} - {status}")
