from aiops.models.store import InMemoryTable

incidents = InMemoryTable("incidents")
silences = InMemoryTable("silences")
notifications = InMemoryTable("notifications")
field_mappings = InMemoryTable("field_mappings")
tuning_proposals = InMemoryTable("tuning_proposals")
active_detector_configs = InMemoryTable("active_detector_configs")

_ALL = (incidents, silences, notifications, field_mappings, tuning_proposals, active_detector_configs)

# 每張表只有擁有者領域能寫；其他領域只能透過自己 dao.py 裡的 *ReadDAO 唯讀（tests/test_architecture.py 檢查）
TABLE_OWNERS = {
    "incidents": "alerts",
    "silences": "alerts",
    "notifications": "alerts",
    "field_mappings": "ingestion",
    "tuning_proposals": "tuning",
    "active_detector_configs": "tuning",
}


def reset_all() -> None:
    for table in _ALL:
        table.clear()
