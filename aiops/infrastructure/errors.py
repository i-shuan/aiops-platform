class InfrastructureError(Exception):
    """基礎設施故障（DB、ES、訊息佇列連不上）。由 DAO / Spec 丟出，不是業務錯誤。"""
