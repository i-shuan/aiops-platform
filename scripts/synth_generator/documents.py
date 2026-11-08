"""文件共用部分：新資料串流的名稱、service／labels／kubernetes 區塊、確定性的 id。

新資料一律寫到 gen 開頭的 namespace（每個劇本自己一個，例如 gencascade；兩個劇本是同一批服務、同一段時間，
混在同一個資料串流就分不開），落在平台唯讀 key 與 Grafana 讀得到的 pattern 底下
（metrics-apm.transaction.1m-*、traces-apm-*、logs-apm.*），跟既有的 scn、synth 完全分開。
"""
from __future__ import annotations

import hashlib
import random
import re

from .topology import Service

KINDS = ("metrics", "traces", "logs")
_DS = {"metrics": ("metrics", "apm.transaction.1m"), "traces": ("traces", "apm"), "logs": ("logs", "apm.error")}
NAMESPACE_RE = re.compile(r"^gen[a-z0-9]{0,20}$")
K8S_NAMESPACE = "shop"


def check_namespace(namespace: str) -> str:
    """只准 gen 開頭（小寫英數）：保證不會寫到既有的 scn、synth。"""
    if not NAMESPACE_RE.match(namespace):
        raise ValueError(f"namespace 必須是 gen 開頭的小寫英數（例如 gencascade），不能是 {namespace!r}")
    return namespace


def data_stream(kind: str, namespace: str) -> str:
    """metrics-apm.transaction.1m-<ns>、traces-apm-<ns>、logs-apm.error-<ns>。"""
    t, dataset = _DS[kind]
    return f"{t}-{dataset}-{check_namespace(namespace)}"


def pod_name(service: str) -> str:
    return f"{service}-{hashlib.sha256(service.encode()).hexdigest()[:5]}-0"


def common(svc: Service, kind: str, namespace: str) -> dict:
    """每份文件都有的區塊（巢狀物件，不用點號鍵：ES 的 APM ingest pipeline 讀巢狀欄位）。"""
    pod = pod_name(svc.name)
    return {
        "service": {"name": svc.name, "environment": "prod", "version": svc.version, "node": {"name": pod}},
        "labels": {"dept": svc.dept, "group": svc.group},
        "kubernetes": {"pod": {"name": pod}, "namespace": K8S_NAMESPACE},
        "agent": {"name": svc.agent},
        "data_stream": {"type": _DS[kind][0], "dataset": _DS[kind][1], "namespace": namespace},
    }


def hex_id(rng: random.Random, bits: int) -> str:
    return f"{rng.getrandbits(bits):0{bits // 4}x}"


def grouping_key(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:32]
