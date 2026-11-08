"""開發用：幫本機建一把唯讀 ES API key，直接寫進 secrets.local.yaml。

    python scripts/dev_es_key.py

要建 key 得用 ES 的 elastic 管理員密碼，所以這支腳本要由有叢集權限的人自己跑（AI 助理不跑）。它會：
  1. 開 kubectl port-forward 到 es-test 的 elasticsearch（結束時關掉）
  2. 取得 elastic 密碼：secrets.local.yaml 有填 es.admin_password 就用它，沒有就從叢集 Secret es-test/es-elastic 讀
     （只留在這支腳本的記憶體裡，不印、不寫檔）
  3. 呼叫 ES 的 _security/api_key，建一把只能讀 APM 索引的 key
  4. 把 key 寫進 secrets.local.yaml 的 es.AIOPS_ES_API_KEY（這個檔被 .gitignore 擋住；沒被擋住就拒絕寫）

畫面上只會印 key 的 id（不是機密），不會印密碼或 key 本身。每跑一次就多建一把新 key，舊的不會自動撤銷。
"""
import argparse
import base64
import re
import subprocess
import sys
import time
from pathlib import Path

import httpx
import yaml

ROOT = Path(__file__).resolve().parents[1]
SECRETS = ROOT / "secrets.local.yaml"
EXAMPLE = ROOT / "secrets.local.example.yaml"
INDICES = ["metrics-apm.transaction.1m-*", "traces-apm-*", "logs-apm.*"]
KEY_LINE = re.compile(r"^(?P<indent>[ \t]*)AIOPS_ES_API_KEY:.*$", re.MULTILINE)


def set_key(text: str, encoded: str) -> str:
    """把 yaml 文字裡的 AIOPS_ES_API_KEY 那一行換成新值，其餘（含註解、縮排）不動。找不到那一行就報錯。"""
    if not KEY_LINE.search(text):
        raise ValueError("secrets.local.yaml 裡找不到 AIOPS_ES_API_KEY 這一行，請對照 secrets.local.example.yaml")
    return KEY_LINE.sub(lambda m: f'{m["indent"]}AIOPS_ES_API_KEY: "{encoded}"', text, count=1)


def read_password(context: str, namespace: str, secret: str) -> str:
    out = subprocess.run(["kubectl", "--context", context, "-n", namespace, "get", "secret", secret,
                          "-o", "go-template={{range .data}}{{.}}{{end}}"], capture_output=True, text=True, check=True)
    return base64.b64decode(out.stdout.strip()).decode("utf-8")


def admin_password_from_file(text: str) -> str | None:
    """secrets.local.yaml 裡選填的 es.admin_password（放在這個被 gitignore 的檔，換環境才查得到）。沒填回傳 None。"""
    value = ((yaml.safe_load(text) or {}).get("es") or {}).get("admin_password")
    return str(value) if value else None


def wait_for_es(url: str, timeout: float = 20.0) -> None:
    end = time.time() + timeout
    while time.time() < end:
        try:
            httpx.get(url, timeout=2)  # 401 也算通：代表連到 ES 了，只是還沒帶密碼
            return
        except httpx.HTTPError:
            time.sleep(0.5)
    raise SystemExit(f"連不到 {url}，port-forward 沒起來（叢集有在跑嗎？）")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--context", default="rancher-desktop")
    ap.add_argument("--namespace", default="es-test")
    ap.add_argument("--secret", default="es-elastic")
    ap.add_argument("--port", type=int, default=19200)
    args = ap.parse_args()

    ignored = subprocess.run(["git", "check-ignore", "-q", "secrets.local.yaml"], cwd=ROOT).returncode == 0
    if not ignored:
        raise SystemExit("secrets.local.yaml 沒有被 .gitignore 擋住，拒絕寫入金鑰（先檢查 .gitignore）")
    text = SECRETS.read_text(encoding="utf-8") if SECRETS.exists() else EXAMPLE.read_text(encoding="utf-8")
    set_key(text, "x")  # 先確認格式對，免得 key 建出來才發現寫不進去

    url = f"http://127.0.0.1:{args.port}"
    forward = subprocess.Popen(["kubectl", "--context", args.context, "-n", args.namespace, "port-forward",
                                "svc/elasticsearch", f"{args.port}:9200"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for_es(url)
        password = admin_password_from_file(text) or read_password(args.context, args.namespace, args.secret)
        resp = httpx.post(f"{url}/_security/api_key", auth=("elastic", password), timeout=20, json={
            "name": "aiops-platform-readonly",
            "role_descriptors": {"apm-read": {"indices": [{"names": INDICES,
                                                           "privileges": ["read", "view_index_metadata"]}]}}})
        del password
    finally:
        forward.terminate()
    if resp.status_code != 200 or "encoded" not in resp.json():
        raise SystemExit(f"建 key 失敗：HTTP {resp.status_code} {resp.text[:200]}")
    body = resp.json()
    SECRETS.write_text(set_key(text, body["encoded"]), encoding="utf-8", newline="\n")
    print(f"完成：已把唯讀 key（id={body['id']}）寫進 {SECRETS.name}；畫面上不顯示 key 本身。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
