"""用本機的 Chrome（無頭）開演練 Grafana，截三張展示用的畫面：事件群總覽、產線時序圖（滑鼠停在事故註記上）、告警規則的標籤。

  pip install playwright          # 用本機已安裝的 Chrome，不另外下載瀏覽器
  kubectl -n drill-team port-forward svc/grafana 13001:3000
  G_USER=admin G_PASS=... python scripts/grafana_screenshots.py docs/demo_screenshots [1 2 3]

認證放在 HTTP 標頭（帳密從環境變數讀，不印出來），不經過登入表單。只給本機演練環境用。
畫面上要有事故才拍得到註記：先照 scripts/line_sim 注入故障、等排程跑過、跑一次 RCA。
"""
import base64, os, sys
from playwright.sync_api import sync_playwright

BASE = "http://127.0.0.1:13001"
OUT = sys.argv[1]
auth = base64.b64encode(f"{os.environ['G_USER']}:{os.environ['G_PASS']}".encode()).decode()
only = sys.argv[2:] or ["1", "2", "3"]

with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=2, locale="zh-TW",
                              extra_http_headers={"Authorization": f"Basic {auth}"})
    page = ctx.new_page()

    def settle(ms=6000):
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(ms)

    if "1" in only:
        page.goto(f"{BASE}/d/aiops-incidents?orgId=1&from=now-1h&to=now&kiosk", wait_until="domcontentloaded")
        settle()
        page.screenshot(path=os.path.join(OUT, "01_aiops_incidents.png"))
        print("1 ok", page.title())
    if "2" in only:
        page.goto(f"{BASE}/d/aiops-line-sim?orgId=1&from=now-40m&to=now&kiosk", wait_until="domcontentloaded")
        settle()
        markers = page.locator('[data-testid*="annotation-marker"], [class*="annotation-marker"], [class*="AnnotationMarker"]')
        print("markers:", markers.count())
        if markers.count():
            # 第一張圖（MQ）上開始時間最早的那一筆 = 產線事故（P0，有 RCA）；滑鼠停在它的起點附近
            boxes = [(markers.nth(i).bounding_box(), i) for i in range(markers.count())]
            top = min(b["y"] for b, _ in boxes if b)
            box, idx = min(((b, i) for b, i in boxes if b and abs(b["y"] - top) < 5), key=lambda bi: bi[0]["x"])
            print("marker box", box)
            page.mouse.move(box["x"] + 12, box["y"] + box["height"] / 2)
            page.wait_for_timeout(2500)
        page.screenshot(path=os.path.join(OUT, "02_line_metrics_rca.png"))
        print("2 ok")
    if "3" in only:
        page.goto(f"{BASE}/alerting/grafana/line-cim-connected/view?orgId=1", wait_until="domcontentloaded")
        settle(4000)
        page.screenshot(path=os.path.join(OUT, "03_rule_labels.png"))
        print("3 ok", page.title())
    browser.close()
