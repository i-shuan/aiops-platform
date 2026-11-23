"""給主管看的一張圖：產線指標儀表板（三張時序圖）＋事故註記的浮出視窗，1440×900、兩倍像素密度。

  pip install playwright          # 用本機已安裝的 Chrome，不另外下載瀏覽器
  kubectl -n drill-team port-forward svc/grafana 13001:3000
  G_USER=admin G_PASS=... python scripts/grafana_screenshot_boss.py <輸出檔> <開始毫秒> <結束毫秒> [第幾張圖 0-2] [滑鼠停在註記的哪裡 0-1]

預設停在第一張圖；docs/demo_screenshots/poc_demo_boss.png 是停在第三張圖（2 0.5），浮出視窗落在儀表板下方的空白處。
認證放在 HTTP 標頭（帳密從環境變數讀，不印出來），不經過登入表單。只給本機演練環境用。
"""
import base64, os, sys
from playwright.sync_api import sync_playwright

out, t_from, t_to = sys.argv[1], sys.argv[2], sys.argv[3]
which, frac = int(sys.argv[4]) if len(sys.argv) > 4 else 0, float(sys.argv[5]) if len(sys.argv) > 5 else 0.5
auth = base64.b64encode(f"{os.environ['G_USER']}:{os.environ['G_PASS']}".encode()).decode()
with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=2, locale="zh-TW", timezone_id="Asia/Taipei",
                              extra_http_headers={"Authorization": f"Basic {auth}"})
    page = ctx.new_page()
    page.goto(f"http://127.0.0.1:13001/d/aiops-line-sim?orgId=1&from={t_from}&to={t_to}&kiosk&refresh=", wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle", timeout=60000)
    page.wait_for_timeout(6000)
    markers = page.locator('[data-testid*="annotation-marker"], [class*="annotation-marker"], [class*="AnnotationMarker"]')
    boxes = sorted((markers.nth(i).bounding_box() for i in range(markers.count())), key=lambda b: (round(b["y"]), b["x"]))
    rows = sorted({round(b["y"]) for b in boxes})
    print("markers", len(boxes), "rows", rows)
    box = next(b for b in boxes if round(b["y"]) == rows[min(which, len(rows) - 1)])
    page.mouse.move(box["x"] + box["width"] * frac, box["y"] + box["height"] / 2)
    page.wait_for_timeout(2500)
    page.screenshot(path=out)
    browser.close()
