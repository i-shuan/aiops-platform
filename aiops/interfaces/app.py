from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from aiops.engine.single_flight import SingleFlight
from aiops.interfaces.bootstrap import init_error_handlers, init_infrastructure, init_operators, init_routers


# 回饋頁：一份靜態 HTML（不含任何資料與機密）。送出時由頁面帶 Bearer token 打既有的 /api/v1/tuning/feedback
FEEDBACK_PAGE = (Path(__file__).parent / "static" / "feedback.html").read_text(encoding="utf-8")



def create_app() -> FastAPI:
    app = FastAPI(title="AIOps Platform")
    init_infrastructure()
    init_operators()
    init_error_handlers(app)
    init_routers(app)
    # 同一個事件群的 RCA 同時只跑一份（api/runs.generate_rca）。掛在這個 app 實例上，不是模組層級的狀態
    app.state.rca_flight = SingleFlight()

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/feedback", response_class=HTMLResponse, include_in_schema=False)
    def feedback_page():
        return FEEDBACK_PAGE

    return app


app = create_app()
