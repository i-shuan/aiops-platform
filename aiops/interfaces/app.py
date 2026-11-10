from fastapi import FastAPI

from aiops.engine.single_flight import SingleFlight
from aiops.interfaces.bootstrap import init_error_handlers, init_infrastructure, init_operators, init_routers


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

    return app


app = create_app()
