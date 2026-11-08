from fastapi import FastAPI

from aiops.initialization import init_error_handlers, init_infrastructure, init_operators, init_routers


def create_app() -> FastAPI:
    app = FastAPI(title="AIOps Platform")
    init_infrastructure()
    init_operators()
    init_error_handlers(app)
    init_routers(app)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()
