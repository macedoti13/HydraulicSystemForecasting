from fastapi import FastAPI

from elipse_api.settings import Settings

from . import apps


def create_app() -> FastAPI:
    settings = Settings()
    app = FastAPI(
        title="Elipse API",
        version="0.1.0",
        summary="API Developed by PUCRS students for Elipse.",
        contact={"name": "Pedro Pagnussat", "email": "p.pagnussat@edu.pucrs.br"},
    )
    apps.init_app(app, settings)

    return app
