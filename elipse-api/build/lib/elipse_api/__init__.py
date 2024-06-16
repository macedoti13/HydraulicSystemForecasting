from fastapi import FastAPI

from elipse_api.settings import Settings

from . import apps


def create_app() -> FastAPI:
    settings = Settings()
    app = FastAPI()
    apps.init_app(app, settings)

    return app
