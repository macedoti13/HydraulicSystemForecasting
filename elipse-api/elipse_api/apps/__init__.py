from fastapi import APIRouter, FastAPI

from ..settings import Settings
from . import health


def init_app(app: FastAPI, settings: Settings) -> None:
    router = APIRouter(prefix="/api")
    app.include_router(router)
    app.include_router(health.get_router())
