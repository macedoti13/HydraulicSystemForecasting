from fastapi import APIRouter, FastAPI

from ..settings import Settings
from . import database, health


def init_app(app: FastAPI, settings: Settings) -> None:
    router = APIRouter(prefix="/api")

    app.include_router(router)
    app.include_router(health.get_router())
    app.include_router(database.get_router())
