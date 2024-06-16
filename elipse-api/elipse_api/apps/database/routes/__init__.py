from fastapi import APIRouter

from . import database

SERVICE_NAME = __name__.split(".")[-2].title()


def get_router() -> APIRouter:
    base_router = APIRouter(tags=[SERVICE_NAME])
    base_router.include_router(database.router)
    return base_router
