from fastapi import APIRouter

from . import health

SERVICE_NAME = __name__.split(".")[-1].title()


def get_router() -> APIRouter:
    base_router = APIRouter(tags=[SERVICE_NAME])
    base_router.include_router(health.router)
    return base_router
