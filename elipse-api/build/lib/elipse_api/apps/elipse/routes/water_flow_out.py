from typing import Any

from fastapi import APIRouter, Body, Request

from ..controllers import water_flow_out as wfo_controller

router = APIRouter(prefix="/elipse")


@router.get("/read_one_water_output", status_code=201)
def read_one(timestamp: str):
    return wfo_controller.read_one(timestamp)


@router.get("/read_many_water_output", status_code=201)
def read_many(limit: int = 100, offset: int = 0):
    return wfo_controller.read_many(limit, offset)
