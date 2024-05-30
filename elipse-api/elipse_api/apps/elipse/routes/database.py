from typing import Any

from fastapi import APIRouter, Body, Request

from ..controllers import database as db_controller

router = APIRouter(prefix="/elipse")


@router.post("/create", status_code=201)
def write_to_table(
    request: Request,
):
    pass
