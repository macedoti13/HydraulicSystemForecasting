from fastapi import APIRouter, Body, Request

from ..controllers import description as description_controller

router = APIRouter(prefix="/database")
