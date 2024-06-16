from typing import Any

from fastapi import APIRouter, Body, Request

from ..controllers import database as db_controller

router = APIRouter(prefix="/database")


@router.post("/create/{table_name}", status_code=201)
def write_to_table(
    table_name: str,
    data: dict[str, Any] | list[dict[str, Any]] = Body([{"Name": "John", "Age": 30}, {"Name": "Jane", "Age": 25}]),
):
    return db_controller.write_to_table(data, table_name)


@router.get("/read_many/{table_name}")
def read_many(table_name: str, limit: int = 100, offset: int = 0):
    return db_controller.read_all(table_name, limit, offset)


@router.get("/read_one/{table_name}/{column_name}/{value}")
def read_one(
    table_name: str,
    column_name: str,
    value: Any,
):
    condition = {"column": column_name, "value": value}
    return db_controller.read_one(table_name, condition)


@router.put("/update/{table_name}/{column_name}/{value}")
def update_one(
    table_name: str,
    column_name: str,
    value: Any,
    data: dict[str, Any] = Body({"Name": "John", "Age": 31}),
):
    conditions = {"column": column_name, "value": value}
    return db_controller.update_table(data, table_name, conditions)


@router.delete("/delete/{table_name}/{column_name}/{value}")
def delete_one(
    table_name: str,
    column_name: str,
    value: Any,
):
    conditions = {"column": column_name, "value": value}
    return db_controller.delete_from_table(table_name, conditions)
