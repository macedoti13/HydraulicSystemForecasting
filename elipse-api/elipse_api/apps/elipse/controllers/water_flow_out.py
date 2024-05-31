from typing import Any

from dotenv import load_dotenv

from elipse_api.apps.database.controllers.database import read_all as read_all_db
from elipse_api.apps.database.controllers.database import read_one as read_one_db

load_dotenv()

table_name = "water_flow_out"


def read_one(timestamp: str) -> dict[str, Any]:
    column_name = "timestamp"
    condition = {"column": column_name, "value": timestamp}
    return read_one_db(table_name, condition)


def read_many(limit: int, offset: int) -> list[dict[str, Any]]:
    return read_all_db(table_name, limit, offset)
