import os
from typing import Any

from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

load_dotenv()
connection_string = f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"

engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


metadata = MetaData()
metadata.reflect(bind=engine)


def write_to_table(data: dict["str", Any] | list[dict["str", Any]], table_name: str):

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    if not isinstance(data, list):
        data = [data]

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        session.execute(table.insert(), data)
        session.commit()

    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=409, detail=str("Error inserting data: duplicated key"))
    except DataError as e:
        session.rollback()
        raise HTTPException(status_code=422, detail=str("Error inserting data: wrong data foramt"))
    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error inserting data: internal server error"))
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error inserting data: internal server error"))

    finally:
        session.close()

    return "Data inserted successfully"


def read_one(table_name: str, conditions: dict[str, Any]) -> dict[str, Any]:

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select().where(table.columns[conditions["column"]] == conditions["value"])
        result = session.execute(query).fetchone()
        # parse result to dict

    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error reading data: internal server error"))
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error reading data: internal server error"))

    finally:
        session.close()

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    return dict(result._mapping)


def read_all(table_name: str) -> list[dict[str, Any]]:

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select()
        result = session.execute(query).fetchall()

    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error reading data: internal server error"))
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error reading data: internal server error"))

    finally:
        session.close()

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    return [dict(row._mapping) for row in result]


def update_table(data: dict[str, Any], table_name: str, conditions: dict[str, Any]):

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.update().where(table.columns[conditions["column"]] == conditions["value"]).values(data)
        session.execute(query)
        session.commit()

    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=409, detail=str("Error updating data: duplicated key"))
    except DataError as e:
        session.rollback()
        raise HTTPException(status_code=422, detail=str("Error updating data: wrong data foramt"))
    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error updating data: internal server error"))
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error updating data: internal server error"))

    finally:
        session.close()

    return "Data updated successfully"


def delete_from_table(table_name: str, conditions: dict[str, Any]):
    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.delete().where(table.columns[conditions["column"]] == conditions["value"])
        session.execute(query)
        session.commit()

    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error deleting data: internal server error"))
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error deleting data: internal server error"))

    finally:
        session.close()

    return "Data deleted successfully"


a = read_all("elipse_bronze")
print(a)
