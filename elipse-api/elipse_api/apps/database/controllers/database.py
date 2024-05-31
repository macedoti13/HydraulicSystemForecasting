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
        print(e)
        if "duplicate key value violates unique constraint" in str(e):
            raise HTTPException(status_code=409, detail=str("Error inserting data: duplicated key"))
        raise HTTPException(status_code=409, detail=str("Error inserting data: Schama violation"))

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


def read_one(table_name: str, condition: dict[str, Any]) -> dict[str, Any]:

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select().where(table.columns[condition["column"]] == condition["value"])
        result = session.execute(query).fetchone()

    except SQLAlchemyError as e:
        print(e)
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error reading data: internal server error"))
    except Exception as e:
        print(e)
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error reading data: internal server error"))

    finally:
        session.close()

    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    return dict(result._mapping)


def read_all(table_name: str, limit: int, offset: int) -> list[dict[str, Any]]:

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select().limit(limit).offset(offset)
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


def update_table(data: dict[str, Any], table_name: str, condition: dict[str, Any]):

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        print(data)
        query = table.update().where(table.columns[condition["column"]] == condition["value"]).values(data)
        session.execute(query)
        session.commit()

    except IntegrityError as e:
        session.rollback()
        raise HTTPException(status_code=409, detail=str("Error updating data: duplicated key"))
    except DataError as e:
        session.rollback()
        raise HTTPException(status_code=422, detail=str("Error updating data: wrong data foramt"))
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error updating data: internal server error"))
    except Exception as e:
        print(e)
        session.rollback()
        raise HTTPException(status_code=500, detail=str("Error updating data: internal server error"))

    finally:
        session.close()

    return "Data updated successfully"


def delete_from_table(table_name: str, condition: dict[str, Any]):
    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.delete().where(table.columns[condition["column"]] == condition["value"])
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
