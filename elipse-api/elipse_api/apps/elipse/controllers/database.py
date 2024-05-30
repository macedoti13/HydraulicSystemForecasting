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


def read_one(table_name: str, condition: dict[str, Any]) -> dict[str, Any]:

    session = Session()

    if table_name not in metadata.tables:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        table = Table(table_name, metadata, autoload_with=engine)
        query = table.select().where(table.columns[condition["column"]] == condition["value"])
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
