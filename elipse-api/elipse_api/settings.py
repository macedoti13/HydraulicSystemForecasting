from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Uvicorn (API)
    HOST: str = "0.0.0.0"
    PORT: int = 5000
    RELOAD: bool = True
    WORKERS: int = 1

    # DB
    # OPTX_AI_API_MONGODB_URI: str = Field(default="mongodb://localhost:27017")
    # OPTX_AI_API_MONGODB_DBNAME: str = Field(default="optx_ai")

    class Config:
        env_file = ".env"


settings = Settings()
