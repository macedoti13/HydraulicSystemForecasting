import uvicorn
from pydantic import ValidationError

from .settings import Settings


def main():
    try:
        settings = Settings()
        uvicorn.run(
            # TODO: see app
            "elipse_api:create_app",
            factory=True,
            forwarded_allow_ips="*",
            host=settings.HOST,
            port=settings.PORT,
            reload=settings.RELOAD,
            workers=settings.WORKERS,
        )
    except ValidationError as e:
        print(e)
        exit(1)


if __name__ == "__main__":
    main()
