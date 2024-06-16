from fastapi import APIRouter

router = APIRouter()


@router.get("/", status_code=200)
def health() -> dict[str, str]:
    print("Health check")
    return {"status": "ok"}
