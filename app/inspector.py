"""Same-origin, dependency-free operator UI. No credentials are embedded in assets."""

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse

router = APIRouter(include_in_schema=False)
ROOT = Path(__file__).parent / "static"
HEADERS = {
    "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}


@router.get("/")
async def console_home():
    return RedirectResponse("/inspector", status_code=307, headers=HEADERS)


@router.get("/inspector")
async def inspector():
    return FileResponse(ROOT / "index.html", headers=HEADERS)


@router.get("/inspector/assets/{name}")
async def asset(name: str):
    if name not in {"studio.css", "studio.js"}:
        raise HTTPException(404)
    return FileResponse(ROOT / name, headers=HEADERS)
