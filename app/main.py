from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import SUPPORTED_SEASON, Settings, get_settings
from app.repository import BigQueryWarehouseRepository, WarehouseRepository

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app = FastAPI(title="NBA 2025-26 Public API", version="1.0.0")


def get_repository(
    settings: Annotated[Settings, Depends(get_settings)],
) -> WarehouseRepository:
    return BigQueryWarehouseRepository(settings)


@app.get("/api/leaderboard")
def api_leaderboard(
    repo: Annotated[WarehouseRepository, Depends(get_repository)]
) -> dict:
    return {"season": SUPPORTED_SEASON, "items": repo.get_leaderboard()}


@app.get("/api/trends")
def api_trends(repo: Annotated[WarehouseRepository, Depends(get_repository)]) -> dict:
    return {"season": SUPPORTED_SEASON, "items": repo.get_trends()}


@app.get("/api/analysis/latest")
def api_analysis_latest(
    repo: Annotated[WarehouseRepository, Depends(get_repository)]
) -> dict:
    return {"season": SUPPORTED_SEASON, "item": repo.get_latest_analysis()}


@app.get("/api/health")
def api_health(repo: Annotated[WarehouseRepository, Depends(get_repository)]) -> dict:
    return repo.get_health()


@app.get("/", response_class=HTMLResponse)
def home(
    request: Request, repo: Annotated[WarehouseRepository, Depends(get_repository)]
) -> HTMLResponse:
    context = {
        "request": request,
        "page_title": "NBA 2025-26 Daily Board",
        "season": SUPPORTED_SEASON,
        "leaderboard": repo.get_leaderboard(),
        "trends": repo.get_trends(5),
        "analysis": repo.get_latest_analysis(),
        "health": repo.get_health(),
    }
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/analysis", response_class=HTMLResponse)
def analysis_page(
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
) -> HTMLResponse:
    context = {
        "request": request,
        "page_title": "NBA 2025-26 Analysis Snapshot",
        "season": SUPPORTED_SEASON,
        "analysis": repo.get_latest_analysis(),
        "health": repo.get_health(),
    }
    return templates.TemplateResponse(request, "analysis.html", context)
