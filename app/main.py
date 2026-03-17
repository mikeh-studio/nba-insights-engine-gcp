from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
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


@app.get("/api/recommendations")
def api_recommendations(
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    limit: int = Query(10, ge=1, le=50),
    insight_type: str | None = Query(default=None),
) -> dict:
    return {
        "season": SUPPORTED_SEASON,
        "items": repo.get_recommendations(limit=limit, insight_type=insight_type),
    }


@app.get("/api/rankings")
def api_rankings(
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    limit: int = Query(25, ge=1, le=100),
) -> dict:
    return {"season": SUPPORTED_SEASON, "items": repo.get_rankings(limit=limit)}


@app.get("/api/players/search")
def api_player_search(
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    settings: Annotated[Settings, Depends(get_settings)],
    q: str = Query(min_length=1),
) -> dict:
    return {
        "season": SUPPORTED_SEASON,
        "query": q,
        "items": repo.search_players(q, limit=settings.max_search_results),
    }


@app.get("/api/players/{player_id}")
def api_player_detail(
    player_id: int,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
) -> dict:
    detail = repo.get_player_detail(player_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return {"season": SUPPORTED_SEASON, "item": detail}


@app.get("/api/health")
def api_health(repo: Annotated[WarehouseRepository, Depends(get_repository)]) -> dict:
    return repo.get_health()


@app.get("/", response_class=HTMLResponse)
def home(
    request: Request, repo: Annotated[WarehouseRepository, Depends(get_repository)]
) -> HTMLResponse:
    context = {
        "request": request,
        "page_title": "NBA 2025-26 Fantasy Dashboard",
        "season": SUPPORTED_SEASON,
        "recommendations": repo.get_recommendations(limit=5),
        "rankings": repo.get_rankings(limit=8),
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


@app.get("/recommendations", response_class=HTMLResponse)
def recommendations_page(
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
) -> HTMLResponse:
    context = {
        "request": request,
        "page_title": "NBA 2025-26 Fantasy Recommendations",
        "season": SUPPORTED_SEASON,
        "recommendations": repo.get_recommendations(limit=20),
        "health": repo.get_health(),
    }
    return templates.TemplateResponse(request, "recommendations.html", context)


@app.get("/players/{player_id}", response_class=HTMLResponse)
def player_page(
    player_id: int,
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
) -> HTMLResponse:
    player_detail = repo.get_player_detail(player_id)
    if player_detail is None:
        raise HTTPException(status_code=404, detail="Player not found")
    context = {
        "request": request,
        "page_title": f"{player_detail['player']['player_name']} Fantasy Outlook",
        "season": SUPPORTED_SEASON,
        "player_detail": player_detail,
        "health": repo.get_health(),
    }
    return templates.TemplateResponse(request, "player.html", context)
