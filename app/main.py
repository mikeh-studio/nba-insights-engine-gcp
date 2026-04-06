from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import SUPPORTED_SEASON, Settings, get_settings
from app.repository import (
    BigQueryWarehouseRepository,
    CompareFocus,
    CompareWindow,
    WarehouseRepository,
    get_compare_focus_options,
    get_compare_window_options,
)
from app.telemetry import (
    instrument_compare_view,
    instrument_dashboard_view,
    instrument_player_view,
)

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
TRACKING_CAP = 8

app = FastAPI(title="NBA 2025-26 Public API", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


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
    q: str = Query(min_length=1, max_length=64),
) -> dict:
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Search query must not be blank")
    return {
        "season": SUPPORTED_SEASON,
        "query": query,
        "items": repo.search_players(query, limit=settings.max_search_results),
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


@app.get("/api/compare")
def api_compare(
    player_a_id: int,
    player_b_id: int,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    *,
    window: CompareWindow = Query(default="last_5"),
    focus: CompareFocus = Query(default="balanced"),
) -> dict:
    if player_a_id == player_b_id:
        raise HTTPException(
            status_code=400,
            detail="Compare players must be different",
        )
    return repo.get_compare(player_a_id, player_b_id, window=window, focus=focus)


@app.get("/api/health")
def api_health(repo: Annotated[WarehouseRepository, Depends(get_repository)]) -> dict:
    return repo.get_health()


@app.get("/", response_class=HTMLResponse)
def home(
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    as_of_date: str | None = Query(default=None),
) -> HTMLResponse:
    dashboard = repo.get_dashboard(as_of_date=as_of_date)
    health = repo.get_health()
    instrument_dashboard_view(
        route="/",
        season=SUPPORTED_SEASON,
        health=health,
        dashboard=dashboard,
    )
    context = {
        "request": request,
        "page_title": "NBA 2025-26 Stats Dashboard",
        "season": SUPPORTED_SEASON,
        "dashboard": dashboard,
        "health": health,
        "tracking_cap": TRACKING_CAP,
    }
    return templates.TemplateResponse(request, "index.html", context)


@app.get("/players/{player_id}", response_class=HTMLResponse)
def player_page(
    player_id: int,
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
) -> HTMLResponse:
    player_detail = repo.get_player_detail(player_id)
    if player_detail is None:
        raise HTTPException(status_code=404, detail="Player not found")
    health = repo.get_health()
    instrument_player_view(
        route="/players/{player_id}",
        season=SUPPORTED_SEASON,
        health=health,
        player_detail=player_detail,
    )
    context = {
        "request": request,
        "page_title": f"{player_detail['player']['player_name']} Stats Outlook",
        "season": SUPPORTED_SEASON,
        "player_detail": player_detail,
        "health": health,
        "tracking_cap": TRACKING_CAP,
    }
    return templates.TemplateResponse(request, "player.html", context)


@app.get("/api/players/{player_id}/game-log")
def api_player_game_log(
    player_id: int,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    limit: int = Query(30, ge=1, le=82),
) -> dict:
    result = repo.get_player_game_log(player_id, limit=limit)
    if result is None:
        raise HTTPException(status_code=404, detail="Player not found")
    return {"season": SUPPORTED_SEASON, "item": result}


@app.get("/visualize", response_class=HTMLResponse)
def visualize_page(
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    player_id: int | None = None,
) -> HTMLResponse:
    health = repo.get_health()
    player_detail = None
    if player_id is not None:
        player_detail = repo.get_player_detail(player_id)
    context = {
        "request": request,
        "page_title": "Player Stats Explorer",
        "season": SUPPORTED_SEASON,
        "health": health,
        "player_detail": player_detail,
        "player_id": player_id,
    }
    return templates.TemplateResponse(request, "visualize.html", context)


@app.get("/compare", response_class=HTMLResponse)
def compare_page(
    request: Request,
    repo: Annotated[WarehouseRepository, Depends(get_repository)],
    player_a_id: int | None = None,
    player_b_id: int | None = None,
    window: CompareWindow = Query(default="last_5"),
    focus: CompareFocus = Query(default="balanced"),
) -> HTMLResponse:
    compare_error: str | None = None
    comparison: dict | None = None
    health = repo.get_health()
    player_a_detail = (
        repo.get_player_detail(player_a_id) if player_a_id is not None else None
    )
    if player_a_id is not None and player_a_detail is None:
        raise HTTPException(status_code=404, detail="Player not found")
    if player_a_id is not None and player_b_id is not None:
        if player_a_id == player_b_id:
            compare_error = "Compare players must be different."
        else:
            comparison = repo.get_compare(
                player_a_id,
                player_b_id,
                window=window,
                focus=focus,
            )
    instrument_compare_view(
        route="/compare",
        season=SUPPORTED_SEASON,
        health=health,
        comparison=comparison,
    )
    context = {
        "request": request,
        "page_title": "Compare Players",
        "season": SUPPORTED_SEASON,
        "health": health,
        "player_a_detail": player_a_detail,
        "player_a_id": player_a_id,
        "player_b_id": player_b_id,
        "comparison": comparison,
        "compare_error": compare_error,
        "window": window,
        "window_label": next(
            option["label"]
            for option in get_compare_window_options()
            if option["key"] == window
        ),
        "focus": focus,
        "focus_label": next(
            option["label"]
            for option in get_compare_focus_options()
            if option["key"] == focus
        ),
        "compare_window_options": get_compare_window_options(),
        "compare_focus_options": get_compare_focus_options(),
        "tracking_cap": TRACKING_CAP,
    }
    return templates.TemplateResponse(request, "compare.html", context)
