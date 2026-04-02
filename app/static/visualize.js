(function () {
  "use strict";

  const STAT_COLORS = {
    pts: "#f04e23",
    reb: "#5dd39e",
    ast: "#6bc5f2",
    stl: "#f0c823",
    blk: "#c77dff",
    fg3m: "#ff6b6b",
    tov: "#a7afb7",
    min: "#e7e0d4",
    fantasy_points_simple: "#ff6a3d",
  };

  const STAT_LABELS = {
    pts: "PTS",
    reb: "REB",
    ast: "AST",
    stl: "STL",
    blk: "BLK",
    fg3m: "3PM",
    tov: "TOV",
    min: "MIN",
    fantasy_points_simple: "BSI",
  };

  const HIGH_RANGE_STATS = new Set(["pts", "min", "fantasy_points_simple"]);
  const RADAR_AXES = ["pts", "reb", "ast", "stl", "blk", "fg3m"];

  let currentChart = null;
  let gameLog = null;
  let playerProfile = null;
  let currentPlayerId = null;
  let currentChartType = "trend";
  let searchTimeout = null;

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  function getActiveStats() {
    return Array.from($$(".viz-chip.active")).map((el) => el.dataset.stat);
  }

  function getActiveStatCount() {
    return $$(".viz-chip.active").length;
  }

  // --- Search ---
  function initSearch() {
    const input = $("#viz-player-search");
    const resultsEl = $("#viz-search-results");

    input.addEventListener("input", () => {
      clearTimeout(searchTimeout);
      const q = input.value.trim();
      if (q.length < 2) {
        resultsEl.hidden = true;
        return;
      }
      searchTimeout = setTimeout(() => doSearch(q), 280);
    });

    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape") resultsEl.hidden = true;
    });

    document.addEventListener("click", (e) => {
      if (!e.target.closest(".viz-search-row")) resultsEl.hidden = true;
    });

    $("#viz-load-btn").addEventListener("click", () => {
      const q = input.value.trim();
      if (q.length >= 2) doSearch(q);
    });
  }

  async function doSearch(query) {
    const resultsEl = $("#viz-search-results");
    try {
      const resp = await fetch(
        `/api/players/search?q=${encodeURIComponent(query)}`
      );
      if (!resp.ok) return;
      const data = await resp.json();
      if (!data.items || data.items.length === 0) {
        resultsEl.innerHTML =
          '<div class="viz-search-result"><span class="meta">No results</span></div>';
        resultsEl.hidden = false;
        return;
      }
      resultsEl.innerHTML = data.items
        .map(
          (p) =>
            `<button class="viz-search-result" data-id="${p.player_id}">
              ${p.player_name}
              <span class="meta">${p.latest_season || ""}</span>
            </button>`
        )
        .join("");
      resultsEl.hidden = false;

      resultsEl.querySelectorAll(".viz-search-result[data-id]").forEach((btn) =>
        btn.addEventListener("click", () => {
          resultsEl.hidden = true;
          const id = parseInt(btn.dataset.id, 10);
          window.history.replaceState(null, "", `/visualize?player_id=${id}`);
          loadPlayer(id);
        })
      );
    } catch {
      /* swallow */
    }
  }

  // --- Data loading ---
  async function loadPlayer(playerId) {
    currentPlayerId = playerId;
    const [logResp, profileResp] = await Promise.all([
      fetch(`/api/players/${playerId}/game-log`),
      fetch(`/api/players/${playerId}`),
    ]);

    if (!logResp.ok || !profileResp.ok) {
      showEmpty("Could not load player data.");
      return;
    }

    const logData = await logResp.json();
    const profileData = await profileResp.json();
    gameLog = logData.item;
    playerProfile = profileData.item;

    $("#viz-player-search").value = gameLog.player_name || "";
    renderChart();
  }

  // --- Stat chips ---
  function initChips() {
    $$(".viz-chip").forEach((chip) => {
      chip.addEventListener("click", () => {
        if (chip.classList.contains("active")) {
          chip.classList.remove("active");
        } else {
          if (currentChartType === "trend" && getActiveStatCount() >= 5) return;
          chip.classList.add("active");
        }
        if (gameLog) renderChart();
      });
    });
  }

  // --- Chart type tabs ---
  function initTabs() {
    $$(".viz-tab").forEach((tab) => {
      tab.addEventListener("click", () => {
        $$(".viz-tab").forEach((t) => {
          t.classList.remove("active");
          t.classList.add("secondary");
        });
        tab.classList.add("active");
        tab.classList.remove("secondary");
        currentChartType = tab.dataset.chart;
        if (gameLog) renderChart();
      });
    });
  }

  // --- Rendering ---
  function showEmpty(msg) {
    $("#viz-empty-state").hidden = false;
    $("#viz-empty-state").querySelector("p").textContent =
      msg || "Search for a player above to explore their stats.";
    $("#viz-canvas-wrap").hidden = true;
    if (currentChart) {
      currentChart.destroy();
      currentChart = null;
    }
  }

  function showCanvas() {
    $("#viz-empty-state").hidden = true;
    $("#viz-canvas-wrap").hidden = false;
  }

  function renderChart() {
    if (!gameLog || !gameLog.games || gameLog.games.length === 0) {
      showEmpty("No game data available for this player.");
      return;
    }

    showCanvas();
    if (currentChart) {
      currentChart.destroy();
      currentChart = null;
    }

    const titleEl = $("#viz-chart-title");
    const metaEl = $("#viz-chart-meta");
    const playerName = gameLog.player_name || "Player";

    if (currentChartType === "trend") {
      titleEl.textContent = `${playerName} Game Trend`;
      metaEl.textContent = `${gameLog.games.length} games`;
      renderTrendChart();
    } else if (currentChartType === "compare") {
      titleEl.textContent = `${playerName} Window Averages`;
      metaEl.textContent = "Last 5 / Prior 5 / Last 10";
      renderCompareChart();
    } else if (currentChartType === "profile") {
      titleEl.textContent = `${playerName} Category Profile`;
      metaEl.textContent = "Z-score radar";
      renderRadarChart();
    }
  }

  function renderTrendChart() {
    const ctx = $("#viz-chart-canvas").getContext("2d");
    const stats = getActiveStats();
    if (stats.length === 0) {
      showEmpty("Select at least one stat to chart.");
      return;
    }

    const games = gameLog.games;
    const labels = games.map((g) => {
      const d = g.game_date || "";
      return d.length >= 10 ? d.slice(5, 10) : d;
    });

    const hasHigh = stats.some((s) => HIGH_RANGE_STATS.has(s));
    const hasLow = stats.some((s) => !HIGH_RANGE_STATS.has(s));
    const useDualAxis = hasHigh && hasLow;

    const datasets = stats.map((stat) => ({
      label: STAT_LABELS[stat] || stat,
      data: games.map((g) => parseFloat(g[stat]) || 0),
      borderColor: STAT_COLORS[stat],
      backgroundColor: STAT_COLORS[stat] + "33",
      borderWidth: 2,
      pointRadius: 3,
      pointHoverRadius: 5,
      tension: 0.3,
      yAxisID: useDualAxis && !HIGH_RANGE_STATS.has(stat) ? "y1" : "y",
    }));

    const scales = {
      x: {
        ticks: { color: "#a7afb7", maxRotation: 45 },
        grid: { color: "rgba(255,255,255,0.06)" },
      },
      y: {
        position: "left",
        ticks: { color: "#a7afb7" },
        grid: { color: "rgba(255,255,255,0.06)" },
      },
    };

    if (useDualAxis) {
      scales.y1 = {
        position: "right",
        ticks: { color: "#a7afb7" },
        grid: { drawOnChartArea: false },
      };
    }

    currentChart = new Chart(ctx, {
      type: "line",
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { labels: { color: "#f3f1ec" } },
          tooltip: {
            callbacks: {
              title(items) {
                const idx = items[0].dataIndex;
                const g = games[idx];
                const opp = g.opponent_abbr || "";
                const wl = g.wl || "";
                const dt = g.game_date || "";
                return `${dt}  vs ${opp}  ${wl}`;
              },
            },
          },
        },
        scales,
      },
    });
  }

  function renderCompareChart() {
    const ctx = $("#viz-chart-canvas").getContext("2d");
    const stats = getActiveStats();
    if (stats.length === 0 || !playerProfile) {
      showEmpty("Select stats and ensure player data is loaded.");
      return;
    }

    const recentForm = playerProfile.recent_form || [];
    const windowMap = {};
    recentForm.forEach((w) => {
      windowMap[w.window_key] = w;
    });

    const windowKeys = ["last_5", "prior_5", "last_10"];
    const windowLabels = ["Last 5", "Prior 5", "Last 10"];
    const windowColors = ["#f04e23", "#6bc5f2", "#5dd39e"];

    const statFieldMap = {
      pts: "avg_pts",
      reb: "avg_reb",
      ast: "avg_ast",
      stl: "avg_stl",
      blk: "avg_blk",
      fg3m: "avg_fg3m",
      tov: "avg_tov",
      min: "avg_minutes",
      fantasy_points_simple: "fantasy_proxy",
    };

    const statLabels = stats.map((s) => STAT_LABELS[s] || s);

    const datasets = windowKeys.map((wk, i) => {
      const w = windowMap[wk] || {};
      return {
        label: windowLabels[i],
        data: stats.map((s) => parseFloat(w[statFieldMap[s]]) || 0),
        backgroundColor: windowColors[i] + "cc",
        borderColor: windowColors[i],
        borderWidth: 1,
      };
    });

    currentChart = new Chart(ctx, {
      type: "bar",
      data: { labels: statLabels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: "#f3f1ec" } },
        },
        scales: {
          x: {
            ticks: { color: "#a7afb7" },
            grid: { color: "rgba(255,255,255,0.06)" },
          },
          y: {
            ticks: { color: "#a7afb7" },
            grid: { color: "rgba(255,255,255,0.06)" },
          },
        },
      },
    });
  }

  function renderRadarChart() {
    const ctx = $("#viz-chart-canvas").getContext("2d");
    if (!playerProfile || !playerProfile.category_profile) {
      showEmpty("No category profile data available.");
      return;
    }

    const profile = playerProfile.category_profile;
    const zMap = {};
    profile.forEach((item) => {
      const key = item.category === "3PM" ? "fg3m" : item.category.toLowerCase();
      zMap[key] = item.impact_score;
    });

    const radarLabels = RADAR_AXES.map((s) => STAT_LABELS[s] || s);
    const radarData = RADAR_AXES.map((s) => (zMap[s] || 0) + 3);

    currentChart = new Chart(ctx, {
      type: "radar",
      data: {
        labels: radarLabels,
        datasets: [
          {
            label: gameLog.player_name || "Player",
            data: radarData,
            backgroundColor: "rgba(240, 78, 35, 0.2)",
            borderColor: "#f04e23",
            borderWidth: 2,
            pointBackgroundColor: "#f04e23",
            pointRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: "#f3f1ec" } },
          tooltip: {
            callbacks: {
              label(item) {
                const raw = item.raw - 3;
                return `${item.dataset.label}: z=${raw.toFixed(2)}`;
              },
            },
          },
        },
        scales: {
          r: {
            beginAtZero: true,
            min: 0,
            ticks: { color: "#a7afb7", backdropColor: "transparent" },
            grid: { color: "rgba(255,255,255,0.1)" },
            angleLines: { color: "rgba(255,255,255,0.1)" },
            pointLabels: { color: "#f3f1ec", font: { size: 13 } },
          },
        },
      },
    });
  }

  // --- Bootstrap ---
  function init() {
    initSearch();
    initChips();
    initTabs();
  }

  window.__vizBootstrap = function (playerId, profileData) {
    currentPlayerId = playerId;
    playerProfile = profileData;
    loadPlayer(playerId);
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
