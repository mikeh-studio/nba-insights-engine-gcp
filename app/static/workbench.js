export const TRACKING_KEY = "nba_workbench_tracked_players_v1";
export const TRACKING_VERSION = 1;
export const TRACKING_CAP = 8;

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function normalizePlayerIds(playerIds, cap = TRACKING_CAP) {
  if (!Array.isArray(playerIds)) {
    return [];
  }
  const normalized = [];
  for (const value of playerIds) {
    if (!Number.isInteger(value)) {
      return [];
    }
    if (!normalized.includes(value)) {
      normalized.push(value);
    }
  }
  if (normalized.length > cap) {
    return [];
  }
  return normalized;
}

export function normalizeTrackedPayload(payload, cap = TRACKING_CAP) {
  if (!isObject(payload) || payload.version !== TRACKING_VERSION) {
    return { version: TRACKING_VERSION, player_ids: [] };
  }
  const playerIds = normalizePlayerIds(payload.player_ids, cap);
  if (playerIds.length !== payload.player_ids.length) {
    return { version: TRACKING_VERSION, player_ids: [] };
  }
  return { version: TRACKING_VERSION, player_ids: playerIds };
}

export function parseTrackedPayload(raw, cap = TRACKING_CAP) {
  if (typeof raw !== "string" || !raw.trim()) {
    return { version: TRACKING_VERSION, player_ids: [] };
  }
  try {
    return normalizeTrackedPayload(JSON.parse(raw), cap);
  } catch {
    return { version: TRACKING_VERSION, player_ids: [] };
  }
}

export function serializeTrackedPayload(payload) {
  return JSON.stringify(payload);
}

export function loadTrackedPlayers(storage, cap = TRACKING_CAP) {
  if (!storage || typeof storage.getItem !== "function") {
    return { version: TRACKING_VERSION, player_ids: [] };
  }
  const normalized = parseTrackedPayload(storage.getItem(TRACKING_KEY), cap);
  storage.setItem(TRACKING_KEY, serializeTrackedPayload(normalized));
  return normalized;
}

export function saveTrackedPlayers(storage, payload) {
  if (!storage || typeof storage.setItem !== "function") {
    return;
  }
  storage.setItem(TRACKING_KEY, serializeTrackedPayload(payload));
}

export function addTrackedPlayer(payload, playerId, cap = TRACKING_CAP) {
  if (!Number.isInteger(playerId)) {
    return { payload, error: "invalid_player_id" };
  }
  if (payload.player_ids.includes(playerId)) {
    return { payload, error: null };
  }
  if (payload.player_ids.length >= cap) {
    return { payload, error: "cap_reached" };
  }
  return {
    payload: {
      version: TRACKING_VERSION,
      player_ids: [...payload.player_ids, playerId],
    },
    error: null,
  };
}

export function removeTrackedPlayer(payload, playerId) {
  return {
    version: TRACKING_VERSION,
    player_ids: payload.player_ids.filter((value) => value !== playerId),
  };
}

export function buildCompareHref(playerAId, playerBId, window, focus = "balanced") {
  return `/compare?player_a_id=${playerAId}&player_b_id=${playerBId}&window=${encodeURIComponent(window)}&focus=${encodeURIComponent(focus)}`;
}

function getStorage() {
  try {
    return globalThis.localStorage;
  } catch {
    return null;
  }
}

function formatTrackedSummary(count, cap) {
  return `${count}/${cap} tracked in this browser`;
}

function renderHeadshot(player) {
  const initials = player.player_initials || "NBA";
  const imageMarkup = player.headshot_url
    ? `<img src="${player.headshot_url}" alt="" loading="lazy" onerror="this.hidden=true; this.nextElementSibling.hidden=false;" />`
    : "";
  const fallbackHidden = player.headshot_url ? " hidden" : "";
  return `
    <div class="player-avatar" aria-hidden="true">
      ${imageMarkup}
      <span class="player-avatar-fallback"${fallbackHidden}>${initials}</span>
    </div>
  `;
}

function hydrateTrackedCard(item) {
  const detail = item.item || {};
  const player = detail.player || {};
  const state = detail.availability_state || "unavailable";
  const reason = detail.reason_summary || detail.availability_reason || "Status unavailable";
  return `
    <article class="tracked-card">
      <div class="card-header">
        <div class="identity-row">
          ${renderHeadshot(player)}
          <div>
            <h3><a href="/players/${player.player_id}">${player.player_name || "Unknown player"}</a></h3>
            <p class="meta">${player.team_abbr || "NBA"} · ${state}</p>
          </div>
        </div>
        <button class="track-button secondary" type="button" data-track-button data-player-id="${player.player_id}">
          Remove
        </button>
      </div>
      <p>${reason}</p>
      <div class="chip-row">
        ${player.overall_rank ? `<span class="chip">Rank #${player.overall_rank}</span>` : `<span class="chip">Unranked</span>`}
        ${player.recommendation_score ? `<span class="chip">Score ${player.recommendation_score}</span>` : ""}
        <a class="button-link secondary" href="/compare?player_a_id=${player.player_id}">Compare</a>
      </div>
    </article>
  `;
}

async function fetchTrackedPlayer(playerId) {
  const response = await fetch(`/api/players/${playerId}`);
  if (!response.ok) {
    return {
      item: {
        player: {
          player_id: playerId,
          player_name: `Player ${playerId}`,
          team_abbr: null,
          overall_rank: null,
          recommendation_score: null,
        },
        availability_state: "unavailable",
        availability_reason: "Player not found",
        reason_summary: null,
      },
    };
  }
  return response.json();
}

function syncTrackButtons(payload, cap) {
  const trackedIds = new Set(payload.player_ids);
  document.querySelectorAll("[data-track-button]").forEach((button) => {
    const playerId = Number(button.dataset.playerId);
    const isTracked = trackedIds.has(playerId);
    button.disabled = !isTracked && payload.player_ids.length >= cap;
    button.textContent = isTracked
      ? "Untrack player"
      : payload.player_ids.length >= cap
        ? "Tracked max reached"
        : "Track player";
  });
}

async function renderTrackedRail(payload, cap) {
  const rail = document.querySelector("[data-tracked-rail]");
  if (!rail) {
    return;
  }
  const grid = rail.querySelector("[data-tracked-grid]");
  const empty = rail.querySelector("[data-tracked-empty]");
  const summary = rail.querySelector("[data-tracked-summary]");
  const capMessage = rail.querySelector("[data-tracked-cap-message]");
  if (!grid || !empty || !summary || !capMessage) {
    return;
  }

  summary.textContent = formatTrackedSummary(payload.player_ids.length, cap);
  capMessage.hidden = payload.player_ids.length < cap;

  if (payload.player_ids.length === 0) {
    grid.innerHTML = "";
    empty.hidden = false;
    return;
  }

  empty.hidden = true;
  const responses = await Promise.all(payload.player_ids.map(fetchTrackedPlayer));
  grid.innerHTML = responses.map(hydrateTrackedCard).join("");
  syncTrackButtons(payload, cap);
}

async function toggleTrackedPlayer(playerId) {
  const storage = getStorage();
  if (!storage) {
    return;
  }
  const cap = Number(document.body.dataset.trackingCap || TRACKING_CAP);
  const payload = loadTrackedPlayers(storage, cap);
  const isTracked = payload.player_ids.includes(playerId);
  const nextPayload = isTracked
    ? removeTrackedPlayer(payload, playerId)
    : addTrackedPlayer(payload, playerId, cap).payload;
  saveTrackedPlayers(storage, nextPayload);
  syncTrackButtons(nextPayload, cap);
  await renderTrackedRail(nextPayload, cap);
}

function setupTrackButtons() {
  document.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement) || !target.matches("[data-track-button]")) {
      return;
    }
    const playerId = Number(target.dataset.playerId);
    if (!Number.isInteger(playerId)) {
      return;
    }
    event.preventDefault();
    await toggleTrackedPlayer(playerId);
  });
}

function setupCompareSearch() {
  document.querySelectorAll("[data-compare-search-form]").forEach((formNode) => {
    if (!(formNode instanceof HTMLFormElement)) {
      return;
    }
    formNode.addEventListener("submit", async (event) => {
      event.preventDefault();
      const playerAId = Number(formNode.dataset.playerAId);
      const input = formNode.querySelector('input[name="q"]');
      const windowSelect = formNode.querySelector('select[name="window"]');
      const focusSelect = formNode.querySelector('select[name="focus"]');
      const message = formNode.querySelector("[data-compare-search-message]");
      if (
        !Number.isInteger(playerAId) ||
        !(input instanceof HTMLInputElement) ||
        !(windowSelect instanceof HTMLSelectElement) ||
        !(focusSelect instanceof HTMLSelectElement) ||
        !(message instanceof HTMLElement)
      ) {
        return;
      }
      const query = input.value.trim();
      if (!query) {
        return;
      }
      message.textContent = "";
      try {
        const response = await fetch(`/api/players/search?q=${encodeURIComponent(query)}`);
        const data = await response.json();
        if (!response.ok) {
          message.textContent = data.detail || "Search failed";
          return;
        }
        if (!data.items || data.items.length === 0) {
          message.textContent = "No results found";
          return;
        }
        const playerBId = data.items[0].player_id;
        globalThis.location.href = buildCompareHref(
          playerAId,
          playerBId,
          windowSelect.value,
          focusSelect.value
        );
      } catch {
        message.textContent = "Search failed";
      }
    });
  });
}

export function initWorkbench() {
  setupTrackButtons();
  setupCompareSearch();
  const storage = getStorage();
  if (!storage) {
    return;
  }
  const cap = Number(document.body.dataset.trackingCap || TRACKING_CAP);
  const payload = loadTrackedPlayers(storage, cap);
  syncTrackButtons(payload, cap);
  renderTrackedRail(payload, cap);
}

if (typeof document !== "undefined") {
  document.addEventListener("DOMContentLoaded", initWorkbench);
}
