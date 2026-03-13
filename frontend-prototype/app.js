(() => {
  const appVersion = String(window.__DATAFLOW_APP_VERSION__ || "1.0").trim() || "1.0";
  const appVersionBadge = document.getElementById("appVersionBadge");
  if (appVersionBadge) {
    appVersionBadge.textContent = `v${appVersion}`;
  }
  document.title = `数据流查看工具 v${appVersion}`;

  const dropdownMode = "overlay";
  const searchInput = document.getElementById("searchInput");
  const selectedContextMeta = document.getElementById("selectedContextMeta");
  const clearSearch = document.getElementById("clearSearch");
  const historyDropdown = document.getElementById("historyDropdown");
  const resultDropdown = document.getElementById("resultDropdown");
  const searchWrap = document.querySelector(".search-wrap");
  const runFlow = document.getElementById("runFlow");
  const userMenuBtn = document.getElementById("userMenuBtn");
  const userMenuPanel = document.getElementById("userMenuPanel");
  const userMenuWho = document.getElementById("userMenuWho");
  const changePasswordBtn = document.getElementById("changePasswordBtn");
  const openUserAdminBtn = document.getElementById("openUserAdminBtn");
  const logoutBtn = document.getElementById("logoutBtn");
  const loginGate = document.getElementById("loginGate");
  const loginShell = loginGate ? loginGate.querySelector(".login-shell") : null;
  const loginUsername = document.getElementById("loginUsername");
  const loginPassword = document.getElementById("loginPassword");
  const loginErrorText = document.getElementById("loginErrorText");
  const loginSubmitBtn = document.getElementById("loginSubmitBtn");
  const loginCancelBtn = document.getElementById("loginCancelBtn");
  const changePasswordModal = document.getElementById("changePasswordModal");
  const changePasswordModalShell = changePasswordModal ? changePasswordModal.querySelector(".import-shell") : null;
  const maxChangePasswordModal = document.getElementById("maxChangePasswordModal");
  const closeChangePasswordModal = document.getElementById("closeChangePasswordModal");
  const currentPasswordInput = document.getElementById("currentPasswordInput");
  const newPasswordInput = document.getElementById("newPasswordInput");
  const submitChangePasswordBtn = document.getElementById("submitChangePasswordBtn");
  const userAdminModal = document.getElementById("userAdminModal");
  const userAdminModalShell = userAdminModal ? userAdminModal.querySelector(".import-shell") : null;
  const maxUserAdminModal = document.getElementById("maxUserAdminModal");
  const closeUserAdminModal = document.getElementById("closeUserAdminModal");
  const createUserModal = document.getElementById("createUserModal");
  const createUserModalShell = createUserModal ? createUserModal.querySelector(".import-shell") : null;
  const maxCreateUserModal = document.getElementById("maxCreateUserModal");
  const closeCreateUserModal = document.getElementById("closeCreateUserModal");
  const cancelCreateUserBtn = document.getElementById("cancelCreateUserBtn");
  const adminResetUserModal = document.getElementById("adminResetUserModal");
  const adminResetUserModalShell = adminResetUserModal ? adminResetUserModal.querySelector(".import-shell") : null;
  const maxAdminResetUserModal = document.getElementById("maxAdminResetUserModal");
  const closeAdminResetUserModal = document.getElementById("closeAdminResetUserModal");
  const cancelAdminResetUserBtn = document.getElementById("cancelAdminResetUserBtn");
  const submitAdminResetUserBtn = document.getElementById("submitAdminResetUserBtn");
  const adminResetUserNameInput = document.getElementById("adminResetUserNameInput");
  const adminResetUserRoleInput = document.getElementById("adminResetUserRoleInput");
  const adminResetUserPasswordInput = document.getElementById("adminResetUserPasswordInput");
  const userAdminTableBody = document.getElementById("userAdminTableBody");
  const newUserNameInput = document.getElementById("newUserNameInput");
  const newUserPasswordInput = document.getElementById("newUserPasswordInput");
  const newUserRoleSelect = document.getElementById("newUserRoleSelect");
  const createUserBtn = document.getElementById("createUserBtn");
  const submitCreateUserBtn = document.getElementById("submitCreateUserBtn");
  const refreshUsersBtn = document.getElementById("refreshUsersBtn");
  const maxMore = document.getElementById("maxMore");
  const closeMore = document.getElementById("closeMore");
  const morePage = document.getElementById("morePage");
  const moreShell = document.getElementById("moreShell");
  const moreHead = moreShell ? moreShell.querySelector(".more-head") : null;
  const moreResizeHandle = document.getElementById("moreResizeHandle");
  const moreTable = document.getElementById("moreTable");
  const moreColGroup = document.getElementById("moreColGroup");
  const moreTableBody = document.getElementById("moreTableBody");
  const moreSearchInput = document.getElementById("moreSearchInput");
  const moreSearchBtn = document.getElementById("moreSearchBtn");
  const moreFlowBtn = document.getElementById("moreFlowBtn");
  const importRstranCard = document.getElementById("importRstranCard");
  const importBwObjectCard = document.getElementById("importBwObjectCard");
  const importRstranTime = document.getElementById("importRstranTime");
  const importBwObjectTime = document.getElementById("importBwObjectTime");
  const importRstranCount = document.getElementById("importRstranCount");
  const importBwObjectCount = document.getElementById("importBwObjectCount");
  const importModal = document.getElementById("importModal");
  const importModalShell = importModal ? importModal.querySelector(".import-shell") : null;
  const maxImportModal = document.getElementById("maxImportModal");
  const closeImportModal = document.getElementById("closeImportModal");
  const importModalTitle = document.getElementById("importModalTitle");
  const importFileInput = document.getElementById("importFileInput");
  const importSheetSelect = document.getElementById("importSheetSelect");
  const importMeta = document.getElementById("importMeta");
  const importProgressWrap = document.getElementById("importProgressWrap");
  const importProgressText = document.getElementById("importProgressText");
  const importProgressBar = document.getElementById("importProgressBar");
  const importMapBody = document.getElementById("importMapBody");
  const autoMapBtn = document.getElementById("autoMapBtn");
  const clearAllTableDataBtn = document.getElementById("clearAllTableDataBtn");
  const confirmImportBtn = document.getElementById("confirmImportBtn");
  const appToast = document.getElementById("appToast");

  let currentUser = null;
  let authStateEpoch = 0;
  let isAuthBootstrapInFlight = true;
  function normalizeLoopbackApiHost(rawHost) {
    const host = String(rawHost || "").trim();
    if (!host) return host;
    try {
      const url = new URL(host);
      const loopbacks = new Set(["localhost", "127.0.0.1"]);
      const pageHost = String(window.location.hostname || "").trim();
      if (loopbacks.has(url.hostname) && loopbacks.has(pageHost) && url.hostname !== pageHost) {
        url.hostname = pageHost;
      }
      return url.toString().replace(/\/+$/, "");
    } catch {
      return host.replace(/\/+$/, "");
    }
  }

  function resolveApiBase() {
    const fromRuntime = String(window.__DATAFLOW_API_BASE__ || "").trim();
    const fromQuery = String(new URLSearchParams(window.location.search).get("apiBase") || "").trim();
    const pageDefault = `${window.location.protocol}//${window.location.hostname || "localhost"}:8000`;
    if (!fromRuntime && !fromQuery) {
      try {
        window.localStorage.removeItem("df-api-base");
      } catch {
        // Ignore storage failures in restrictive browser contexts.
      }
    }
    const host = fromRuntime || fromQuery || pageDefault;
    return `${normalizeLoopbackApiHost(host)}/api`;
  }

  const importStatusApiBase = resolveApiBase();
  const importStatusApiHost = importStatusApiBase.replace(/\/api\/?$/, "");
  try {
    // Keep flow page and home page on the same API host even if an older cached flow-page.js is loaded.
    window.localStorage.setItem("df-api-base", importStatusApiHost);
  } catch {
    // Ignore storage failures in restrictive browser contexts.
  }
  const HOME_STATE_KEY = "df-home-state-v1";
  const RECENT_SEARCH_KEY = "df-recent-searches-v2";
  const LEGACY_RECENT_SEARCH_KEY = "df-recent-searches-v1";
  const RECENT_SEARCH_MAX = 10;
  let activeImportTable = "";
  let activeExcelHeaders = [];
  let activeImportWorkbook = null;
  let activeImportFileName = "";
  let toastTimer = null;
  let searchRequestToken = 0;
  let recentSearches = [];
  let loginLockCountdownTimer = null;
  let loginLockRemainingSeconds = 0;
  let recentMetaHydrating = false;
  let selectedStartContext = { id: "", source: "", type: "" };
  let selectedAdminResetUsername = "";
  let importProgressTimer = null;
  let importProgressValue = 0;
  let importTaskLock = false;

  async function apiFetch(url, options = {}, skipAuthGuard = false) {
    const opts = options || {};
    const hasExternalSignal = Boolean(opts.signal);
    const timeoutMs = Number.isFinite(Number(opts.timeoutMs)) ? Number(opts.timeoutMs) : 20000;
    const controller = hasExternalSignal ? null : new AbortController();
    const timeoutId = hasExternalSignal
      ? null
      : window.setTimeout(() => {
        controller.abort();
      }, timeoutMs);

    try {
      const response = await fetch(url, {
        credentials: "include",
        ...opts,
        signal: hasExternalSignal ? opts.signal : controller.signal
      });
      if (response.status === 401 && !skipAuthGuard) {
        showLoginGate();
      }
      return response;
    } catch (error) {
      if (error?.name === "AbortError") {
        throw new Error("request_timeout");
      }
      throw error;
    } finally {
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    }
  }

  function parseErrorText(text, fallback) {
    let message = text || fallback;
    try {
      const parsed = JSON.parse(text);
      if (parsed?.detail) {
        message = String(parsed.detail);
      }
    } catch {
      // Keep raw text if response is not JSON.
    }
    return message;
  }

  function isUnauthorizedError(error) {
    const message = String(error?.message || "");
    return /(?:^|\b)status 401\b|unauthorized|未登录|未认证|会话已过期/i.test(message);
  }

  function setSelectedStartContext(id = "", source = "", type = "") {
    selectedStartContext = {
      id: String(id || "").trim(),
      source: String(source || "").trim(),
      type: String(type || "").trim()
    };
    updateSelectedContextDisplay();
  }

  function updateSelectedContextDisplay() {
    if (!selectedContextMeta) return;

    const source = String(selectedStartContext.source || "").trim();
    const type = String(selectedStartContext.type || "").trim();

    if (!source && !type) {
      selectedContextMeta.textContent = "";
      selectedContextMeta.classList.add("hidden");
      return;
    }

    selectedContextMeta.textContent = `源系统: ${source || "--"} | 类型: ${type || "--"}`;
    selectedContextMeta.classList.remove("hidden");
  }

  async function authLogin(username, password) {
    const resp = await apiFetch(`${importStatusApiBase}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    }, true);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function authMe() {
    const resp = await apiFetch(`${importStatusApiBase}/auth/me`, {}, true);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function authLogout() {
    const resp = await apiFetch(`${importStatusApiBase}/auth/logout`, { method: "POST" }, true);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
  }

  async function authChangePassword(currentPassword, newPassword) {
    const resp = await apiFetch(`${importStatusApiBase}/auth/change-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ current_password: currentPassword, new_password: newPassword })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminListUsers() {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users`);
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminCreateUser(username, password, role) {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password, role })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminToggleLock(username, lock) {
    const endpoint = lock ? "lock" : "unlock";
    const resp = await apiFetch(`${importStatusApiBase}/admin/users/${encodeURIComponent(username)}/${endpoint}`, {
      method: "POST"
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminResetPassword(username, newPassword) {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users/${encodeURIComponent(username)}/reset-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_password: newPassword })
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  async function adminDeleteUser(username) {
    const resp = await apiFetch(`${importStatusApiBase}/admin/users/${encodeURIComponent(username)}`, {
      method: "DELETE"
    });
    if (!resp.ok) {
      throw new Error(parseErrorText(await resp.text(), `status ${resp.status}`));
    }
    return resp.json();
  }

  function normalizeRecentSearchItem(value) {
    if (typeof value === "string") {
      const id = value.trim();
      return { id, source: "", type: "" };
    }

    if (value && typeof value === "object") {
      const id = String(value.id || value.value || "").trim();
      const source = String(value.source || value.SOURCESYS || "").trim();
      const type = String(value.type || value.BW_OBJECT_TYPE || "").trim();
      return { id, source, type };
    }

    return { id: "", source: "", type: "" };
  }

  function getRecentSearchIdentity(value) {
    const item = normalizeRecentSearchItem(value);
    const id = String(item.id || "").trim().toLowerCase();
    const source = String(item.source || "").trim().toLowerCase();
    const type = String(item.type || "").trim().toLowerCase();
    return `${id}__${source}__${type}`;
  }

  function sanitizeRecentSearches(items) {
    const merged = [];
    const seen = new Set();

    (items || []).forEach((raw) => {
      const item = normalizeRecentSearchItem(raw);
      const identity = getRecentSearchIdentity(item);
      if (!item.id || seen.has(identity)) return;
      merged.push(item);
      seen.add(identity);
    });

    return merged.slice(0, RECENT_SEARCH_MAX);
  }

  const importSchemas = {
    rstran: [
      "TRANID", "OWNER", "TSTPNM", "SOURCETYPE", "SOURCESUBTYPE", "SOURCENAME", "TARGETTYPE", "TARGETSUBTYPE",
      "TARGETNAME", "STARTROUTINE", "ENDROUTINE", "EXPERT", "GLBCODE", "TRANPROG", "GLBCODE2", "SOURCESYS", "DATASOURCE"
    ],
    bw_object_name: ["BW_OBJECT", "SOURCESYS", "BW_OBJECT_TYPE", "OBJECT_NAME"]
  };

  const logicManagedFields = {
    rstran: {
      SOURCESYS: "__LOGIC_SOURCENAME_SPLIT_LAST__",
      DATASOURCE: "__LOGIC_SOURCENAME_SPLIT_FIRST__"
    }
  };

  const logicRuleDesc = {
    rstran: "逻辑规则: SOURCENAME按空格拆分，前段->DATASOURCE，后段->SOURCESYS"
  };
  function esc(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function escAttr(str) {
    return esc(str)
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function markKeyword(text, keyword) {
    const t = String(text);
    if (!keyword) return esc(t);
    const idx = t.toLowerCase().indexOf(keyword.toLowerCase());
    if (idx < 0) return esc(t);
    const a = esc(t.slice(0, idx));
    const b = esc(t.slice(idx, idx + keyword.length));
    const c = esc(t.slice(idx + keyword.length));
    return `${a}<span class="mark">${b}</span>${c}`;
  }

  function showToast(message, variant = "success") {
    if (!appToast) return;
    if (toastTimer) {
      clearTimeout(toastTimer);
      toastTimer = null;
    }

    appToast.classList.remove("error");
    if (variant === "error") {
      appToast.classList.add("error");
    }
    appToast.textContent = message;
    appToast.classList.remove("hidden");

    const holdMs = variant === "error" ? 4200 : 2800;
    toastTimer = window.setTimeout(() => {
      appToast.classList.add("hidden");
      toastTimer = null;
    }, holdMs);
  }

  function clearImportProgressTimer() {
    if (importProgressTimer) {
      window.clearInterval(importProgressTimer);
      importProgressTimer = null;
    }
  }

  function resetImportProgress() {
    clearImportProgressTimer();
    importProgressValue = 0;
    if (importProgressBar) {
      importProgressBar.style.width = "0%";
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", "0");
    }
    if (importProgressText) importProgressText.textContent = "处理中...";
    if (importProgressWrap) importProgressWrap.classList.add("hidden");
  }

  function setImportBusyState(isBusy, label = "处理中...") {
    if (importModalShell) {
      importModalShell.classList.toggle("is-busy", isBusy);
    }
    [importFileInput, importSheetSelect, autoMapBtn, clearAllTableDataBtn, confirmImportBtn].forEach((el) => {
      if (!el) return;
      el.disabled = isBusy;
    });

    if (!isBusy) {
      resetImportProgress();
      return;
    }

    if (importProgressWrap) importProgressWrap.classList.remove("hidden");
    if (importProgressText) importProgressText.textContent = label;

    clearImportProgressTimer();
    importProgressValue = 8;
    if (importProgressBar) {
      importProgressBar.style.width = `${importProgressValue}%`;
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", String(Math.round(importProgressValue)));
    }

    importProgressTimer = window.setInterval(() => {
      if (!importProgressBar) return;
      const inc = 2 + Math.random() * 6;
      importProgressValue = Math.min(92, importProgressValue + inc);
      importProgressBar.style.width = `${importProgressValue}%`;
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", String(Math.round(importProgressValue)));
    }, 220);
  }

  function completeImportBusyState() {
    clearImportProgressTimer();
    if (importProgressBar) {
      importProgressValue = 100;
      importProgressBar.style.width = "100%";
      const track = importProgressBar.parentElement;
      if (track) track.setAttribute("aria-valuenow", "100");
    }

    window.setTimeout(() => {
      setImportBusyState(false);
    }, 260);
  }

  function buildDropdownWithMore(rowsHtml, moreId) {
    return `<div class="dropdown-item dropdown-more" id="${moreId}">搜索更多--&gt;</div><div class="dropdown-list">${rowsHtml}</div>`;
  }

  function buildDropdownRows(items, keyword = "") {
    return items.map((raw) => {
      const item = normalizeRecentSearchItem(raw);
      const source = String(item.source || "").trim();
      const type = String(item.type || "").trim();
      const meta = `${source || "--"} | ${type || "--"}`;
      const idText = keyword ? markKeyword(item.id, keyword) : esc(item.id);
      return `<div class="dropdown-item" data-value="${esc(item.id)}" data-source="${esc(source)}" data-type="${esc(type)}"><span>${idText}</span><small>${esc(meta)}</small></div>`;
    });
  }

  function renderHistory() {
    const list = sanitizeRecentSearches(recentSearches)
      .map((item) => normalizeRecentSearchItem(item))
      .filter((item) => Boolean(item.id));
    const rows = buildDropdownRows(list).join("");
    const emptyTip = rows ? "" : '<div class="dropdown-item">暂无历史</div>';
    historyDropdown.innerHTML = buildDropdownWithMore(`${rows}${emptyTip}`, "moreLinkHistory");
    historyDropdown.classList.remove("hidden");
    resultDropdown.classList.add("hidden");

    if (currentUser) {
      const hasMissingMeta = list.some((item) => !item.source || !item.type);
      if (hasMissingMeta && !recentMetaHydrating) {
        void hydrateRecentSearchMeta();
      }
    }
  }

  async function resolveRecentMetaById(id) {
    const keyword = String(id || "").trim();
    if (!keyword || keyword.length < 3) return { source: "", type: "" };
    try {
      const rows = await fetchTopSearchRows(keyword);
      const exact = rows.find((row) => String(row?.id || "").trim().toLowerCase() === keyword.toLowerCase());
      if (!exact) return { source: "", type: "" };
      return {
        source: String(exact.source || "").trim(),
        type: String(exact.type || "").trim()
      };
    } catch {
      return { source: "", type: "" };
    }
  }

  async function hydrateRecentSearchMeta() {
    if (recentMetaHydrating) return;
    recentMetaHydrating = true;
    try {
      let changed = false;
      for (let i = 0; i < recentSearches.length; i += 1) {
        const item = normalizeRecentSearchItem(recentSearches[i]);
        if (!item.id) continue;
        if (item.source && item.type) continue;
        const meta = await resolveRecentMetaById(item.id);
        if (!meta.source && !meta.type) continue;
        recentSearches[i] = {
          ...item,
          source: item.source || meta.source,
          type: item.type || meta.type
        };
        changed = true;
      }

      if (changed) {
        recentSearches = sanitizeRecentSearches(recentSearches);
        saveRecentSearches();
        if (!historyDropdown.classList.contains("hidden")) {
          renderHistory();
        }
      }
    } finally {
      recentMetaHydrating = false;
    }
  }

  async function fetchTopSearchRows(keyword) {
    const key = (keyword || "").trim();
    if (key.length < 3) return [];
    const resp = await apiFetch(`${importStatusApiBase}/search/bw-object-name?keyword=${encodeURIComponent(key)}`);
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    const payload = await resp.json();
    return Array.isArray(payload?.items) ? payload.items : [];
  }

  async function renderSearchResult(keyword) {
    const key = keyword.trim();
    const token = ++searchRequestToken;

    if (key.length < 3) {
      renderHistory();
      return;
    }

    // Show dropdown immediately to avoid blank state while async search is in-flight.
    resultDropdown.innerHTML = buildDropdownWithMore('<div class="dropdown-item">加载中...</div>', "moreLink");
    resultDropdown.classList.remove("hidden");
    historyDropdown.classList.add("hidden");

    try {
      const filtered = await fetchTopSearchRows(key);
      if (token !== searchRequestToken) return;

      const rows = buildDropdownRows(filtered, key);

      if (!rows.length) {
        rows.push('<div class="dropdown-item">未找到匹配数据</div>');
      }

      resultDropdown.innerHTML = buildDropdownWithMore(rows.join(""), "moreLink");
      resultDropdown.classList.remove("hidden");
      historyDropdown.classList.add("hidden");
    } catch (error) {
      if (token !== searchRequestToken) return;
      if (isUnauthorizedError(error)) {
        resultDropdown.innerHTML = buildDropdownWithMore('<div class="dropdown-item">请先登录后再搜索</div>', "moreLink");
      } else {
        resultDropdown.innerHTML = buildDropdownWithMore('<div class="dropdown-item">加载失败，请确认后端服务已启动</div>', "moreLink");
      }
      resultDropdown.classList.remove("hidden");
      historyDropdown.classList.add("hidden");
    }
  }

  async function resolveStartContextForFlow(startName) {
    const normalizedStart = String(startName || "").trim();
    const currentCtx = normalizeRecentSearchItem(selectedStartContext);
    if (currentCtx.id && currentCtx.id.toLowerCase() === normalizedStart.toLowerCase() && (currentCtx.source || currentCtx.type)) {
      return { id: normalizedStart, source: currentCtx.source, type: currentCtx.type };
    }

    const rows = await fetchMoreRows(normalizedStart, true);
    const exact = rows.filter((row) => String(row?.id || "").trim().toLowerCase() === normalizedStart.toLowerCase());
    if (!exact.length) {
      return { id: normalizedStart, source: "", type: "" };
    }

    const mapped = exact.map((row) => ({
      id: normalizedStart,
      source: String(row?.source || "").trim(),
      type: String(row?.type || "").trim()
    }));

    const uniq = new Map();
    mapped.forEach((item) => {
      const key = `${item.source}__${item.type}`;
      if (!uniq.has(key)) uniq.set(key, item);
    });
    const uniqueItems = [...uniq.values()];

    if (uniqueItems.length === 1) {
      return uniqueItems[0];
    }

    const fromCtx = uniqueItems.find((item) => item.source === currentCtx.source && item.type === currentCtx.type);
    if (fromCtx) {
      return fromCtx;
    }

    const err = new Error("ambiguous_start");
    err.code = "ambiguous_start";
    throw err;
  }

  async function openFlowResult(startValue) {
    const resolvedStart = (startValue || searchInput.value || "").trim();
    if (!resolvedStart) {
      showToast("请先选择对象后再生成数据流图。", "error");
      searchInput.focus();
      renderHistory();
      return;
    }

    let startContext;
    try {
      startContext = await resolveStartContextForFlow(resolvedStart);
    } catch (err) {
      if (err?.code === "ambiguous_start") {
        showToast("同技术名存在多个源系统/类型，请从下拉中选择具体项后再生成。", "error");
        void renderSearchResult(resolvedStart);
        searchInput.focus();
        return;
      }
      showToast(`解析基础模型失败：${err?.message || "未知错误"}`, "error");
      return;
    }

    if (resolvedStart) {
      pushRecentSearch(startContext);
      setSelectedStartContext(startContext.id, startContext.source, startContext.type);
    }
    saveHomeState();
    historyDropdown.classList.add("hidden");
    resultDropdown.classList.add("hidden");
    searchInput.blur();
    const selectedMode = getSelectedMode();
    const flowMode = selectedMode;

    const params = new URLSearchParams({
      start: resolvedStart,
      mode: flowMode,
      types: getSelectedTypes().join(","),
      apiBase: importStatusApiHost
    });
    if (startContext.source) params.set("startSource", startContext.source);
    if (startContext.type) params.set("startType", startContext.type);
    const flowUrl = `./flow.html?${params.toString()}`;
    const opened = window.open(flowUrl, "_blank");
    // Keep homepage in current tab; if popup is blocked, ask user to allow popups.
    if (!opened) {
      showToast("浏览器拦截了新页签，请允许弹出窗口后重试。", "error");
    }
  }

  function setupSearchBehavior() {
    if (searchWrap) {
      const reopenDropdown = () => {
        const key = searchInput.value.trim();
        if (key.length >= 3) {
          void renderSearchResult(key);
        } else {
          renderHistory();
        }
      };
      searchWrap.addEventListener("pointerdown", reopenDropdown);
      searchWrap.addEventListener("click", reopenDropdown);
    }

    searchInput.addEventListener("focus", () => {
      // Always show at least the history list (includes "搜索更多") immediately.
      renderHistory();
      const key = searchInput.value.trim();
      if (key.length >= 3) {
        void renderSearchResult(key);
      }
    });

    searchInput.addEventListener("input", () => {
      const currentId = String(searchInput.value || "").trim().toLowerCase();
      if (!selectedStartContext.id || selectedStartContext.id.toLowerCase() !== currentId) {
        setSelectedStartContext();
      }
      clearSearch.style.display = searchInput.value ? "inline-flex" : "none";
      if (searchInput.value.trim().length >= 3) {
        void renderSearchResult(searchInput.value);
      } else {
        renderHistory();
      }
    });

    clearSearch.addEventListener("click", () => {
      // Invalidate any in-flight async search so stale results cannot overwrite history.
      searchRequestToken += 1;
      setSelectedStartContext();
      searchInput.value = "";
      clearSearch.style.display = "none";
      resultDropdown.classList.add("hidden");
      renderHistory();
      searchInput.focus();
    });

    historyDropdown.addEventListener("click", (event) => {
      const more = event.target.closest("#moreLinkHistory");
      if (more) {
        openMorePage();
        return;
      }

      const item = event.target.closest(".dropdown-item");
      if (!item) return;
      historyDropdown.querySelectorAll(".dropdown-item").forEach((el) => el.classList.remove("selected-outline"));
      item.classList.add("selected-outline");
      searchInput.value = item.dataset.value || "";
      setSelectedStartContext(item.dataset.value || "", item.dataset.source || "", item.dataset.type || "");
      historyDropdown.classList.add("hidden");
      clearSearch.style.display = "inline-flex";
    });

    resultDropdown.addEventListener("click", (event) => {
      const more = event.target.closest("#moreLink");
      if (more) {
        openMorePage();
        return;
      }
      const item = event.target.closest(".dropdown-item");
      if (!item || !item.dataset.value) return;
      resultDropdown.querySelectorAll(".dropdown-item").forEach((el) => el.classList.remove("selected-outline"));
      item.classList.add("selected-outline");
      searchInput.value = item.dataset.value;
      setSelectedStartContext(item.dataset.value || "", item.dataset.source || "", item.dataset.type || "");
      pushRecentSearch({
        id: item.dataset.value,
        source: item.dataset.source || "",
        type: item.dataset.type || ""
      });
      resultDropdown.classList.add("hidden");
      clearSearch.style.display = "inline-flex";
    });

    document.addEventListener("pointerdown", (event) => {
      const inside = event.target.closest(".search-wrap") || event.target.closest(".dropdown");
      if (!inside) {
        historyDropdown.classList.add("hidden");
        resultDropdown.classList.add("hidden");
        searchInput.blur();
      }
    });
  }

  function getSelectedTypes() {
    return [...document.querySelectorAll("#typeToggles input:checked")].map((node) => node.value);
  }

  function getSelectedMode() {
    const checked = document.querySelector("#modeGroup input:checked");
    return checked ? checked.value : "both";
  }

  function setSelectedMode(mode) {
    const value = String(mode || "").trim();
    if (!value) return;
    const target = document.querySelector(`#modeGroup input[value="${value}"]`);
    if (target) {
      target.checked = true;
    }
  }

  function loadRecentSearches() {
    // One-time cleanup: drop old v1 data that may contain seeded mock records.
    localStorage.removeItem(LEGACY_RECENT_SEARCH_KEY);
    const raw = localStorage.getItem(RECENT_SEARCH_KEY);
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          recentSearches = sanitizeRecentSearches(parsed)
            .map((item) => normalizeRecentSearchItem(item))
            .filter((item) => Boolean(item.id))
            .slice(0, RECENT_SEARCH_MAX);
          return;
        }
      } catch {
        // Ignore invalid cache and reset to empty history.
      }
    }
    recentSearches = [];
  }

  function saveRecentSearches() {
    localStorage.setItem(RECENT_SEARCH_KEY, JSON.stringify(recentSearches));
  }

  function pushRecentSearch(value) {
    const normalized = normalizeRecentSearchItem(value);
    if (!normalized.id) return;
    const identity = getRecentSearchIdentity(normalized);
    recentSearches = sanitizeRecentSearches([normalized]
      .concat(recentSearches.filter((item) => getRecentSearchIdentity(item) !== identity))
    ).slice(0, RECENT_SEARCH_MAX);
    saveRecentSearches();
    if (currentUser && (!normalized.source || !normalized.type)) {
      void hydrateRecentSearchMeta();
    }
  }

  function saveHomeState() {
    const payload = {
      mode: getSelectedMode()
    };
    localStorage.setItem(HOME_STATE_KEY, JSON.stringify(payload));
  }

  function restoreHomeState() {
    const qs = new URLSearchParams(window.location.search);
    const modeFromUrl = (qs.get("mode") || "").trim();

    let state = {};
    const raw = localStorage.getItem(HOME_STATE_KEY);
    if (raw) {
      try {
        state = JSON.parse(raw) || {};
      } catch {
        state = {};
      }
    }

    const resolvedMode = modeFromUrl || String(state.mode || "").trim();
    const hadTransientParams = qs.has("start") || qs.has("startSource") || qs.has("startType") || qs.has("l1");

    searchInput.value = "";
    setSelectedStartContext();
    clearSearch.style.display = "none";
    historyDropdown.classList.add("hidden");
    resultDropdown.classList.add("hidden");

    if (resolvedMode) {
      setSelectedMode(resolvedMode);
    }

    // Keep home URL clean so refresh does not re-introduce transient flow params.
    if (hadTransientParams) {
      const next = new URLSearchParams();
      if (resolvedMode) {
        next.set("mode", resolvedMode);
      }
      const nextUrl = `${window.location.pathname}${next.toString() ? `?${next.toString()}` : ""}`;
      window.history.replaceState({}, "", nextUrl);
    }
  }

  function openMorePage() {
    moreSearchInput.value = "";
    moreTableBody.innerHTML = '<tr><td colspan="4">请输入至少3个字符并点击“查找”</td></tr>';
    morePage.classList.remove("hidden");
  }

  async function fetchMoreRows(keyword, searchMode) {
    const kw = (keyword || "").trim();
    const query = searchMode ? kw : "";
    const resp = await apiFetch(`${importStatusApiBase}/search-more/bw-object-name?keyword=${encodeURIComponent(query)}`);
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    const payload = await resp.json();
    return Array.isArray(payload?.items) ? payload.items : [];
  }

  async function renderMoreTable(keyword, searchMode = false) {
    const key = (keyword || "").trim();
    moreTableBody.innerHTML = '<tr><td colspan="4">加载中...</td></tr>';

    try {
      const rows = await fetchMoreRows(key, searchMode);
      if (!rows.length) {
        moreTableBody.innerHTML = '<tr><td colspan="4">未找到匹配数据</td></tr>';
        return;
      }

      moreTableBody.innerHTML = rows
      .map((row, index) => {
        return `<tr data-id="${esc(row.id)}" data-source="${esc(row.source || "")}" data-type="${esc(row.type || "")}" data-index="${index}"><td>${markKeyword(row.type || "-", key)}</td><td>${markKeyword(row.id || "-", key)}</td><td>${markKeyword(row.source || "-", key)}</td><td>${markKeyword(row.desc || "-", key)}</td></tr>`;
      })
      .join("");
    } catch (error) {
      if (String(error?.message || "") === "request_timeout") {
        moreTableBody.innerHTML = '<tr><td colspan="4">请求超时，请稍后重试或检查后端状态</td></tr>';
        return;
      }
      if (isUnauthorizedError(error)) {
        moreTableBody.innerHTML = '<tr><td colspan="4">请先登录后再查找</td></tr>';
        return;
      }
      moreTableBody.innerHTML = '<tr><td colspan="4">加载失败，请确认后端服务已启动</td></tr>';
    }
  }

  function setupMorePage() {
    const confirmMoreSelection = () => {
      const selected = moreTableBody.querySelector("tr.active");
      if (!selected) {
        window.alert("请选择一行然后点确定，或者直接双击选中行。");
        return;
      }

      const selectedTechName = selected.dataset.id || "";
      const selectedSource = selected.dataset.source || "";
      const selectedType = selected.dataset.type || "";
      searchInput.value = selectedTechName;
      setSelectedStartContext(selectedTechName, selectedSource, selectedType);
      pushRecentSearch({ id: selectedTechName, source: selectedSource, type: selectedType });
      clearSearch.style.display = selectedTechName ? "inline-flex" : "none";
      historyDropdown.classList.add("hidden");
      resultDropdown.classList.add("hidden");
      morePage.classList.add("hidden");
    };

    closeMore.addEventListener("click", () => morePage.classList.add("hidden"));
    maxMore.addEventListener("click", () => {
      const isMaximized = moreShell.classList.contains("maximized");
      if (!isMaximized) {
        if (moreShell.style.width) {
          moreShell.dataset.restoreWidth = moreShell.style.width;
        }
        if (moreShell.style.height) {
          moreShell.dataset.restoreHeight = moreShell.style.height;
        }
        moreShell.classList.add("maximized");
        moreShell.style.setProperty("--more-shell-dx", "0px");
        moreShell.style.setProperty("--more-shell-dy", "0px");
        moreShell.style.width = "";
        moreShell.style.height = "";
        maxMore.setAttribute("title", "还原");
      } else {
        moreShell.classList.remove("maximized");
        if (moreShell.dataset.restoreWidth) {
          moreShell.style.width = moreShell.dataset.restoreWidth;
        }
        if (moreShell.dataset.restoreHeight) {
          moreShell.style.height = moreShell.dataset.restoreHeight;
        }
        maxMore.setAttribute("title", "最大化");
      }
    });
    moreSearchBtn.addEventListener("click", () => {
      const key = moreSearchInput.value.trim();
      if (key.length < 3) {
        window.alert("请输入至少3个字符后再查找。默认仅展示前100条。");
        return;
      }
      renderMoreTable(key, true);
    });
    moreSearchInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        const key = moreSearchInput.value.trim();
        if (key.length < 3) {
          window.alert("请输入至少3个字符后再查找。默认仅展示前100条。");
          return;
        }
        renderMoreTable(key, true);
      }
    });
    moreTableBody.addEventListener("click", (event) => {
      const row = event.target.closest("tr");
      if (!row) return;
      moreTableBody.querySelectorAll("tr").forEach((r) => r.classList.remove("active"));
      row.classList.add("active");
    });
    moreTableBody.addEventListener("dblclick", (event) => {
      const row = event.target.closest("tr");
      if (!row) return;
      moreTableBody.querySelectorAll("tr").forEach((r) => r.classList.remove("active"));
      row.classList.add("active");
      confirmMoreSelection();
    });
    moreFlowBtn.addEventListener("click", confirmMoreSelection);
    morePage.addEventListener("click", (event) => {
      if (event.target === morePage) {
        morePage.classList.add("hidden");
      }
    });
  }

  function setupMoreDialogDrag() {
    if (!moreHead || !moreShell) return;

    let dragStartX = 0;
    let dragStartY = 0;
    let startDx = 0;
    let startDy = 0;

    const getOffset = (name) => {
      const raw = moreShell.style.getPropertyValue(name).trim();
      const parsed = Number.parseFloat(raw || "0");
      return Number.isFinite(parsed) ? parsed : 0;
    };

    const clampOffset = (dx, dy) => {
      const rect = moreShell.getBoundingClientRect();
      const maxX = Math.max(0, (window.innerWidth - rect.width) / 2);
      const maxY = Math.max(0, (window.innerHeight - rect.height) / 2);
      return {
        dx: Math.max(-maxX, Math.min(maxX, dx)),
        dy: Math.max(-maxY, Math.min(maxY, dy))
      };
    };

    const onMove = (event) => {
      const nextDx = startDx + (event.clientX - dragStartX);
      const nextDy = startDy + (event.clientY - dragStartY);
      const clamped = clampOffset(nextDx, nextDy);
      moreShell.style.setProperty("--more-shell-dx", `${clamped.dx}px`);
      moreShell.style.setProperty("--more-shell-dy", `${clamped.dy}px`);
    };

    const onUp = () => {
      moreShell.classList.remove("dragging");
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };

    moreHead.addEventListener("pointerdown", (event) => {
      const blockDrag = event.target.closest("button, input, .window-controls");
      if (blockDrag || moreShell.classList.contains("maximized")) return;

      dragStartX = event.clientX;
      dragStartY = event.clientY;
      startDx = getOffset("--more-shell-dx");
      startDy = getOffset("--more-shell-dy");

      moreShell.classList.add("dragging");
      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });
  }

  function setupMoreTableResize() {
    if (!moreTable || !moreColGroup) return;

    const cols = [...moreColGroup.querySelectorAll("col")];
    const minPx = [90, 140, 120, 180];

    function applyWidth(index, value) {
      cols[index].style.width = `${value}px`;
    }

    function getColWidth(index) {
      return cols[index].getBoundingClientRect().width;
    }

    moreTable.querySelectorAll(".col-resizer").forEach((handle) => {
      handle.addEventListener("pointerdown", (event) => {
        event.preventDefault();

        const index = Number(handle.dataset.col);
        const nextIndex = index + 1;
        if (!Number.isFinite(index) || !cols[nextIndex]) return;

        const startX = event.clientX;
        const startW = getColWidth(index);
        const nextStartW = getColWidth(nextIndex);

        const onMove = (moveEvent) => {
          const delta = moveEvent.clientX - startX;
          const nextMin = minPx[nextIndex] || 100;
          let newW = startW + delta;
          let nextW = nextStartW - delta;

          if (newW < minPx[index]) {
            newW = minPx[index];
            nextW = startW + nextStartW - newW;
          }

          if (nextW < nextMin) {
            nextW = nextMin;
            newW = startW + nextStartW - nextW;
          }

          applyWidth(index, newW);
          applyWidth(nextIndex, nextW);
        };

        const onUp = () => {
          window.removeEventListener("pointermove", onMove);
          window.removeEventListener("pointerup", onUp);
        };

        window.addEventListener("pointermove", onMove);
        window.addEventListener("pointerup", onUp);
      });
    });
  }

  function setupMoreDialogResize() {
    if (!moreResizeHandle || !moreShell) return;

    const minWidth = 520;
    const minHeight = 280;

    let startX = 0;
    let startY = 0;
    let startWidth = 0;
    let startHeight = 0;

    const onMove = (event) => {
      const maxWidth = window.innerWidth - 32;
      const maxHeight = window.innerHeight - 32;
      const width = Math.max(minWidth, Math.min(maxWidth, startWidth + (event.clientX - startX)));
      const height = Math.max(minHeight, Math.min(maxHeight, startHeight + (event.clientY - startY)));

      moreShell.style.width = `${width}px`;
      moreShell.style.height = `${height}px`;
      moreShell.dataset.restoreWidth = moreShell.style.width;
      moreShell.dataset.restoreHeight = moreShell.style.height;
    };

    const onUp = () => {
      moreShell.classList.remove("resizing");
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };

    moreResizeHandle.addEventListener("pointerdown", (event) => {
      if (moreShell.classList.contains("maximized")) return;

      event.preventDefault();
      const rect = moreShell.getBoundingClientRect();
      startX = event.clientX;
      startY = event.clientY;
      startWidth = rect.width;
      startHeight = rect.height;

      moreShell.classList.add("resizing");
      window.addEventListener("pointermove", onMove);
      window.addEventListener("pointerup", onUp);
    });
  }

  function showLoginGate() {
    if (!loginGate) return;
    loginGate.classList.remove("hidden");
    if (loginErrorText) {
      loginErrorText.textContent = "";
      loginErrorText.classList.add("hidden");
    }
    if (userMenuBtn) userMenuBtn.classList.add("hidden");
    if (userMenuPanel) userMenuPanel.classList.add("hidden");
  }

  function hideLoginGate() {
    if (!loginGate) return;
    loginGate.classList.add("hidden");
    if (userMenuBtn) userMenuBtn.classList.remove("hidden");
  }

  function refreshUserMenu() {
    if (!currentUser) {
      if (isAuthBootstrapInFlight) {
        if (userMenuBtn) userMenuBtn.classList.add("hidden");
        if (userMenuWho) userMenuWho.textContent = "--";
        if (userMenuPanel) userMenuPanel.classList.add("hidden");
        hideLoginGate();
        return;
      }
      if (userMenuBtn) {
        userMenuBtn.textContent = "登录";
        userMenuBtn.classList.remove("hidden");
      }
      if (userMenuWho) userMenuWho.textContent = "--";
      if (userMenuPanel) userMenuPanel.classList.add("hidden");
      showLoginGate();
      return;
    }
    if (userMenuBtn) userMenuBtn.textContent = `${currentUser.username}`;
    if (userMenuWho) userMenuWho.textContent = `${currentUser.username} (${currentUser.role})`;
    if (openUserAdminBtn) {
      openUserAdminBtn.classList.toggle("hidden", currentUser.role !== "admin");
    }
    hideLoginGate();
  }

  function renderUserRows(rows) {
    if (!userAdminTableBody) return;
    userAdminTableBody.innerHTML = rows.map((row) => {
      const isLocked = Boolean(row.is_locked);
      const hasTempLock = Boolean(row.temp_lock_until);
      const isLockAction = !isLocked && !hasTempLock;
      const lockActionLabel = isLockAction ? "锁定用户" : "解锁用户";
      const isCurrentLoginUser = String(row.username || "").trim().toLowerCase() === String(currentUser?.username || "").trim().toLowerCase();
      const disableToggleLock = isLockAction && isCurrentLoginUser;
      const status = isLocked
        ? "锁定"
        : (hasTempLock ? `临时限制至 ${String(row.temp_lock_until).replace("T", " ").slice(0, 19)}` : "正常");
      const lastLogin = row.last_login_at ? row.last_login_at.replace("T", " ").slice(0, 19) : "--";
      return `
        <tr data-username="${esc(row.username)}" data-role="${esc(row.role)}" class="${isLocked ? "" : "mapped-row"}">
          <td>${esc(row.username)}</td>
          <td>${esc(row.role)}</td>
          <td>${esc(status)}</td>
          <td>${esc(lastLogin)}</td>
          <td>
            <div class="user-action-buttons">
              <button type="button" class="glass-btn tiny danger-btn js-reset-user-password">修改密码</button>
              <button type="button" class="glass-btn tiny danger-btn${isLockAction ? " primary-lock-btn" : ""} js-toggle-lock-user${disableToggleLock ? " is-disabled" : ""}" data-lock-action="${isLockAction ? "lock" : "unlock"}" ${disableToggleLock ? "disabled" : ""}>${lockActionLabel}</button>
              <button type="button" class="glass-btn tiny danger-btn js-delete-user">删除用户</button>
            </div>
          </td>
        </tr>
      `;
    }).join("");
  }

  async function refreshUserAdminTable() {
    const payload = await adminListUsers();
    renderUserRows(Array.isArray(payload?.users) ? payload.users : []);
  }

  function setupAuthUi() {
    const clearLoginCountdown = () => {
      if (loginLockCountdownTimer) {
        window.clearInterval(loginLockCountdownTimer);
        loginLockCountdownTimer = null;
      }
      loginLockRemainingSeconds = 0;
    };

    const formatRemain = (seconds) => {
      const s = Math.max(0, Math.floor(seconds));
      const mm = String(Math.floor(s / 60)).padStart(2, "0");
      const ss = String(s % 60).padStart(2, "0");
      return `${mm}:${ss}`;
    };

    const startLoginCountdown = (seconds) => {
      clearLoginCountdown();
      loginLockRemainingSeconds = Math.max(1, Number(seconds) || 0);

      const tick = () => {
        if (!loginErrorText) {
          clearLoginCountdown();
          return;
        }
        loginErrorText.textContent = `登录失败次数过多，请 ${formatRemain(loginLockRemainingSeconds)} 后再试`;
        if (loginLockRemainingSeconds <= 0) {
          clearLoginCountdown();
          loginShell?.classList.remove("has-error");
          loginErrorText.classList.add("hidden");
          loginErrorText.textContent = "";
          return;
        }
        loginLockRemainingSeconds -= 1;
      };

      tick();
      loginLockCountdownTimer = window.setInterval(tick, 1000);
    };

    const setLoginError = (message) => {
      if (!loginErrorText) return;
      const text = String(message || "").trim();
      if (!text) {
        clearLoginCountdown();
        loginErrorText.textContent = "";
        loginErrorText.classList.add("hidden");
        loginShell?.classList.remove("has-error");
        return;
      }

      const lockMatch = text.match(/请\s*(\d+)\s*分钟后再试/);
      if (lockMatch) {
        const mins = Number(lockMatch[1]);
        const seconds = Number.isFinite(mins) ? mins * 60 : 0;
        loginErrorText.classList.remove("hidden");
        loginShell?.classList.add("has-error");
        startLoginCountdown(seconds);
        return;
      }

      clearLoginCountdown();
      loginErrorText.textContent = text;
      loginErrorText.classList.remove("hidden");
      loginShell?.classList.add("has-error");
    };

    const submitLogin = async () => {
      const username = String(loginUsername?.value || "").trim();
      const password = String(loginPassword?.value || "");
      if (!username || !password) {
        setLoginError("请输入用户名和密码");
        return;
      }
      try {
        setLoginError("");
        // Invalidate any older bootstrapAuth result still in flight.
        authStateEpoch += 1;
        await authLogin(username, password);
        currentUser = await authMe();
        if (loginPassword) loginPassword.value = "";
        if (searchInput) searchInput.value = "";
        setSelectedStartContext();
        if (clearSearch) clearSearch.style.display = "none";
        historyDropdown.classList.add("hidden");
        resultDropdown.classList.add("hidden");
        refreshUserMenu();
        showToast("登录成功");
        saveHomeState();
        await refreshImportCardTimes();
      } catch (err) {
        const msg = String(err?.message || "未知错误");
        if (/failed to fetch/i.test(msg)) {
          setLoginError(`登录失败：无法连接后端服务（${importStatusApiBase}）。请确认后端已启动且 CORS 白名单包含当前前端地址。`);
        } else if (/401|unauthorized|未登录|未认证/i.test(msg)) {
          setLoginError("登录失败：会话未建立，请确认前端与后端同域策略/CORS 与 Cookie 设置后重试。");
        } else {
          setLoginError(`登录失败：${msg}`);
        }
      }
    };

    if (loginSubmitBtn) {
      loginSubmitBtn.addEventListener("click", submitLogin);
    }

    [loginUsername, loginPassword].forEach((input) => {
      if (!input) return;
      input.addEventListener("keydown", (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        void submitLogin();
      });
    });

    if (loginCancelBtn) {
      loginCancelBtn.addEventListener("click", () => {
        if (loginUsername) loginUsername.value = "";
        if (loginPassword) loginPassword.value = "";
        setLoginError("");
        showLoginGate();
      });
    }

    if (userMenuBtn) {
      userMenuBtn.addEventListener("click", () => {
        if (!currentUser) {
          showLoginGate();
          return;
        }
        if (userMenuPanel) userMenuPanel.classList.toggle("hidden");
      });
    }

    document.addEventListener("click", (event) => {
      if (!userMenuPanel || !userMenuBtn) return;
      if (userMenuPanel.contains(event.target) || userMenuBtn.contains(event.target)) return;
      userMenuPanel.classList.add("hidden");
    });

    if (logoutBtn) {
      logoutBtn.addEventListener("click", async () => {
        try {
          await authLogout();
        } catch {
          // Ignore logout call failure and still clear local state.
        }
        currentUser = null;
        refreshUserMenu();
      });
    }

    if (changePasswordBtn) {
      changePasswordBtn.addEventListener("click", () => {
        if (changePasswordModal) changePasswordModal.classList.remove("hidden");
        if (currentPasswordInput) currentPasswordInput.value = "";
        if (newPasswordInput) newPasswordInput.value = "";
      });
    }

    if (closeChangePasswordModal) {
      closeChangePasswordModal.addEventListener("click", () => changePasswordModal?.classList.add("hidden"));
    }

    if (maxChangePasswordModal && changePasswordModalShell) {
      maxChangePasswordModal.addEventListener("click", () => {
        const isMaximized = changePasswordModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (changePasswordModalShell.style.width) {
            changePasswordModalShell.dataset.restoreWidth = changePasswordModalShell.style.width;
          }
          if (changePasswordModalShell.style.height) {
            changePasswordModalShell.dataset.restoreHeight = changePasswordModalShell.style.height;
          }
          changePasswordModalShell.classList.add("maximized");
          changePasswordModalShell.style.width = "";
          changePasswordModalShell.style.height = "";
          maxChangePasswordModal.setAttribute("title", "还原");
        } else {
          changePasswordModalShell.classList.remove("maximized");
          if (changePasswordModalShell.dataset.restoreWidth) {
            changePasswordModalShell.style.width = changePasswordModalShell.dataset.restoreWidth;
          }
          if (changePasswordModalShell.dataset.restoreHeight) {
            changePasswordModalShell.style.height = changePasswordModalShell.dataset.restoreHeight;
          }
          maxChangePasswordModal.setAttribute("title", "最大化");
        }
      });
    }

    if (submitChangePasswordBtn) {
      submitChangePasswordBtn.addEventListener("click", async () => {
        const oldPass = String(currentPasswordInput?.value || "");
        const newPass = String(newPasswordInput?.value || "");
        if (!oldPass || !newPass) {
          showToast("请输入当前密码和新密码", "error");
          return;
        }
        try {
          await authChangePassword(oldPass, newPass);
          showToast("密码修改成功");
          changePasswordModal?.classList.add("hidden");
        } catch (err) {
          showToast(`修改失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (openUserAdminBtn) {
      openUserAdminBtn.addEventListener("click", async () => {
        if (!currentUser || currentUser.role !== "admin") return;
        userAdminModal?.classList.remove("hidden");
        try {
          await refreshUserAdminTable();
        } catch (err) {
          showToast(`加载用户失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (closeUserAdminModal) {
      closeUserAdminModal.addEventListener("click", () => userAdminModal?.classList.add("hidden"));
    }

    if (maxUserAdminModal && userAdminModalShell) {
      maxUserAdminModal.addEventListener("click", () => {
        const isMaximized = userAdminModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (userAdminModalShell.style.width) {
            userAdminModalShell.dataset.restoreWidth = userAdminModalShell.style.width;
          }
          if (userAdminModalShell.style.height) {
            userAdminModalShell.dataset.restoreHeight = userAdminModalShell.style.height;
          }
          userAdminModalShell.classList.add("maximized");
          userAdminModalShell.style.width = "";
          userAdminModalShell.style.height = "";
          maxUserAdminModal.setAttribute("title", "还原");
        } else {
          userAdminModalShell.classList.remove("maximized");
          if (userAdminModalShell.dataset.restoreWidth) {
            userAdminModalShell.style.width = userAdminModalShell.dataset.restoreWidth;
          }
          if (userAdminModalShell.dataset.restoreHeight) {
            userAdminModalShell.style.height = userAdminModalShell.dataset.restoreHeight;
          }
          maxUserAdminModal.setAttribute("title", "最大化");
        }
      });
    }

    const closeCreateUserModalPanel = () => {
      createUserModal?.classList.add("hidden");
    };

    const closeAdminResetUserModalPanel = () => {
      adminResetUserModal?.classList.add("hidden");
      selectedAdminResetUsername = "";
      if (adminResetUserPasswordInput) adminResetUserPasswordInput.value = "";
    };

    if (closeCreateUserModal) {
      closeCreateUserModal.addEventListener("click", closeCreateUserModalPanel);
    }

    if (maxCreateUserModal && createUserModalShell) {
      maxCreateUserModal.addEventListener("click", () => {
        const isMaximized = createUserModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (createUserModalShell.style.width) {
            createUserModalShell.dataset.restoreWidth = createUserModalShell.style.width;
          }
          if (createUserModalShell.style.height) {
            createUserModalShell.dataset.restoreHeight = createUserModalShell.style.height;
          }
          createUserModalShell.classList.add("maximized");
          createUserModalShell.style.width = "";
          createUserModalShell.style.height = "";
          maxCreateUserModal.setAttribute("title", "还原");
        } else {
          createUserModalShell.classList.remove("maximized");
          if (createUserModalShell.dataset.restoreWidth) {
            createUserModalShell.style.width = createUserModalShell.dataset.restoreWidth;
          }
          if (createUserModalShell.dataset.restoreHeight) {
            createUserModalShell.style.height = createUserModalShell.dataset.restoreHeight;
          }
          maxCreateUserModal.setAttribute("title", "最大化");
        }
      });
    }

    if (cancelCreateUserBtn) {
      cancelCreateUserBtn.addEventListener("click", closeCreateUserModalPanel);
    }

    if (closeAdminResetUserModal) {
      closeAdminResetUserModal.addEventListener("click", closeAdminResetUserModalPanel);
    }

    if (cancelAdminResetUserBtn) {
      cancelAdminResetUserBtn.addEventListener("click", closeAdminResetUserModalPanel);
    }

    if (maxAdminResetUserModal && adminResetUserModalShell) {
      maxAdminResetUserModal.addEventListener("click", () => {
        const isMaximized = adminResetUserModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (adminResetUserModalShell.style.width) {
            adminResetUserModalShell.dataset.restoreWidth = adminResetUserModalShell.style.width;
          }
          if (adminResetUserModalShell.style.height) {
            adminResetUserModalShell.dataset.restoreHeight = adminResetUserModalShell.style.height;
          }
          adminResetUserModalShell.classList.add("maximized");
          adminResetUserModalShell.style.width = "";
          adminResetUserModalShell.style.height = "";
          maxAdminResetUserModal.setAttribute("title", "还原");
        } else {
          adminResetUserModalShell.classList.remove("maximized");
          if (adminResetUserModalShell.dataset.restoreWidth) {
            adminResetUserModalShell.style.width = adminResetUserModalShell.dataset.restoreWidth;
          }
          if (adminResetUserModalShell.dataset.restoreHeight) {
            adminResetUserModalShell.style.height = adminResetUserModalShell.dataset.restoreHeight;
          }
          maxAdminResetUserModal.setAttribute("title", "最大化");
        }
      });
    }

    if (submitAdminResetUserBtn) {
      submitAdminResetUserBtn.addEventListener("click", async () => {
        const username = String(selectedAdminResetUsername || "").trim();
        const newPassword = String(adminResetUserPasswordInput?.value || "");
        if (!username) {
          showToast("未选择用户", "error");
          return;
        }
        if (!newPassword) {
          showToast("请输入新密码", "error");
          return;
        }
        try {
          await adminResetPassword(username, newPassword);
          showToast(`用户 ${username} 密码修改成功`);
          closeAdminResetUserModalPanel();
          await refreshUserAdminTable();
        } catch (err) {
          showToast(`修改失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (refreshUsersBtn) {
      refreshUsersBtn.addEventListener("click", async () => {
        try {
          await refreshUserAdminTable();
        } catch (err) {
          showToast(`刷新失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (createUserBtn) {
      createUserBtn.addEventListener("click", () => {
        if (newUserNameInput) newUserNameInput.value = "";
        if (newUserPasswordInput) newUserPasswordInput.value = "";
        if (newUserRoleSelect) newUserRoleSelect.value = "user";
        createUserModal?.classList.remove("hidden");
      });
    }

    if (submitCreateUserBtn) {
      submitCreateUserBtn.addEventListener("click", async () => {
        const username = String(newUserNameInput?.value || "").trim();
        const password = String(newUserPasswordInput?.value || "");
        const role = String(newUserRoleSelect?.value || "user");
        if (!username || !password) {
          showToast("请输入用户名和初始密码", "error");
          return;
        }
        try {
          await adminCreateUser(username, password, role);
          if (newUserNameInput) newUserNameInput.value = "";
          if (newUserPasswordInput) newUserPasswordInput.value = "";
          closeCreateUserModalPanel();
          await refreshUserAdminTable();
          showToast("用户创建成功");
        } catch (err) {
          showToast(`创建失败：${err?.message || "未知错误"}`, "error");
        }
      });
    }

    if (userAdminTableBody) {
      userAdminTableBody.addEventListener("click", async (event) => {
        const row = event.target.closest("tr[data-username]");
        if (!row) return;
        const username = row.dataset.username || "";
        const role = row.dataset.role || "";

        if (event.target.closest(".js-reset-user-password")) {
          selectedAdminResetUsername = username;
          if (adminResetUserNameInput) adminResetUserNameInput.value = username;
          if (adminResetUserRoleInput) adminResetUserRoleInput.value = role;
          if (adminResetUserPasswordInput) adminResetUserPasswordInput.value = "";
          adminResetUserModal?.classList.remove("hidden");
          return;
        }

        if (event.target.closest(".js-toggle-lock-user")) {
          const lockButton = event.target.closest(".js-toggle-lock-user");
          if (lockButton?.hasAttribute("disabled")) return;
          const action = lockButton?.dataset.lockAction === "lock" ? "lock" : "unlock";
          const actionLabel = action === "lock" ? "锁定" : "解锁";
          const ok = window.confirm(`确认${actionLabel}用户 ${username} 吗？`);
          if (!ok) return;
          try {
            await adminToggleLock(username, action === "lock");
            await refreshUserAdminTable();
            showToast(`已${actionLabel}用户 ${username}`);
          } catch (err) {
            showToast(`${actionLabel}失败：${err?.message || "未知错误"}`, "error");
          }
          return;
        }

        if (event.target.closest(".js-delete-user")) {
          const ok = window.confirm(`确认删除用户 ${username} 吗？`);
          if (!ok) return;
          try {
            await adminDeleteUser(username);
            await refreshUserAdminTable();
            showToast(`已删除用户 ${username}`);
          } catch (err) {
            showToast(`删除失败：${err?.message || "未知错误"}`, "error");
          }
        }
      });
    }
  }

  async function bootstrapAuth() {
    const epoch = authStateEpoch;
    try {
      const me = await authMe();
      if (epoch !== authStateEpoch) return;
      if (currentUser) return;
      currentUser = me;
    } catch {
      if (epoch !== authStateEpoch) return;
      if (currentUser) return;
      currentUser = null;
    } finally {
      isAuthBootstrapInFlight = false;
      refreshUserMenu();
    }
  }

  function setupControls() {
    runFlow.addEventListener("click", () => {
      void openFlowResult("");
    });
    const modeGroup = document.getElementById("modeGroup");
    if (modeGroup) {
      modeGroup.addEventListener("change", () => saveHomeState());
    }
    searchInput.addEventListener("change", () => saveHomeState());
  }

  function normalizeFieldName(name) {
    return String(name || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");
  }

  async function fetchImportStatus() {
    const resp = await apiFetch(`${importStatusApiBase}/import-status`);
    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }
    return resp.json();
  }

  async function markImportUpdated(tableName) {
    const resp = await apiFetch(`${importStatusApiBase}/import-status/upsert`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ table_name: tableName })
    });

    if (!resp.ok) {
      throw new Error(`status ${resp.status}`);
    }

    return resp.json();
  }

  async function executeImport({ tableName, mapping, sheetName, file }) {
    const formData = new FormData();
    formData.append("table_name", tableName);
    formData.append("mapping_json", JSON.stringify(mapping));
    formData.append("sheet_name", sheetName || "");
    formData.append("file", file);

    const resp = await apiFetch(`${importStatusApiBase}/import/execute`, {
      method: "POST",
      body: formData
    });

    if (!resp.ok) {
      const text = await resp.text();
      const msg = parseErrorText(text, `status ${resp.status}`);
      throw new Error(msg);
    }

    return resp.json();
  }

  async function clearImportTable(tableName) {
    const formData = new FormData();
    formData.append("table_name", tableName);

    const resp = await apiFetch(`${importStatusApiBase}/import/clear-table`, {
      method: "POST",
      body: formData
    });

    if (!resp.ok) {
      const text = await resp.text();
      const msg = parseErrorText(text, `status ${resp.status}`);
      throw new Error(msg);
    }

    return resp.json();
  }

  async function refreshImportCardTimes() {
    let payload = {};
    try {
      payload = await fetchImportStatus();
    } catch {
      payload = {};
    }

    const rstranTime = payload?.rstran?.last_update || "--";
    const bwTime = payload?.bw_object_name?.last_update || "--";
    const rstranCount = Number(payload?.rstran?.last_count ?? 0);
    const bwCount = Number(payload?.bw_object_name?.last_count ?? 0);
    if (importRstranTime) importRstranTime.textContent = rstranTime;
    if (importBwObjectTime) importBwObjectTime.textContent = bwTime;
    if (importRstranCount) importRstranCount.textContent = String(rstranCount);
    if (importBwObjectCount) importBwObjectCount.textContent = String(bwCount);
  }

  function buildMapOptions(excelFields, selected) {
    const opts = [`<option value="">未映射</option>`]
      .concat(
        excelFields.map((field) => {
          const isSelected = selected === field ? "selected" : "";
          return `<option value="${escAttr(field)}" ${isSelected}>${esc(field)}</option>`;
        })
      )
      .join("");
    return opts;
  }

  function readMappingFromUI() {
    const mapping = {};
    importMapBody.querySelectorAll("tr").forEach((row) => {
      const dbField = row.dataset.dbField || "";
      const select = row.querySelector("select");
      if (dbField && select && select.value) {
        mapping[dbField] = select.value;
      }
    });

    const logicFields = logicManagedFields[activeImportTable] || {};
    Object.entries(logicFields).forEach(([dbField, marker]) => {
      mapping[dbField] = marker;
    });

    return mapping;
  }

  function getVisibleDbFields(tableName) {
    const allFields = importSchemas[tableName] || [];
    const hidden = new Set(Object.keys(logicManagedFields[tableName] || {}));
    return allFields.filter((f) => !hidden.has(f));
  }

  function renderMappingRows(dbFields, excelHeaders, presetMap = {}) {
    importMapBody.innerHTML = dbFields
      .map((dbField) => {
        const selected = presetMap[dbField] || "";
        const options = buildMapOptions(excelHeaders, selected);
        const rowClass = selected ? "mapped-row" : "";
        return `<tr data-db-field="${esc(dbField)}" class="${rowClass}"><td>${esc(dbField)}</td><td><select>${options}</select></td></tr>`;
      })
      .join("");

    importMapBody.querySelectorAll("tr").forEach((row) => {
      const select = row.querySelector("select");
      if (!select) return;
      select.addEventListener("change", () => {
        row.classList.toggle("mapped-row", Boolean(select.value));
        renderMappingProgressMeta();
      });
    });

    renderMappingProgressMeta();
  }

  function renderMappingProgressMeta() {
    if (!activeImportTable) return;

    const visibleDbFields = getVisibleDbFields(activeImportTable);
    const total = visibleDbFields.length;
    if (!total) return;

    const mapping = readMappingFromUI();
    const mapped = visibleDbFields.reduce((count, dbField) => {
      const value = String(mapping[dbField] || "").trim();
      if (!value || value.startsWith("__LOGIC_")) return count;
      return count + 1;
    }, 0);
    const unmapped = Math.max(0, total - mapped);
    const isComplete = unmapped === 0;
    const statusText = isComplete
      ? `映射：${mapped}/${total} 字段已满足`
      : `映射：${unmapped}个字段未映射`;
    const logicText = logicRuleDesc[activeImportTable] || "";

    importMeta.innerHTML = `
      <div class="import-meta-status ${isComplete ? "complete" : "incomplete"}">${esc(statusText)}</div>
      ${logicText ? `<div class="import-meta-logic">${esc(logicText)}</div>` : ""}
    `;
  }

  function suggestMapping(dbFields, excelHeaders) {
    const excelByNorm = new Map(excelHeaders.map((f) => [normalizeFieldName(f), f]));
    const mapped = {};

    dbFields.forEach((dbField) => {
      const direct = excelByNorm.get(normalizeFieldName(dbField));
      if (direct) {
        mapped[dbField] = direct;
      }
    });

    return mapped;
  }

  async function parseExcelHeaders(file) {
    const fileName = file.name.toLowerCase();
    if (fileName.endsWith(".csv")) {
      const text = await file.text();
      const first = text.split(/\r?\n/).find((line) => line.trim());
      return first ? first.split(",").map((x) => x.trim()).filter(Boolean) : [];
    }

    if (!window.XLSX) {
      window.alert("当前环境未加载 Excel 解析库，请联网后重试或使用 CSV 文件。");
      return [];
    }

    const buffer = await file.arrayBuffer();
    const workbook = window.XLSX.read(buffer, { type: "array" });
    const firstSheet = workbook.SheetNames[0];
    if (!firstSheet) return [];
    const sheet = workbook.Sheets[firstSheet];
    const rows = window.XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false });
    const headerRow = Array.isArray(rows[0]) ? rows[0] : [];
    return headerRow.map((x) => String(x || "").trim()).filter(Boolean);
  }

  function getHeadersFromSheet(workbook, sheetName) {
    if (!workbook || !sheetName) return [];
    const sheet = workbook.Sheets[sheetName];
    if (!sheet) return [];
    const rows = window.XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false });
    const headerRow = Array.isArray(rows[0]) ? rows[0] : [];
    return headerRow.map((x) => String(x || "").trim()).filter(Boolean);
  }

  function renderMappingByHeaders(headers) {
    const dbFields = getVisibleDbFields(activeImportTable);
    const suggested = suggestMapping(dbFields, headers);

    activeExcelHeaders = headers;
    if (!headers.length) {
      importMeta.innerHTML = "";
      importMapBody.innerHTML = "";
      showToast("未读取到表头，请确认第一行为字段名。");
      return;
    }

    renderMappingRows(dbFields, headers, suggested);
  }

  function openImportModal(tableName) {
    activeImportTable = tableName;
    activeExcelHeaders = [];
    importTaskLock = false;
    resetImportProgress();
    importModalTitle.textContent = `字段映射 - ${tableName}`;
    importMeta.innerHTML = "";
    importFileInput.value = "";
    if (importSheetSelect) {
      importSheetSelect.innerHTML = '<option value="">Sheet页</option>';
      importSheetSelect.disabled = true;
    }
    importMapBody.innerHTML = "";
    activeImportWorkbook = null;
    activeImportFileName = "";
    importModal.classList.remove("hidden");
  }

  function setupImportMapping() {
    if (!importRstranCard || !importBwObjectCard || !importModal) return;

    importRstranCard.addEventListener("click", () => openImportModal("rstran"));
    importBwObjectCard.addEventListener("click", () => openImportModal("bw_object_name"));

    closeImportModal.addEventListener("click", () => importModal.classList.add("hidden"));

    if (maxImportModal && importModalShell) {
      maxImportModal.addEventListener("click", () => {
        const isMaximized = importModalShell.classList.contains("maximized");
        if (!isMaximized) {
          if (importModalShell.style.width) {
            importModalShell.dataset.restoreWidth = importModalShell.style.width;
          }
          if (importModalShell.style.height) {
            importModalShell.dataset.restoreHeight = importModalShell.style.height;
          }
          importModalShell.classList.add("maximized");
          importModalShell.style.width = "";
          importModalShell.style.height = "";
          maxImportModal.setAttribute("title", "还原");
        } else {
          importModalShell.classList.remove("maximized");
          if (importModalShell.dataset.restoreWidth) {
            importModalShell.style.width = importModalShell.dataset.restoreWidth;
          }
          if (importModalShell.dataset.restoreHeight) {
            importModalShell.style.height = importModalShell.dataset.restoreHeight;
          }
          maxImportModal.setAttribute("title", "最大化");
        }
      });
    }

    importModal.addEventListener("click", (event) => {
      if (event.target === importModal) {
        importModal.classList.add("hidden");
      }
    });

    importFileInput.addEventListener("change", async () => {
      const file = importFileInput.files && importFileInput.files[0];
      if (!file || !activeImportTable) return;

      activeImportFileName = file.name;
      const fileNameLower = file.name.toLowerCase();

      if (fileNameLower.endsWith(".csv")) {
        activeImportWorkbook = null;
        if (importSheetSelect) {
          importSheetSelect.innerHTML = '<option value="">CSV无Sheet</option>';
          importSheetSelect.disabled = true;
        }
        const headers = await parseExcelHeaders(file);
        renderMappingByHeaders(headers);
        return;
      }

      if (!window.XLSX) {
        window.alert("当前环境未加载 Excel 解析库，请联网后重试或使用 CSV 文件。");
        return;
      }

      const buffer = await file.arrayBuffer();
      const workbook = window.XLSX.read(buffer, { type: "array" });
      activeImportWorkbook = workbook;
      const sheets = workbook.SheetNames || [];

      if (importSheetSelect) {
        importSheetSelect.innerHTML = sheets
          .map((name, idx) => `<option value="${escAttr(name)}" ${idx === 0 ? "selected" : ""}>${esc(name)}</option>`)
          .join("");
        importSheetSelect.disabled = sheets.length === 0;
      }

      const firstSheet = sheets[0] || "";
      const headers = getHeadersFromSheet(workbook, firstSheet);
      renderMappingByHeaders(headers);
    });

    if (importSheetSelect) {
      importSheetSelect.addEventListener("change", () => {
        if (!activeImportWorkbook) return;
        const sheetName = importSheetSelect.value;
        const headers = getHeadersFromSheet(activeImportWorkbook, sheetName);
        renderMappingByHeaders(headers);
      });
    }

    autoMapBtn.addEventListener("click", () => {
      if (!activeImportTable || !activeExcelHeaders.length) return;
      const dbFields = getVisibleDbFields(activeImportTable);
      const suggested = suggestMapping(dbFields, activeExcelHeaders);
      renderMappingRows(dbFields, activeExcelHeaders, suggested);
    });

    if (clearAllTableDataBtn) {
      clearAllTableDataBtn.addEventListener("click", async () => {
        if (importTaskLock) return;
        const ok = window.confirm(`确认删除当前表(${activeImportTable})的全部数据吗？此操作不可撤销。`);
        if (!ok) return;

        try {
          importTaskLock = true;
          setImportBusyState(true, "正在删除库表数据...");
          const result = await clearImportTable(activeImportTable);
          await refreshImportCardTimes();
          const deletedCount = Number(result.deleted_rows || 0);
          const currentCount = Number(result.current_count || 0);
          const successText = `删除完成：${result.table_name}，共删除 ${deletedCount} 条，当前数据条目 ${currentCount} 条，更新时间 ${result.last_update}。`;
          showToast(successText);
          completeImportBusyState();
        } catch (err) {
          showToast(`删除失败，请确认后端服务已启动（${importStatusApiBase}）。${err?.message ? ` 详情: ${err.message}` : ""}`, "error");
          setImportBusyState(false);
        } finally {
          importTaskLock = false;
        }
      });
    }

    confirmImportBtn.addEventListener("click", async () => {
      if (importTaskLock) return;
      if (!activeImportTable) return;
      const selectedFile = importFileInput.files && importFileInput.files[0];
      if (!selectedFile) {
        window.alert("请先选择要导入的 Excel/CSV 文件。");
        return;
      }

      const mapping = readMappingFromUI();
      const mappedCount = Object.values(mapping).filter((v) => v && !String(v).startsWith("__LOGIC_")).length;
      if (!mappedCount) {
        window.alert("请至少映射一个字段后再导入。");
        return;
      }

      try {
        importTaskLock = true;
        setImportBusyState(true, "正在导入数据...");
        const sheetName = importSheetSelect && !importSheetSelect.disabled ? importSheetSelect.value : "";
        const result = await executeImport({
          tableName: activeImportTable,
          mapping,
          sheetName,
          file: selectedFile
        });
        await refreshImportCardTimes();
        const importedCount = Number(result.db_count ?? result.affected_rows ?? 0);
        const successText = `导入完成: ${result.table_name}，当前数据条目 ${importedCount}，更新时间 ${result.last_update}`;
        showToast(successText);
        completeImportBusyState();
      } catch (err) {
        const rawMsg = String(err?.message || "").trim();
        if (/no rows to import/i.test(rawMsg)) {
          showToast("导入失败：文件未读取到可导入数据行。请确认有表头并至少包含1行数据，再重试。", "error");
        } else if (/未读取到可导入数据行/.test(rawMsg)) {
          showToast(rawMsg, "error");
        } else {
          showToast(`导入失败，请确认后端服务已启动（${importStatusApiBase}）。${rawMsg ? ` 详情: ${rawMsg}` : ""}`, "error");
        }
        setImportBusyState(false);
      } finally {
        importTaskLock = false;
      }
    });

    refreshImportCardTimes();
  }

  function setupSelectionHighlight() {
    document.addEventListener("click", (event) => {
      const passiveControl = event.target.closest(".glass-radio, .liquid-toggle");
      if (passiveControl) {
        document.querySelectorAll(".selected-outline").forEach((el) => el.classList.remove("selected-outline"));
        return;
      }

      const target = event.target.closest(".glass-btn, .search-wrap");
      if (!target || target.classList.contains("text-btn")) return;
      document.querySelectorAll(".selected-outline").forEach((el) => el.classList.remove("selected-outline"));
      target.classList.add("selected-outline");
    });
  }


  function startBackgroundAnimation() {
    const canvas = document.getElementById("bgCanvas");
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const particles = Array.from({ length: 24 }).map((_, i) => ({
      x: Math.random(),
      y: Math.random(),
      r: 2 + Math.random() * 3,
      vx: (Math.random() - 0.5) * 0.00012,
      vy: (Math.random() - 0.5) * 0.00012,
      hue: i % 2 === 0 ? 200 : 165
    }));

    function resize() {
      canvas.width = window.innerWidth;
      canvas.height = window.innerHeight;
    }

    function draw() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      particles.forEach((p) => {
        p.x += p.vx;
        p.y += p.vy;
        if (p.x < 0 || p.x > 1) p.vx *= -1;
        if (p.y < 0 || p.y > 1) p.vy *= -1;

        const px = p.x * canvas.width;
        const py = p.y * canvas.height;
        const grad = ctx.createRadialGradient(px, py, 0, px, py, p.r * 12);
        grad.addColorStop(0, `hsla(${p.hue}, 92%, 78%, 0.45)`);
        grad.addColorStop(1, "hsla(0, 0%, 100%, 0)");

        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.arc(px, py, p.r * 11, 0, Math.PI * 2);
        ctx.fill();
      });

      for (let i = 0; i < particles.length; i += 1) {
        for (let j = i + 1; j < particles.length; j += 1) {
          const a = particles[i];
          const b = particles[j];
          const dx = (a.x - b.x) * canvas.width;
          const dy = (a.y - b.y) * canvas.height;
          const d = Math.hypot(dx, dy);
          if (d < 180) {
            ctx.strokeStyle = `rgba(130, 195, 255, ${0.13 * (1 - d / 180)})`;
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(a.x * canvas.width, a.y * canvas.height);
            ctx.lineTo(b.x * canvas.width, b.y * canvas.height);
            ctx.stroke();
          }
        }
      }

      requestAnimationFrame(draw);
    }

    window.addEventListener("resize", resize);
    resize();
    draw();
  }

  function init() {
    document.body.setAttribute("data-dropdown-mode", dropdownMode);
    loadRecentSearches();
    setupSearchBehavior();
    setupAuthUi();
    refreshUserMenu();
    setupControls();
    restoreHomeState();
    setupMorePage();
    setupMoreDialogDrag();
    setupMoreDialogResize();
    setupMoreTableResize();
    setupSelectionHighlight();
    setupImportMapping();
    void bootstrapAuth();
    startBackgroundAnimation();
  }

  init();
})();
