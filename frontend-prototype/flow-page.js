(() => {
  function showFatalBanner(message) {
    const existed = document.getElementById("flowFatalBanner");
    if (existed) existed.remove();
    const bar = document.createElement("div");
    bar.id = "flowFatalBanner";
    bar.style.position = "fixed";
    bar.style.left = "10px";
    bar.style.top = "10px";
    bar.style.zIndex = "9999";
    bar.style.maxWidth = "min(760px, calc(100vw - 20px))";
    bar.style.padding = "8px 12px";
    bar.style.borderRadius = "10px";
    bar.style.background = "rgba(88, 16, 24, 0.92)";
    bar.style.border = "1px solid rgba(255, 142, 142, 0.68)";
    bar.style.color = "#ffeef0";
    bar.style.fontSize = "12px";
    bar.style.lineHeight = "1.4";
    bar.style.whiteSpace = "pre-wrap";
    bar.textContent = message;
    document.body.appendChild(bar);
  }

  window.addEventListener("error", (event) => {
    const msg = String(event?.message || "Unknown error");
    const file = String(event?.filename || "").split("/").pop() || "inline";
    const line = Number(event?.lineno || 0);
    showFatalBanner(`流图脚本异常: ${msg} (${file}:${line})`);
  });

  const appVersion = String(window.__DATAFLOW_APP_VERSION__ || "1.0").trim() || "1.0";
  const flowVersionBadge = document.getElementById("flowVersionBadge");
  if (flowVersionBadge) {
    flowVersionBadge.textContent = `v${appVersion}`;
  }
  document.title = `数据流图 v${appVersion}`;

  const flowCanvas = document.getElementById("flowCanvas");
  const flowViewport = document.getElementById("flowViewport");
  const flowNodes = document.getElementById("flowNodes");
  const flowEdges = document.getElementById("flowEdges");
  const zoomInBtn = document.getElementById("zoomIn");
  const zoomOutBtn = document.getElementById("zoomOut");
  const zoomSlider = document.getElementById("zoomSlider");
  const zoomValue = document.getElementById("zoomValue");
  const zoomDrawer = document.getElementById("zoomDrawer");
  const zoomToggle = document.getElementById("zoomToggle");
  const legendDrawer = document.getElementById("legendDrawer");
  const legendToggle = document.getElementById("legendToggle");
  const legendList = document.getElementById("legendList");
  const includeDrawer = document.getElementById("includeDrawer");
  const includeToggle = document.getElementById("includeToggle");
  const includeToggles = document.getElementById("includeToggles");
  const miniMapDrawer = document.getElementById("miniMapDrawer");
  const miniMapToggle = document.getElementById("miniMapToggle");
  const miniMapCanvas = document.getElementById("miniMapCanvas");
  const layoutSpreadControl = document.getElementById("layoutSpreadControl");
  const textToggleBtn = document.getElementById("textToggle");
  const textToggleInput = document.getElementById("textToggleInput");
  const resetViewBtn = document.getElementById("resetView");
  const hiddenToggleBtn = document.getElementById("hiddenToggleBtn");
  const backToSearchLink = document.querySelector('.topbar-actions a[href="./index.html"]');
  const flowTitleEl = document.querySelector(".flow-title");

  let selectedTypes = [];
  let graphData = { nodes: [], edges: [] };
  let graphColorMap = {};
  let focusStartName = "";
  let focusStartSource = "";
  let focusStartType = "";
  let currentTraceMode = "both";
  let layoutSpacingL1 = null;
  let nodeExtraTextVisible = false;
  let scale = 1;
  let offsetX = 0;
  let offsetY = 0;
  let layoutEngine = "elk";
  let elkPendingSignature = "";
  let elkDisabled = false;
  let renderRevision = 0;
  const nodeManualOffsets = new Map();
  const elkPositionCache = new Map();

  const GRAPH_WIDTH = 1600;
  const GRAPH_HEIGHT = 900;
  const FIXED_TYPES = ["infosource", "adso"];
  const OPTIONAL_TYPES = ["datasource", "cp", "report"];
  const MIN_SCALE = 0.4;
  const MAX_SCALE = 2.2;
  const L1_MIN = Math.round(110 * 0.2);
  const L1_MAX = Math.round(110 * 0.9);
  const L1_DEFAULT = Math.round((L1_MIN + L1_MAX) / 2);
  const L1_STEP = Math.round(110 * 0.18);
  const DRAWER_TOP_START = 50;
  const DRAWER_GAP = 8;
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

  const flowApiBase = resolveApiBase();
  const ELK_NODE_WIDTH = 130;
  const ELK_NODE_HEIGHT = 36;
  const ELK_CANVAS_PADDING = 64;
  const ELK_LAYER_SPACING = 72;
  const ELK_NODE_SPACING = 24;
  const FLOW_SETTLE_MS = 1200;
  const pointers = new Map();
  let dragStartX = 0;
  let dragStartY = 0;
  let startOffsetX = 0;
  let startOffsetY = 0;
  let isDragging = false;
  let pinchStartDistance = 0;
  let pinchStartScale = 1;
  let pinchWorldX = 0;
  let pinchWorldY = 0;
  let currentGraphWidth = GRAPH_WIDTH;
  let currentGraphHeight = GRAPH_HEIGHT;
  let miniMapTransform = {
    scale: 1,
    offsetX: 0,
    offsetY: 0,
    canvasWidth: 0,
    canvasHeight: 0
  };
  let nodeContextMenu = null;
  let contextMenuPayload = null;
  let activeRenderEdges = [];
  let edgeRefsByNode = new Map();
  let activeFlowNodeId = "";
  let flowSettleTimer = null;
  let flowStatusTimer = null;
  let hiddenObjectKeys = new Set();
  let hiddenCountInCurrentView = 0;
  let hiddenPreviewOpen = false;
  let hiddenToastTimer = null;
  let lastRenderContext = null;

  if (flowCanvas) {
    flowCanvas.style.minHeight = "62vh";
  }

  function esc(str) {
    return String(str)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function clearFlowStatusOverlay() {
    document.querySelectorAll(".flow-status-banner").forEach((el) => el.remove());
    if (flowStatusTimer) {
      window.clearTimeout(flowStatusTimer);
      flowStatusTimer = null;
    }
  }

  function navigateBackHome() {
    const qp = new URLSearchParams();
    if (currentTraceMode) qp.set("mode", currentTraceMode);
    const next = `./index.html${qp.toString() ? `?${qp.toString()}` : ""}`;
    window.location.href = next;
  }

  function showFlowStatusOverlay(message, options = {}) {
    clearFlowStatusOverlay();

    const variant = String(options.variant || "info").trim().toLowerCase();
    const autoBackMs = Number(options.autoBackMs || 0);

    const box = document.createElement("section");
    box.className = `flow-status-banner ${variant}`;
    box.setAttribute("role", "status");
    box.setAttribute("aria-live", "polite");
    box.style.position = "fixed";
    box.style.left = "50%";
    box.style.top = "50%";
    box.style.transform = "translate(-50%, -50%)";
    box.style.zIndex = "900";
    box.style.width = "min(560px, calc(100% - 36px))";
    box.style.padding = "16px 18px";
    box.style.borderRadius = "14px";
    box.style.border = "1px solid rgba(173, 214, 255, 0.36)";
    box.style.background = "rgba(7, 15, 28, 0.8)";
    box.style.backdropFilter = "blur(6px)";
    box.style.boxShadow = "0 10px 34px rgba(5, 13, 26, 0.5)";
    box.style.textAlign = "center";

    if (variant === "warning") {
      box.style.borderColor = "rgba(255, 219, 125, 0.46)";
      box.style.background = "rgba(31, 24, 8, 0.82)";
    } else if (variant === "danger") {
      box.style.borderColor = "rgba(255, 146, 146, 0.5)";
      box.style.background = "rgba(43, 14, 18, 0.82)";
    }

    const msg = document.createElement("p");
    msg.textContent = String(message || "当前没有可展示的数据流。");
    msg.style.margin = "0";
    msg.style.color = "#eaf4ff";
    msg.style.fontSize = "14px";
    msg.style.lineHeight = "1.5";
    box.appendChild(msg);

    const actions = document.createElement("div");
    actions.className = "flow-status-actions";
    actions.style.marginTop = "12px";
    actions.style.display = "flex";
    actions.style.justifyContent = "center";

    const homeBtn = document.createElement("button");
    homeBtn.type = "button";
    homeBtn.className = "flow-status-btn";
    homeBtn.textContent = "返回主页";
    homeBtn.style.border = "1px solid rgba(255, 255, 255, 0.3)";
    homeBtn.style.background = "rgba(255, 255, 255, 0.11)";
    homeBtn.style.color = "#f4f9ff";
    homeBtn.style.borderRadius = "999px";
    homeBtn.style.padding = "6px 14px";
    homeBtn.style.fontSize = "12px";
    homeBtn.style.cursor = "pointer";
    homeBtn.addEventListener("click", navigateBackHome);
    actions.appendChild(homeBtn);

    box.appendChild(actions);
    document.body.appendChild(box);

    if (autoBackMs > 0) {
      flowStatusTimer = window.setTimeout(() => {
        navigateBackHome();
      }, autoBackMs);
    }
  }

  function getQueryParams() {
    const params = new URLSearchParams(window.location.search);
    return {
      start: params.get("start") || "",
      startSource: params.get("startSource") || "",
      startType: params.get("startType") || "",
      mode: params.get("mode") || "both",
      types: (params.get("types") || "").split(",").filter(Boolean),
      l1: params.get("l1") || ""
    };
  }

  function parseLayoutL1(rawValue) {
    const n = Number(rawValue);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.max(L1_MIN, Math.min(L1_MAX, Math.round(n)));
  }

  function normalizeNodeName(value) {
    return String(value || "").trim().toLowerCase();
  }

  function isFocusNode(node) {
    const seed = normalizeNodeName(focusStartName);
    if (!seed) return false;
    const nodeId = normalizeNodeName(node?.id);
    const nodeLabel = normalizeNodeName(node?.label);
    return nodeId === seed || nodeLabel === seed;
  }

  function syncBackLink(params) {
    if (!backToSearchLink) return;
    const qp = new URLSearchParams();
    if (params.mode) qp.set("mode", params.mode);
    backToSearchLink.href = `./index.html${qp.toString() ? `?${qp.toString()}` : ""}`;
  }

  function setupBackHomeAction() {
    if (!backToSearchLink) return;
    backToSearchLink.addEventListener("click", (event) => {
      const href = backToSearchLink.getAttribute("href") || "./index.html";
      if (window.opener && !window.opener.closed) {
        event.preventDefault();
        try {
          window.opener.focus();
        } catch {
          // Ignore cross-window focus failures and continue.
        }
        // Most browsers allow script-closing tabs opened by script.
        window.close();
        // Fallback if close is blocked by browser policy.
        window.location.href = href;
      }
    });
  }

  function syncFlowModeClass() {
    if (!flowCanvas) return;
    flowCanvas.classList.remove("flow-mode-upstream", "flow-mode-downstream", "flow-mode-both", "flow-mode-full");
    flowCanvas.classList.add(`flow-mode-${currentTraceMode}`);
  }

  function getEffectiveL1() {
    return Math.max(L1_MIN, Math.min(L1_MAX, layoutSpacingL1 ?? L1_DEFAULT));
  }

  function applyGraphDimensions(width, height) {
    currentGraphWidth = Math.max(GRAPH_WIDTH, Math.round(width || GRAPH_WIDTH));
    currentGraphHeight = Math.max(GRAPH_HEIGHT, Math.round(height || GRAPH_HEIGHT));

    const w = `${currentGraphWidth}px`;
    const h = `${currentGraphHeight}px`;
    flowViewport.style.width = w;
    flowViewport.style.height = h;
    flowEdges.style.width = w;
    flowEdges.style.height = h;
    flowNodes.style.width = w;
    flowNodes.style.height = h;
  }

  function syncL1InUrl() {
    const params = new URLSearchParams(window.location.search);
    params.set("l1", String(getEffectiveL1()));
    const next = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, "", next);
  }


  function getElkLayoutSignature(nodes, edges) {
    const l1 = getEffectiveL1();
    const nodePart = nodes
      .map((n) => `${n.id}:${Number.isFinite(Number(n.level)) ? Number(n.level) : 999}:${n.type || ""}`)
      .sort()
      .join("|");
    const edgePart = edges
      .map((e) => `${e.source}>${e.target}`)
      .sort()
      .join("|");
    // Include spread so ELK cache refreshes when user adjusts spacing.
    return `${currentTraceMode}|elk-template-v2|l1:${l1}|${nodePart}|${edgePart}`;
  }

  function estimateElkNodeSize(node) {
    void node;
    return { width: ELK_NODE_WIDTH, height: ELK_NODE_HEIGHT };
  }

  async function computeElkLayoutPositions(nodes, edges) {
    if (elkDisabled) {
      throw new Error("ELK layout is disabled");
    }
    if (!window.ELK) {
      throw new Error("ELK runtime is not available");
    }

    const elk = new window.ELK();
    const l1 = getEffectiveL1();
    const spreadRatio = Math.max(0, Math.min(1, (l1 - L1_MIN) / Math.max(1, L1_MAX - L1_MIN)));
    // Horizontal expansion should be very obvious; vertical expansion should stay subtle.
    const elkLayerSpacing = Math.round(64 + spreadRatio * 14);
    const elkNodeSpacing = Math.round(24 + spreadRatio * 240);
    const directionByMode = {
      upstream: "UP",
      downstream: "UP",
      both: "UP",
      full: "UP"
    };

    const graph = {
      id: "root",
      layoutOptions: {
        "elk.algorithm": "layered",
        "elk.direction": directionByMode[currentTraceMode] || "DOWN",
        "elk.layered.spacing.nodeNodeBetweenLayers": String(elkLayerSpacing),
        "elk.spacing.nodeNode": String(elkNodeSpacing),
        "elk.layered.nodePlacement.bk.fixedAlignment": "BALANCED",
        "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
        "elk.edgeRouting": "ORTHOGONAL"
      },
      children: nodes.map((n) => ({
        id: n.id,
        width: estimateElkNodeSize(n).width,
        height: estimateElkNodeSize(n).height
      })),
      edges: edges.map((e, idx) => ({
        id: `e-${idx}-${e.source}-${e.target}`,
        sources: [e.source],
        targets: [e.target]
      }))
    };

    const result = await elk.layout(graph);
    const children = Array.isArray(result?.children) ? result.children : [];
    if (!children.length) {
      return {
        positions: new Map(),
        canvasWidth: GRAPH_WIDTH,
        canvasHeight: GRAPH_HEIGHT
      };
    }

    const minX = Math.min(...children.map((c) => Number(c.x) || 0));
    const minY = Math.min(...children.map((c) => Number(c.y) || 0));
    const maxX = Math.max(...children.map((c) => (Number(c.x) || 0) + (Number(c.width) || ELK_NODE_WIDTH)));
    const maxY = Math.max(...children.map((c) => (Number(c.y) || 0) + (Number(c.height) || ELK_NODE_HEIGHT)));
    const layoutW = Math.max(1, maxX - minX);
    const layoutH = Math.max(1, maxY - minY);

    const canvasWidth = Math.max(GRAPH_WIDTH, Math.ceil(layoutW + ELK_CANVAS_PADDING * 2));
    const canvasHeight = Math.max(GRAPH_HEIGHT, Math.ceil(layoutH + ELK_CANVAS_PADDING * 2));

    const positions = new Map();
    children.forEach((c) => {
      const rawX = (Number(c.x) || 0) - minX + ELK_CANVAS_PADDING;
      const rawY = (Number(c.y) || 0) - minY + ELK_CANVAS_PADDING;
      positions.set(c.id, { x: rawX, y: rawY });
    });
    return { positions, canvasWidth, canvasHeight };
  }

  function requestElkLayoutIfNeeded(signature, nodes, edges, revision) {
    if (elkPositionCache.has(signature)) return;
    if (elkPendingSignature === signature) return;

    elkPendingSignature = signature;
    computeElkLayoutPositions(nodes, edges)
      .then((layoutResult) => {
        elkPositionCache.set(signature, layoutResult);
      })
      .catch((error) => {
        console.warn("[flow elk] layout failed, fallback to rule layout", error);
        elkDisabled = true;
        layoutEngine = "rule";
      })
      .finally(() => {
        if (elkPendingSignature === signature) {
          elkPendingSignature = "";
        }
        if (layoutEngine === "elk" && revision === renderRevision) {
          buildGraphView(selectedTypes);
          fitToView();
        }
      });
  }

  function refreshSpreadControlState() {
    if (!layoutSpreadControl) return;
    const value = getEffectiveL1();
    const normalized = (value - L1_MIN) / Math.max(1, L1_MAX - L1_MIN);

    let preset = 0;
    if (normalized <= 0.15) preset = -2;
    else if (normalized < 0.45) preset = -1;
    else if (normalized <= 0.55) preset = 0;
    else if (normalized < 0.85) preset = 1;
    else preset = 2;

    layoutSpreadControl.querySelectorAll(".spread-petal, .spread-core").forEach((btn) => btn.classList.remove("is-active"));
    const selector = preset === 0
      ? '.spread-core[data-l1-step="0"]'
      : `.spread-petal[data-l1-step="${preset}"]`;
    const activeBtn = layoutSpreadControl.querySelector(selector);
    if (activeBtn) activeBtn.classList.add("is-active");

    const nameEl = layoutSpreadControl.querySelector(".spread-name");
    if (nameEl) {
      nameEl.textContent = "Spread Control";
    }
  }

  function setupLayoutSpreadControl() {
    if (!layoutSpreadControl) return;

    const applyDelta = (step) => {
      const base = getEffectiveL1();
      const effectiveStep = step === 2 ? 10 : step;
      const next = step === 0
        ? L1_DEFAULT
        : Math.max(L1_MIN, Math.min(L1_MAX, Math.round(base + effectiveStep * L1_STEP)));
      layoutSpacingL1 = next;
      buildGraphView(selectedTypes);
      syncL1InUrl();
      syncBackLink({ start: focusStartName, mode: currentTraceMode, l1: String(next) });
      refreshSpreadControlState();
    };

    layoutSpreadControl.addEventListener("click", (event) => {
      const btn = event.target.closest("button[data-l1-step]");
      if (!btn) return;
      const step = Number(btn.dataset.l1Step || "0");
      if (!Number.isFinite(step)) return;
      applyDelta(step);
    });

    refreshSpreadControlState();
  }

  function refreshTextToggleState() {
    if (!textToggleBtn || !textToggleInput || !flowCanvas) return;
    flowCanvas.classList.toggle("show-node-extra-text", nodeExtraTextVisible);
    textToggleBtn.classList.toggle("is-active", nodeExtraTextVisible);
    textToggleInput.checked = nodeExtraTextVisible;
    textToggleBtn.title = nodeExtraTextVisible ? "隐藏附加文本" : "显示附加文本";
  }

  function setupTextToggle() {
    if (!textToggleInput) return;
    textToggleInput.addEventListener("change", () => {
      nodeExtraTextVisible = Boolean(textToggleInput.checked);
      refreshTextToggleState();
    });
    refreshTextToggleState();
  }

  function wrapByWordCount(text, maxCharsPerLine = 25) {
    const raw = String(text || "").trim();
    if (!raw) return "";
    if (raw.length <= maxCharsPerLine) return raw;

    const words = raw.split(/\s+/).filter(Boolean);
    if (!words.length) return raw;

    const lines = [];
    let line = "";
    words.forEach((word) => {
      if (!line) {
        line = word;
        return;
      }
      const candidate = `${line} ${word}`;
      if (candidate.length <= maxCharsPerLine) {
        line = candidate;
      } else {
        lines.push(line);
        // Keep long words intact to avoid breaking words.
        line = word;
      }
    });
    if (line) lines.push(line);
    return lines.join("\n");
  }

  function splitDatasourceLabel(node) {
    const raw = String(node?.label || "").trim();
    const techName = String(node?.id || "").trim();
    const line1Base = techName || raw;
    if (!line1Base) return null;

    let sourceSystem = "";

    // Preferred form: "<tech_name> <source_system>"
    if (techName && raw.startsWith(`${techName} `)) {
      sourceSystem = raw.slice(techName.length).trim();
    }

    // Alternate form: "<tech_name> [<source_system>]"
    if (!sourceSystem) {
      const bracket = raw.match(/^(.+?)\s*\[([^\]]+)\]\s*$/);
      if (bracket) {
        sourceSystem = bracket[2].trim();
      }
    }

    // Fallback: split by last token when raw looks like "<name> <system>".
    if (!sourceSystem) {
      const parts = raw.split(/\s+/).filter(Boolean);
      if (parts.length >= 2) {
        const tail = parts[parts.length - 1];
        const head = parts.slice(0, -1).join(" ");
        const looksLikeTechName = head.includes("_") || (techName && head === techName);
        if (looksLikeTechName) {
          sourceSystem = tail;
        }
      }
    }

    sourceSystem = sourceSystem.replace(/^\[/, "").replace(/\]$/, "").trim();
    if (!sourceSystem) return null;

    const escapeRegExp = (value) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const srcRe = escapeRegExp(sourceSystem);
    let line1 = line1Base
      .replace(new RegExp(`\\s*\\[${srcRe}\\]\\s*$`, "i"), "")
      .replace(new RegExp(`\\s+${srcRe}\\s*$`, "i"), "")
      .trim();
    if (!line1) {
      line1 = raw
        .replace(new RegExp(`\\s*\\[${srcRe}\\]\\s*$`, "i"), "")
        .replace(new RegExp(`\\s+${srcRe}\\s*$`, "i"), "")
        .trim();
    }
    if (!line1) line1 = line1Base;

    return {
      line1,
      line2: `[${sourceSystem}]`
    };
  }

  function normalizeHiddenObject(value) {
    return String(value || "").trim().toUpperCase();
  }

  function normalizeHiddenSource(value) {
    const txt = String(value || "").trim().toUpperCase();
    return txt.length >= 3 ? txt : "";
  }

  function buildHiddenKey(bwObject, sourcesys) {
    const objectPart = normalizeHiddenObject(bwObject);
    if (!objectPart) return "";
    const sourcePart = normalizeHiddenSource(sourcesys);
    return `${objectPart}||${sourcePart}`;
  }

  function parseDatasourceSourceFromLine2(line2) {
    return normalizeHiddenSource(String(line2 || "").replace(/^\[/, "").replace(/\]$/, "").trim());
  }

  function getNodeHiddenIdentity(node) {
    const bwObject = normalizeHiddenObject(node?.id);
    if (!bwObject) {
      return { bwObject: "", sourcesys: "", key: "" };
    }

    let sourcesys = "";
    if (normalizeTypeCategory(node?.type) === "datasource") {
      const ds = splitDatasourceLabel(node);
      if (ds?.line2) {
        sourcesys = parseDatasourceSourceFromLine2(ds.line2);
      }
    }

    return {
      bwObject,
      sourcesys,
      key: buildHiddenKey(bwObject, sourcesys)
    };
  }

  function hideNodeContextMenu() {
    if (!nodeContextMenu) return;
    nodeContextMenu.classList.add("hidden");
    contextMenuPayload = null;
  }

  function showFlowToast(message) {
    const text = String(message || "").trim();
    if (!text) return;

    let toast = document.getElementById("flowToast");
    if (!toast) {
      toast = document.createElement("div");
      toast.id = "flowToast";
      toast.className = "flow-toast";
      document.body.appendChild(toast);
    }

    toast.textContent = text;
    toast.classList.add("show");

    if (hiddenToastTimer) {
      window.clearTimeout(hiddenToastTimer);
    }
    hiddenToastTimer = window.setTimeout(() => {
      toast.classList.remove("show");
      hiddenToastTimer = null;
    }, 1900);
  }

  async function copyTextToClipboard(text) {
    const value = String(text || "").trim();
    if (!value) return;
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      const ta = document.createElement("textarea");
      ta.value = value;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.left = "-9999px";
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
    }
  }

  async function copyNodeContextPayload(mode = "both") {
    if (!contextMenuPayload) return;
    const techName = String(contextMenuPayload.techName || "").trim();
    const hoverText = String(contextMenuPayload.hoverText || "").trim();

    let textToCopy = "";
    if (mode === "tech") {
      textToCopy = techName;
    } else if (mode === "name") {
      textToCopy = hoverText;
    } else {
      textToCopy = hoverText ? `${techName}\n${hoverText}` : techName;
    }

    if (!textToCopy) {
      hideNodeContextMenu();
      return;
    }
    await copyTextToClipboard(textToCopy);
    hideNodeContextMenu();
  }

  async function flowApiRequest(path, options = {}) {
    let resp;
    try {
      resp = await fetch(`${flowApiBase}${path}`, {
        credentials: "include",
        ...options
      });
    } catch (networkErr) {
      const err = new Error("network_error");
      err.code = "network_error";
      err.cause = networkErr;
      throw err;
    }

    if (!resp.ok) {
      let detail = "";
      try {
        const payload = await resp.json();
        detail = String(payload?.detail || "").trim();
      } catch {
        try {
          detail = String(await resp.text() || "").trim();
        } catch {
          detail = "";
        }
      }
      const err = new Error(`status ${resp.status}${detail ? `: ${detail}` : ""}`);
      err.status = resp.status;
      err.detail = detail;
      throw err;
    }

    if (resp.status === 204) {
      return {};
    }
    return resp.json();
  }

  function normalizeApiErrorMessage(error) {
    const detail = String(error?.detail || error?.message || "").trim();
    const status = Number(error?.status || 0);
    const normalized = detail.toLowerCase();
    if (status === 404 || /not\s*fou?nd|not\s*fund/.test(normalized)) {
      return "后端未找到隐藏对象接口，请先部署最新后端";
    }
    if (status === 401 || status === 403) {
      return "登录状态失效，请重新登录后重试";
    }
    return detail || "请稍后重试";
  }

  async function loadHiddenObjectKeys() {
    const payload = await flowApiRequest("/flow/hidden-objects");
    const rows = Array.isArray(payload?.items) ? payload.items : [];
    hiddenObjectKeys = new Set(
      rows
        .map((row) => buildHiddenKey(row?.bw_object, row?.sourcesys))
        .filter(Boolean)
    );
  }

  async function addHiddenObject(bwObject, sourcesys) {
    return flowApiRequest("/flow/hidden-objects", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        bw_object: normalizeHiddenObject(bwObject),
        sourcesys: normalizeHiddenSource(sourcesys)
      })
    });
  }

  async function removeHiddenObject(bwObject, sourcesys) {
    const query = new URLSearchParams({
      bw_object: normalizeHiddenObject(bwObject),
      sourcesys: normalizeHiddenSource(sourcesys)
    });
    return flowApiRequest(`/flow/hidden-objects?${query.toString()}`, {
      method: "DELETE"
    });
  }

  function updateHiddenToggleButtonState() {
    if (!hiddenToggleBtn) return;
    hiddenToggleBtn.textContent = hiddenPreviewOpen ? "关闭隐藏对象" : "打开隐藏对象";
    hiddenToggleBtn.classList.toggle("is-active", hiddenPreviewOpen);
  }

  function applyHiddenVisibilityToRenderedGraph(options = {}) {
    if (!lastRenderContext) return 0;

    const { edges, nodePos, nodeSize, hiddenNodeIds } = lastRenderContext;
    const forceHideNodeIds = new Set(Array.isArray(options.forceHideNodeIds) ? options.forceHideNodeIds : []);
    const forceShowNodeIds = new Set(Array.isArray(options.forceShowNodeIds) ? options.forceShowNodeIds : []);
    hiddenCountInCurrentView = hiddenNodeIds.size;
    if (!hiddenCountInCurrentView) {
      hiddenPreviewOpen = false;
    }

    const visibleNodeIds = new Set();
    const nodeEls = [...flowNodes.querySelectorAll(".node")];

    nodeEls.forEach((el) => {
      const id = String(el.dataset.id || "").trim();
      if (!id) return;
      const isHidden = hiddenNodeIds.has(id);
      const visible = forceHideNodeIds.has(id)
        ? false
        : forceShowNodeIds.has(id)
          ? true
          : (!isHidden || hiddenPreviewOpen);
      el.dataset.isUserHidden = isHidden ? "1" : "0";
      el.classList.toggle("user-hidden-node", isHidden);
      el.classList.toggle("user-hidden-node-visible", isHidden && hiddenPreviewOpen);
      el.style.display = visible ? "" : "none";
      if (visible) {
        visibleNodeIds.add(id);
      }
    });

    flowNodes.querySelectorAll(".node-extra-text").forEach((textEl) => {
      const id = String(textEl.dataset.id || "").trim();
      textEl.style.display = visibleNodeIds.has(id) ? "" : "none";
    });

    const visibleEdges = edges.filter((e) => visibleNodeIds.has(e.source) && visibleNodeIds.has(e.target));
    return renderEdgesFromRects(visibleEdges, (id) => {
      if (!visibleNodeIds.has(id)) return null;
      const p = nodePos.get(id);
      if (!p) return null;
      const s = nodeSize(id);
      return { x: p.x, y: p.y, w: s.w, h: s.h };
    });
  }

  async function toggleHiddenFromContextMenu() {
    if (!contextMenuPayload) return;

    const bwObject = normalizeHiddenObject(contextMenuPayload.bwObject);
    const sourcesys = normalizeHiddenSource(contextMenuPayload.sourcesys);
    const nodeId = String(contextMenuPayload.nodeId || "").trim();
    const key = buildHiddenKey(bwObject, sourcesys);
    if (!key) {
      hideNodeContextMenu();
      return;
    }

    const currentlyHidden = hiddenObjectKeys.has(key);
    try {
      if (currentlyHidden) {
        await removeHiddenObject(bwObject, sourcesys);
        hiddenObjectKeys.delete(key);
      } else {
        await addHiddenObject(bwObject, sourcesys);
        hiddenObjectKeys.add(key);
      }
    } catch (error) {
      showFlowToast(`操作失败：${normalizeApiErrorMessage(error)}`);
      hideNodeContextMenu();
      return;
    }

    hideNodeContextMenu();
    if (lastRenderContext?.hiddenNodeIds) {
      if (currentlyHidden) {
        lastRenderContext.hiddenNodeIds.delete(nodeId);
        applyHiddenVisibilityToRenderedGraph({ forceShowNodeIds: nodeId ? [nodeId] : [] });
      } else {
        lastRenderContext.hiddenNodeIds.add(nodeId);
        applyHiddenVisibilityToRenderedGraph({ forceHideNodeIds: nodeId ? [nodeId] : [] });
      }
      updateHiddenToggleButtonState();
    } else {
      buildGraphView(selectedTypes);
    }
    showFlowToast(currentlyHidden ? "已显示对象" : "已隐藏对象");
  }

  function ensureNodeContextMenu() {
    if (nodeContextMenu) return;

    nodeContextMenu = document.createElement("div");
    nodeContextMenu.className = "node-context-menu hidden";
    nodeContextMenu.innerHTML = [
      '<button type="button" class="node-context-menu-item" data-action="toggle-hidden"></button>',
      '<button type="button" class="node-context-menu-item" data-action="copy-both">复制技术名和名称</button>',
      '<button type="button" class="node-context-menu-item" data-action="copy-tech">复制技术名</button>',
      '<button type="button" class="node-context-menu-item" data-action="copy-name">复制名称</button>'
    ].join("");
    document.body.appendChild(nodeContextMenu);

    nodeContextMenu.addEventListener("click", (event) => {
      const btn = event.target.closest(".node-context-menu-item");
      if (!btn) return;
      const action = btn.dataset.action;
      if (action === "toggle-hidden") {
        toggleHiddenFromContextMenu();
      } else if (action === "copy-tech") {
        copyNodeContextPayload("tech");
      } else if (action === "copy-name") {
        copyNodeContextPayload("name");
      } else {
        copyNodeContextPayload("both");
      }
    });

    document.addEventListener("click", (event) => {
      if (!nodeContextMenu || nodeContextMenu.classList.contains("hidden")) return;
      if (nodeContextMenu.contains(event.target)) return;
      hideNodeContextMenu();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") hideNodeContextMenu();
    });
    window.addEventListener("scroll", hideNodeContextMenu, true);
    window.addEventListener("resize", hideNodeContextMenu);
  }

  function showNodeContextMenu(clientX, clientY, payload) {
    ensureNodeContextMenu();
    if (!nodeContextMenu) return;
    contextMenuPayload = payload;

    const toggleBtn = nodeContextMenu.querySelector('[data-action="toggle-hidden"]');
    if (toggleBtn) {
      const key = buildHiddenKey(payload?.bwObject, payload?.sourcesys);
      const isHidden = key ? hiddenObjectKeys.has(key) : false;
      toggleBtn.textContent = isHidden ? "显示" : "隐藏";
      toggleBtn.style.display = key ? "block" : "none";
    }

    nodeContextMenu.classList.remove("hidden");

    const rect = nodeContextMenu.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const left = Math.max(8, Math.min(clientX, vw - rect.width - 8));
    const top = Math.max(8, Math.min(clientY, vh - rect.height - 8));
    nodeContextMenu.style.left = `${left}px`;
    nodeContextMenu.style.top = `${top}px`;
  }

  function measureHiddenElementHeight(el) {
    if (!el) return 14;
    const direct = el.offsetHeight;
    if (direct > 0) return direct;

    // If element is hidden by current UI state, force a non-visible layout pass.
    const prevDisplay = el.style.display;
    const prevVisibility = el.style.visibility;
    el.style.display = "block";
    el.style.visibility = "hidden";
    const measured = el.offsetHeight;
    el.style.display = prevDisplay;
    el.style.visibility = prevVisibility;
    return measured || 14;
  }

  function normalizeTypeCategory(rawType) {
    const code = String(rawType || "").trim().toUpperCase();
    const byCode = {
      RSDS: "datasource",
      TRCS: "infosource",
      ADSO: "adso",
      IOBJ: "adso",
      HCPR: "cp",
      ELEM: "report",
      DEST: "report"
    };
    if (byCode[code]) return byCode[code];

    const byLegacy = {
      datasource: "datasource",
      infosource: "infosource",
      adso: "adso",
      cp: "cp",
      report: "report"
    };
    const legacy = String(rawType || "").trim().toLowerCase();
    return byLegacy[legacy] || "report";
  }

  function getLayoutProfile(mode) {
    const normalized = String(mode || "").trim().toLowerCase();
    if (normalized === "upstream") {
      return {
        id: "layout-upstream",
        levelSorter: (a, b) => b - a,
        enforceForwardPrecedence: true,
        useBackboneColumns: true,
        useCrossingOptimizers: false
      };
    }

    if (normalized === "downstream") {
      return {
        id: "layout-downstream",
        levelSorter: (a, b) => b - a,
        enforceForwardPrecedence: false,
        useBackboneColumns: false,
        useCrossingOptimizers: true
      };
    }

    if (normalized === "both") {
      return {
        id: "layout-both",
        levelSorter: (a, b) => b - a,
        enforceForwardPrecedence: false,
        useBackboneColumns: false,
        useCrossingOptimizers: true
      };
    }

    if (normalized === "full") {
      return {
        id: "layout-full",
        // Full mode uses its own layout profile.
        levelSorter: (a, b) => b - a,
        enforceForwardPrecedence: false,
        useBackboneColumns: false,
        useCrossingOptimizers: true
      };
    }

    return {
      id: "layout-generic",
      levelSorter: (a, b) => a - b,
      enforceForwardPrecedence: false,
      useBackboneColumns: false,
      useCrossingOptimizers: true
    };
  }

  function resolveNodeColor(node) {
    const code = String(node?.type || "").trim().toUpperCase();
    if (code && graphColorMap[code]) {
      return graphColorMap[code];
    }
    const category = normalizeTypeCategory(node?.type);
    return graphColorMap[category] || MockData.colorMap[category] || "#9cb4ff";
  }

  async function loadGraphData(startName, mode, startSource = "", startType = "") {
    const start = String(startName || "").trim();
    const m = String(mode || "").trim().toLowerCase();

    if (!start) {
      graphData = { nodes: [], edges: [] };
      graphColorMap = MockData.colorMap || {};
      return { effectiveMode: m || "both", downgraded: false, usedMock: false, noStart: true };
    }

    let effectiveMode = m || "downstream";
    let downgraded = false;
    if (!["downstream", "upstream", "both", "full"].includes(effectiveMode)) {
      effectiveMode = "downstream";
      downgraded = true;
    }

    let resp;
    try {
      const query = new URLSearchParams({
        start_name: start,
        mode: effectiveMode
      });
      if (String(startSource || "").trim()) query.set("start_source", String(startSource || "").trim());
      if (String(startType || "").trim()) query.set("start_type", String(startType || "").trim());

      resp = await fetch(`${flowApiBase}/flow/trace?${query.toString()}`, {
        credentials: "include"
      });
    } catch (networkErr) {
      const err = new Error("network_error");
      err.code = "network_error";
      err.cause = networkErr;
      throw err;
    }

    if (!resp.ok) {
      let detail = "";
      try {
        const payload = await resp.json();
        detail = String(payload?.detail || "").trim();
      } catch {
        try {
          detail = String(await resp.text() || "").trim();
        } catch {
          detail = "";
        }
      }
      const err = new Error(`status ${resp.status}${detail ? `: ${detail}` : ""}`);
      err.status = resp.status;
      err.detail = detail;
      throw err;
    }
    const payload = await resp.json();
    graphData = payload?.graph || { nodes: [], edges: [] };
    graphColorMap = payload?.graph?.color_map || {};
    const resolvedStartName = String(payload?.resolved_start_name || "").trim();
    return { effectiveMode, downgraded, usedMock: false, resolvedStartName };
  }

  function edgeDefsMarkup() {
    return `
      <defs>
        <marker id="flowEdgeArrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse" markerUnits="strokeWidth">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--edge-color)" />
        </marker>
      </defs>
    `;
  }

  function renderEdgesFromRects(edges, rectById) {
    flowEdges.innerHTML = edgeDefsMarkup();
    edgeRefsByNode = new Map();

    const addEdgeRef = (nodeId, ref) => {
      if (!edgeRefsByNode.has(nodeId)) {
        edgeRefsByNode.set(nodeId, []);
      }
      edgeRefsByNode.get(nodeId).push(ref);
    };

    const directedEdgeSet = new Set(edges.map((e) => `${e.source}=>${e.target}`));
    const directedEdgeCount = new Map();
    edges.forEach((e) => {
      const key = `${e.source}=>${e.target}`;
      directedEdgeCount.set(key, (directedEdgeCount.get(key) || 0) + 1);
    });
    const directedEdgeOrdinal = new Map();
    let renderedEdgeCount = 0;

    edges.forEach((edge) => {
      const s = rectById(edge.source);
      const t = rectById(edge.target);
      if (!s || !t) return;

      const directedKey = `${edge.source}=>${edge.target}`;
      const ordinal = directedEdgeOrdinal.get(directedKey) || 0;
      directedEdgeOrdinal.set(directedKey, ordinal + 1);
      const sameDirTotal = directedEdgeCount.get(directedKey) || 1;
      const sameDirShift = (ordinal - (sameDirTotal - 1) / 2) * 8;

      if (edge.source === edge.target) {
        const startX = s.x + s.w * 0.7 + sameDirShift * 0.25;
        const startY = s.y - 2;
        const endX = s.x + s.w * 0.7 + sameDirShift * 0.25;
        const endY = s.y + s.h + 2;
        const baseSpan = Math.abs(endY - startY);
        const rx = Math.max(baseSpan * 0.81, 27);
        const ry = Math.max(baseSpan * 0.738, baseSpan / 2 + 1);
        const loop = document.createElementNS("http://www.w3.org/2000/svg", "path");
        loop.setAttribute("d", `M ${startX} ${startY} A ${rx} ${ry} 0 1 1 ${endX} ${endY}`);
        const pivotX = s.x + s.w * 0.7;
        const pivotY = s.y + s.h / 2;
        loop.setAttribute("transform", `rotate(-7 ${pivotX} ${pivotY})`);
        loop.setAttribute("fill", "none");
        loop.setAttribute("stroke", "var(--edge-color)");
        loop.setAttribute("stroke-width", "1.3");
        loop.setAttribute("stroke-linecap", "round");
        loop.setAttribute("marker-end", "url(#flowEdgeArrow)");
        loop.classList.add("flow-edge");
        flowEdges.appendChild(loop);
        addEdgeRef(edge.source, { el: loop, source: edge.source, target: edge.target });
        renderedEdgeCount += 1;
        return;
      }

      const fromCenterX = s.x + s.w / 2;
      const toCenterX = t.x + t.w / 2;
      const sourceMidY = s.y + s.h / 2;
      const targetMidY = t.y + t.h / 2;

      let fromCenterY = s.y - 1;
      let toCenterY = t.y + t.h + 1;
      if (sourceMidY < targetMidY - 0.5) {
        fromCenterY = s.y + s.h + 1;
        toCenterY = t.y - 1;
      } else if (Math.abs(sourceMidY - targetMidY) <= 0.5) {
        fromCenterY = sourceMidY;
        toCenterY = targetMidY;
      }

      const dx = toCenterX - fromCenterX;
      const dy = toCenterY - fromCenterY;
      const dist = Math.hypot(dx, dy) || 1;
      const ux = dx / dist;
      const uy = dy / dist;
      const nx = -uy;
      const ny = ux;

      const hasReverse = directedEdgeSet.has(`${edge.target}=>${edge.source}`);
      const pairShift = hasReverse ? 10 : 0;
      const laneShift = pairShift + sameDirShift;

      const x1 = fromCenterX + nx * laneShift;
      const y1 = fromCenterY + ny * laneShift;
      const x2 = toCenterX + nx * laneShift;
      const y2 = toCenterY + ny * laneShift;

      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", String(x1));
      line.setAttribute("y1", String(y1));
      line.setAttribute("x2", String(x2));
      line.setAttribute("y2", String(y2));
      line.setAttribute("stroke", "var(--edge-color)");
      line.setAttribute("stroke-width", "1.3");
      line.setAttribute("stroke-linecap", "round");
      line.setAttribute("marker-end", "url(#flowEdgeArrow)");
      line.classList.add("flow-edge");
      flowEdges.appendChild(line);
      const ref = { el: line, source: edge.source, target: edge.target };
      addEdgeRef(edge.source, ref);
      addEdgeRef(edge.target, ref);
      renderedEdgeCount += 1;
    });

    applyFlowHighlightState(false);

    return renderedEdgeCount;
  }

  function collectNodeRectsFromDom() {
    const rects = new Map();
    flowNodes.querySelectorAll(".node").forEach((el) => {
      const id = el.dataset.id;
      if (!id) return;
      rects.set(id, {
        x: Number.parseFloat(el.style.left || "0") || 0,
        y: Number.parseFloat(el.style.top || "0") || 0,
        w: el.offsetWidth || 110,
        h: el.offsetHeight || 34
      });
    });
    return rects;
  }

  function refreshNodeExtraTextPositions(rects) {
    flowNodes.querySelectorAll(".node-extra-text").forEach((tag) => {
      const id = tag.dataset.id;
      if (!id) return;
      const r = rects.get(id);
      if (!r) return;
      tag.style.left = `${r.x + r.w / 2}px`;
      const textHeight = measureHiddenElementHeight(tag);
      tag.style.top = `${r.y - textHeight}px`;
    });
  }

  function syncGraphAfterManualDrag(redrawMiniMap = false) {
    const rects = collectNodeRectsFromDom();
    renderEdgesFromRects(activeRenderEdges, (id) => rects.get(id));
    refreshNodeExtraTextPositions(rects);
    if (redrawMiniMap) {
      renderMiniMap();
    } else {
      updateMiniMapViewport();
    }
  }

  function clearFlowSettleTimer() {
    if (!flowSettleTimer) return;
    window.clearTimeout(flowSettleTimer);
    flowSettleTimer = null;
  }

  function clearFlowEdgeEffects() {
    flowEdges.querySelectorAll(".flow-edge").forEach((edgeEl) => {
      edgeEl.classList.remove("is-flowing", "is-flow-glow");
      edgeEl.style.removeProperty("--flow-shift");
    });
  }

  function clearFlowNodeEffects() {
    flowNodes.querySelectorAll(".node").forEach((nodeEl) => {
      nodeEl.classList.remove("active", "flow-active");
    });
  }

  function applyFlowHighlightState(playFlow = true) {
    clearFlowSettleTimer();
    clearFlowEdgeEffects();
    clearFlowNodeEffects();

    if (!activeFlowNodeId) return;
    const targetNode = flowNodes.querySelector(`.node[data-id="${CSS.escape(activeFlowNodeId)}"]`);
    if (!targetNode) {
      activeFlowNodeId = "";
      return;
    }

    targetNode.classList.add("active", "flow-active");
    const linked = edgeRefsByNode.get(activeFlowNodeId) || [];
    if (!linked.length) return;

    if (!playFlow) {
      linked.forEach((ref) => {
        ref.el.classList.add("is-flow-glow");
      });
      return;
    }

    linked.forEach((ref) => {
      const isOutward = ref.source === activeFlowNodeId;
      ref.el.style.setProperty("--flow-shift", isOutward ? "-38px" : "38px");
      ref.el.classList.add("is-flowing");
    });

    flowSettleTimer = window.setTimeout(() => {
      if (!activeFlowNodeId) return;
      const currentLinked = edgeRefsByNode.get(activeFlowNodeId) || [];
      currentLinked.forEach((ref) => {
        ref.el.classList.remove("is-flowing");
        ref.el.classList.add("is-flow-glow");
      });
      flowSettleTimer = null;
    }, FLOW_SETTLE_MS);
  }

  function toggleNodeFlowHighlight(nodeId) {
    if (!nodeId) return;
    if (activeFlowNodeId === nodeId) {
      activeFlowNodeId = "";
      clearFlowSettleTimer();
      clearFlowEdgeEffects();
      clearFlowNodeEffects();
      return;
    }
    activeFlowNodeId = nodeId;
    applyFlowHighlightState(true);
  }

  function buildGraphView(types) {
    renderRevision += 1;
    const activeRevision = renderRevision;

    const activeTypes = new Set(types.length ? types : ["datasource", "infosource", "adso", "cp", "report"]);
    const sourceNodes = Array.isArray(graphData?.nodes) ? graphData.nodes : [];
    const sourceEdges = Array.isArray(graphData?.edges) ? graphData.edges : [];
    const nodes = sourceNodes.filter((n) => activeTypes.has(normalizeTypeCategory(n.type)));
    const nodeSet = new Set(nodes.map((n) => n.id));
    const edges = sourceEdges.filter((e) => nodeSet.has(e.source) && nodeSet.has(e.target));
    activeRenderEdges = edges.slice();
    const elkSignature = getElkLayoutSignature(nodes, edges);

    flowNodes.innerHTML = "";
    flowEdges.innerHTML = edgeDefsMarkup();
    clearFlowStatusOverlay();

    const topPadding = 44;
    const bottomPadding = 44;
    const sidePadding = 70;
    let graphWidth = GRAPH_WIDTH;
    let graphHeight = GRAPH_HEIGHT;
    let centerX = graphWidth / 2 - 55;
    const NODE_W = 110;
    const NODE_H = 34;
    const effectiveL1 = getEffectiveL1();
    const sideBaseGap = NODE_W + effectiveL1;
    const sideStepGap = NODE_W + effectiveL1;

    const nodeMeta = nodes.map((node) => ({
      node,
      iconKind: resolveNodeIconKind(node),
      rawLevel: Number.isFinite(Number(node.level)) ? Number(node.level) : 999
    }));

    // Pre-layout width estimate so spacing can follow edge-to-edge L1 even before DOM render.
    const estimatedSizeById = new Map(
      nodeMeta.map((m) => {
        const label = String(m.node?.label || m.node?.id || "");
        const textW = label.length * 7.2;
        const iconPad = 24;
        const basePad = 20;
        const w = Math.max(110, Math.round(textW + iconPad + basePad));
        return [m.node.id, { w, h: NODE_H }];
      })
    );
    const layoutSize = (id) => estimatedSizeById.get(id) || { w: NODE_W, h: NODE_H };

    const rawLevelById = new Map(nodeMeta.map((m) => [m.node.id, m.rawLevel]));

    // Mode-specific layout profile keeps each trace mode on an independent layout path.
    const layoutProfile = getLayoutProfile(currentTraceMode);
    const isDownstreamLayout = layoutProfile.useBackboneColumns;
    const levelSorter = layoutProfile.levelSorter;
    const collisionEnabledModes = new Set(["upstream", "downstream", "both", "full"]);
    const collisionAvoidanceEnabled = collisionEnabledModes.has(currentTraceMode);

    // Downstream layout must respect all precedence links: if A -> B, B cannot stay
    // on the same/lower visual layer than A (except explicit backward/reference links).
    if (layoutProfile.enforceForwardPrecedence) {
      const visualLevelById = new Map(nodeMeta.map((m) => [m.node.id, m.rawLevel]));
      const maxPass = Math.max(1, nodeMeta.length * 2);

      for (let pass = 0; pass < maxPass; pass += 1) {
        let changed = false;

        edges.forEach((edge) => {
          if (edge.source === edge.target) return;

          const rawSrc = rawLevelById.get(edge.source);
          const rawTgt = rawLevelById.get(edge.target);
          if (!Number.isFinite(rawSrc) || !Number.isFinite(rawTgt)) return;

          // Keep backend-defined backward edges from pulling layers upward forever.
          if (rawTgt < rawSrc) return;

          const srcLv = visualLevelById.get(edge.source);
          const tgtLv = visualLevelById.get(edge.target);
          const wanted = srcLv + 1;
          if (wanted > tgtLv) {
            visualLevelById.set(edge.target, wanted);
            changed = true;
          }
        });

        if (!changed) break;
      }

      nodeMeta.forEach((meta) => {
        meta.visualLevel = visualLevelById.get(meta.node.id);
      });
    } else {
      nodeMeta.forEach((meta) => {
        meta.visualLevel = meta.rawLevel;
      });
    }

    // Full-mode rule: datasource nodes must stay on the bottom layer.
    if (currentTraceMode === "full") {
      const levelValues = nodeMeta
        .map((m) => Number(m.visualLevel))
        .filter((v) => Number.isFinite(v));
      if (levelValues.length) {
        const minLevel = Math.min(...levelValues);
        const datasourceLevel = minLevel - 1;
        nodeMeta.forEach((meta) => {
          if (normalizeTypeCategory(meta.node?.type) === "datasource") {
            meta.visualLevel = datasourceLevel;
          }
        });
      }
    }

    const allLevels = [...new Set(nodeMeta.map((m) => m.visualLevel))].sort(levelSorter);
    if (!allLevels.length) {
      applyGraphDimensions(GRAPH_WIDTH, GRAPH_HEIGHT);
      if (!sourceNodes.length) {
        showFlowStatusOverlay("未获取到数据流图数据。请先登录，并确认起点对象存在关联关系。", { variant: "warning" });
      } else {
        showFlowStatusOverlay("当前筛选条件下没有可见节点，请在“数据流图选择”中调整类型。", { variant: "info" });
      }
      return;
    }
    const levelCount = allLevels.length;
    const levelIndexByValue = new Map(allLevels.map((lv, idx) => [lv, idx]));

    const nodeById = new Map(nodeMeta.map((m) => [m.node.id, m]));
    const outAdj = new Map();
    const inAdj = new Map();
    const undirectedAdj = new Map();

    const ensure = (map, key) => {
      if (!map.has(key)) map.set(key, new Set());
      return map.get(key);
    };

    nodeMeta.forEach((m) => {
      ensure(outAdj, m.node.id);
      ensure(inAdj, m.node.id);
      ensure(undirectedAdj, m.node.id);
    });

    edges.forEach((e) => {
      ensure(outAdj, e.source).add(e.target);
      ensure(inAdj, e.target).add(e.source);
      if (e.source !== e.target) {
        ensure(undirectedAdj, e.source).add(e.target);
        ensure(undirectedAdj, e.target).add(e.source);
      }
    });

    const resolveSeedId = () => {
      const seed = normalizeNodeName(focusStartName);
      if (!seed) return nodeMeta[0]?.node.id || "";
      for (const meta of nodeMeta) {
        const nodeId = normalizeNodeName(meta.node.id);
        const label = normalizeNodeName(meta.node.label);
        if (nodeId === seed || label === seed) return meta.node.id;
      }
      return nodeMeta[0]?.node.id || "";
    };

    const seedId = resolveSeedId();
    const nodeName = (id) => String(nodeById.get(id)?.node.label || id || "");
    const hasSuffix = (id, suffix) => nodeName(id).toUpperCase().endsWith(suffix);
    const trunkTransitionScore = (fromId, toId) => {
      if (hasSuffix(fromId, "_OUT") && hasSuffix(toId, "_IN")) return 6;
      if (hasSuffix(fromId, "_IN") && hasSuffix(toId, "_OUT")) return 5;
      if (hasSuffix(toId, "_OUT") || hasSuffix(toId, "_IN")) return 3;
      return 0;
    };

    const longestMemo = new Map();
    const computeLongestPath = (id, stack) => {
      if (longestMemo.has(id)) return longestMemo.get(id);
      if (stack.has(id)) return 0;
      stack.add(id);
      let best = 0;
      const nextSet = outAdj.get(id) || new Set();
      nextSet.forEach((nextId) => {
        if (nextId === id) return;
        best = Math.max(best, 1 + computeLongestPath(nextId, stack));
      });
      stack.delete(id);
      longestMemo.set(id, best);
      return best;
    };

    const outDegree = (id) => (outAdj.get(id) ? outAdj.get(id).size : 0);

    const backbone = [];
    const backboneSet = new Set();
    if (seedId) {
      let cursor = seedId;
      while (cursor && !backboneSet.has(cursor)) {
        backbone.push(cursor);
        backboneSet.add(cursor);
        const nextCandidates = [...(outAdj.get(cursor) || [])].filter((id) => id !== cursor && !backboneSet.has(id));
        if (!nextCandidates.length) break;
        nextCandidates.sort((a, b) => {
          const trunkDiff = trunkTransitionScore(cursor, b) - trunkTransitionScore(cursor, a);
          if (trunkDiff !== 0) return trunkDiff;
          const longestDiff = computeLongestPath(b, new Set()) - computeLongestPath(a, new Set());
          if (longestDiff !== 0) return longestDiff;
          const degreeDiff = outDegree(b) - outDegree(a);
          if (degreeDiff !== 0) return degreeDiff;
          return String(nodeById.get(a)?.node.label || a).localeCompare(String(nodeById.get(b)?.node.label || b));
        });
        cursor = nextCandidates[0];
      }
    }

    if (!backbone.length && nodeMeta.length) {
      backbone.push(nodeMeta[0].node.id);
      backboneSet.add(nodeMeta[0].node.id);
    }

    const backboneIndex = new Map(backbone.map((id, idx) => [id, idx]));

    const nearestBackboneInfo = (startId) => {
      const seen = new Set([startId]);
      const queue = [{ id: startId, dist: 0 }];
      while (queue.length) {
        const current = queue.shift();
        if (backboneSet.has(current.id)) {
          return { anchor: current.id, dist: current.dist };
        }
        const neighbors = undirectedAdj.get(current.id) || new Set();
        neighbors.forEach((nextId) => {
          if (seen.has(nextId)) return;
          seen.add(nextId);
          queue.push({ id: nextId, dist: current.dist + 1 });
        });
      }
      return { anchor: backbone[0], dist: 1 };
    };

    const sideByNode = new Map();
    const distByNode = new Map();
    const anchorByNode = new Map();

    const anchorGroups = new Map();
    nodeMeta.forEach((meta) => {
      const id = meta.node.id;
      const info = nearestBackboneInfo(id);
      const dist = Math.max(0, info.dist);
      anchorByNode.set(id, info.anchor);
      distByNode.set(id, dist);
      if (!anchorGroups.has(info.anchor)) anchorGroups.set(info.anchor, []);
      if (!backboneSet.has(id)) {
        anchorGroups.get(info.anchor).push(id);
      }
    });

    backbone.forEach((id) => sideByNode.set(id, 0));
    anchorGroups.forEach((ids) => {
      const sorted = [...ids].sort((a, b) => String(nodeById.get(a)?.node.label || a).localeCompare(String(nodeById.get(b)?.node.label || b)));
      sorted.forEach((id, idx) => {
        sideByNode.set(id, idx % 2 === 0 ? -1 : 1);
      });
    });

    const nodePos = new Map();
    const hardLockX = new Map();
    const visualLevelByNode = new Map();
    const groupedByLevel = new Map();
    const nodeMetaById = new Map(nodeMeta.map((m) => [m.node.id, m]));
    nodeMeta.forEach((meta) => {
      visualLevelByNode.set(meta.node.id, meta.visualLevel);
      if (!groupedByLevel.has(meta.visualLevel)) groupedByLevel.set(meta.visualLevel, []);
      groupedByLevel.get(meta.visualLevel).push(meta);
    });

    // Expand graph width dynamically to avoid horizontal compression on large graphs.
    const maxNodesInLayer = Math.max(1, ...[...groupedByLevel.values()].map((layer) => layer.length));
    const desiredWidth = sidePadding * 2 + maxNodesInLayer * (NODE_W + effectiveL1) + 220;
    graphWidth = Math.max(GRAPH_WIDTH, desiredWidth);
    centerX = graphWidth / 2 - 55;
    applyGraphDimensions(graphWidth, graphHeight);

    const baseNodeOrder = (aId, bId) => {
      const aBackbone = backboneIndex.has(aId) ? 0 : 1;
      const bBackbone = backboneIndex.has(bId) ? 0 : 1;
      if (aBackbone !== bBackbone) return aBackbone - bBackbone;

      const aMainType = (hasSuffix(aId, "_OUT") || hasSuffix(aId, "_IN")) ? 0 : 1;
      const bMainType = (hasSuffix(bId, "_OUT") || hasSuffix(bId, "_IN")) ? 0 : 1;
      if (aMainType !== bMainType) return aMainType - bMainType;

      const sideDiff = (sideByNode.get(aId) || 0) - (sideByNode.get(bId) || 0);
      if (sideDiff !== 0) return sideDiff;

      const depthDiff = (distByNode.get(aId) || 0) - (distByNode.get(bId) || 0);
      if (depthDiff !== 0) return depthDiff;

      return String(nodeMetaById.get(aId)?.node.label || aId).localeCompare(String(nodeMetaById.get(bId)?.node.label || bId));
    };

    const layerNodeIds = new Map();
    allLevels.forEach((lv) => {
      const ids = (groupedByLevel.get(lv) || []).map((m) => m.node.id).sort(baseNodeOrder);
      layerNodeIds.set(lv, ids);
    });

    const orderByNode = new Map();
    layerNodeIds.forEach((ids) => {
      ids.forEach((id, idx) => orderByNode.set(id, idx));
    });

    // Reorder each layer by neighbor barycenter to reduce crossings while preserving trunk priority.
    const allLevelsReversed = [...allLevels].reverse();
    const barycenterFor = (id) => {
      const refs = [];
      (inAdj.get(id) || new Set()).forEach((pid) => {
        if (visualLevelByNode.get(pid) !== visualLevelByNode.get(id) && orderByNode.has(pid)) {
          refs.push(orderByNode.get(pid));
        }
      });
      (outAdj.get(id) || new Set()).forEach((cid) => {
        if (visualLevelByNode.get(cid) !== visualLevelByNode.get(id) && orderByNode.has(cid)) {
          refs.push(orderByNode.get(cid));
        }
      });
      if (!refs.length) return Number.POSITIVE_INFINITY;
      return refs.reduce((a, b) => a + b, 0) / refs.length;
    };

    for (let pass = 0; pass < 4; pass += 1) {
      const levelSeq = pass % 2 === 0 ? allLevels : allLevelsReversed;
      levelSeq.forEach((lv) => {
        const ids = [...(layerNodeIds.get(lv) || [])];
        ids.sort((a, b) => {
          const baseDiff = baseNodeOrder(a, b);
          if (baseDiff !== 0 && (backboneSet.has(a) || backboneSet.has(b))) return baseDiff;

          const aBar = barycenterFor(a);
          const bBar = barycenterFor(b);
          const aFinite = Number.isFinite(aBar);
          const bFinite = Number.isFinite(bBar);
          if (aFinite && bFinite && Math.abs(aBar - bBar) > 1e-6) {
            return aBar - bBar;
          }
          if (aFinite !== bFinite) return aFinite ? -1 : 1;
          return baseDiff;
        });
        layerNodeIds.set(lv, ids);
        ids.forEach((id, idx) => orderByNode.set(id, idx));
      });
    }

    allLevels.forEach((lv) => {
      const layerNodes = groupedByLevel.get(lv) || [];
      const layerIdx = levelIndexByValue.get(lv) ?? 0;
      const y = levelCount === 1
        ? graphHeight / 2
        : topPadding + ((graphHeight - topPadding - bottomPadding) * layerIdx) / (levelCount - 1);

      const sortedNodes = (layerNodeIds.get(lv) || layerNodes.map((m) => m.node.id))
        .map((id) => nodeMetaById.get(id))
        .filter(Boolean);

      const laneOrderByNode = new Map();
      if (isDownstreamLayout) {
        const lanes = new Map();
        sortedNodes.forEach((meta) => {
          const id = meta.node.id;
          const key = backboneSet.has(id)
            ? "B"
            : `${sideByNode.get(id) || 1}:${distByNode.get(id) || 0}`;
          if (!lanes.has(key)) lanes.set(key, []);
          lanes.get(key).push(id);
        });

        lanes.forEach((ids) => {
          ids.forEach((id, idx) => {
            laneOrderByNode.set(id, { idx, count: ids.length });
          });
        });
      }

      sortedNodes.forEach((meta) => {
        const id = meta.node.id;
        const sign = sideByNode.get(id) || 0;
        const dist = distByNode.get(id) || 0;
        const anchorId = anchorByNode.get(id);

        let x = centerX;
        if (isDownstreamLayout) {
          if (backboneSet.has(id)) {
            x = centerX;
          } else {
            const offset = sideBaseGap + Math.max(0, dist - 1) * sideStepGap;
            x = centerX + (sign || 1) * offset;

            // Keep branch nodes near their anchor's column when levels are sparse.
            if (anchorId && backboneSet.has(anchorId) && dist === 1) {
              x = centerX + (sign || 1) * sideBaseGap;
            }
          }

          const laneOrder = laneOrderByNode.get(id);
          if (laneOrder && laneOrder.count > 1) {
            const spread = 110;
            x += (laneOrder.idx - (laneOrder.count - 1) / 2) * spread;
          }
        } else {
          const count = sortedNodes.length;
          const idx = sortedNodes.findIndex((n) => n.node.id === id);
          x = count === 1
            ? centerX
            : sidePadding + ((graphWidth - sidePadding * 2) * idx) / (count - 1) - 55;
        }

        x = Math.max(sidePadding - 55, Math.min(graphWidth - sidePadding - 55, x));
        nodePos.set(id, { x, y: y - 17, node: meta.node });
      });
    });

    const clampNodeX = (x, width = NODE_W) => {
      const half = width / 2;
      return Math.max(sidePadding - half, Math.min(graphWidth - sidePadding - half, x));
    };
    const clampNodeY = (y, height = NODE_H) => {
      const top = topPadding - height / 2;
      const bottom = graphHeight - bottomPadding - height / 2;
      return Math.max(top, Math.min(bottom, y));
    };

    if (collisionAvoidanceEnabled) {
      // Global rectangle collision pass to keep dense layers readable.
      const fullIds = [...nodePos.keys()];
      const maxPasses = currentTraceMode === "full" ? 18 : 10;
      const minGap = Math.max(12, Math.round(getEffectiveL1() * (currentTraceMode === "full" ? 0.2 : 0.14)));

      for (let pass = 0; pass < maxPasses; pass += 1) {
        let moved = false;

        for (let i = 0; i < fullIds.length; i += 1) {
          const aId = fullIds[i];
          const aPos = nodePos.get(aId);
          if (!aPos) continue;
          const aSize = layoutSize(aId);
          const aCx = aPos.x + aSize.w / 2;
          const aCy = aPos.y + aSize.h / 2;

          for (let j = i + 1; j < fullIds.length; j += 1) {
            const bId = fullIds[j];
            const bPos = nodePos.get(bId);
            if (!bPos) continue;
            const bSize = layoutSize(bId);
            const bCx = bPos.x + bSize.w / 2;
            const bCy = bPos.y + bSize.h / 2;

            const overlapX = aSize.w / 2 + bSize.w / 2 + minGap - Math.abs(aCx - bCx);
            const overlapY = aSize.h / 2 + bSize.h / 2 + minGap - Math.abs(aCy - bCy);
            if (overlapX <= 0 || overlapY <= 0) continue;

            const dxSign = aCx >= bCx ? 1 : -1;
            const dySign = aCy >= bCy ? 1 : -1;

            if (overlapX < overlapY) {
              const shift = overlapX / 2 + 0.5;
              aPos.x = clampNodeX(aPos.x + dxSign * shift, aSize.w);
              bPos.x = clampNodeX(bPos.x - dxSign * shift, bSize.w);
            } else {
              const shift = overlapY / 2 + 0.5;
              aPos.y = clampNodeY(aPos.y + dySign * shift, aSize.h);
              bPos.y = clampNodeY(bPos.y - dySign * shift, bSize.h);
            }
            moved = true;
          }
        }

        if (!moved) break;
      }

      const maxRight = Math.max(
        0,
        ...fullIds.map((id) => {
          const p = nodePos.get(id);
          const s = layoutSize(id);
          return (p?.x || 0) + s.w;
        })
      );
      const maxBottom = Math.max(
        0,
        ...fullIds.map((id) => {
          const p = nodePos.get(id);
          const s = layoutSize(id);
          return (p?.y || 0) + s.h;
        })
      );

      const requiredW = Math.max(graphWidth, Math.ceil(maxRight + sidePadding));
      const requiredH = Math.max(graphHeight, Math.ceil(maxBottom + bottomPadding));
      if (requiredW > graphWidth || requiredH > graphHeight) {
        graphWidth = requiredW;
        graphHeight = requiredH;
        applyGraphDimensions(graphWidth, graphHeight);
      }
    }

    if (isDownstreamLayout) {
      const levelAsc = [...allLevels].reverse();
      const lockedLevels = new Set();
      if (levelAsc.length) lockedLevels.add(levelAsc[0]);

      const idsOfLevel = (lv) => (groupedByLevel.get(lv) || []).map((m) => m.node.id).filter((id) => nodePos.has(id));
      const sortByX = (ids) => ids.sort((a, b) => nodePos.get(a).x - nodePos.get(b).x);
      const applyLayerGap = (lv) => {
        const ids = sortByX(idsOfLevel(lv));
        const gap = effectiveL1;
        for (let i = 1; i < ids.length; i += 1) {
          const leftId = ids[i - 1];
          const rightId = ids[i];
          const left = nodePos.get(leftId);
          const right = nodePos.get(rightId);
          if (!left || !right) continue;
          const leftW = layoutSize(leftId).w;
          const minLeft = left.x + leftW + gap;
          if (right.x < minLeft) {
            right.x = clampNodeX(minLeft, layoutSize(rightId).w);
          }
        }
      };

      const centersByRule = (orderedIds, parentCenterX) => {
        const n = orderedIds.length;
        const centers = new Map();
        const gap = effectiveL1;
        if (!n) return centers;
        if (n === 1) {
          centers.set(orderedIds[0], parentCenterX);
          return centers;
        }

        if (n % 2 === 1) {
          const mid = Math.floor(n / 2);
          const midId = orderedIds[mid];
          centers.set(midId, parentCenterX);
          for (let i = mid - 1; i >= 0; i -= 1) {
            const rightId = orderedIds[i + 1];
            const curId = orderedIds[i];
            const rightC = centers.get(rightId);
            const rightW = layoutSize(rightId).w;
            const curW = layoutSize(curId).w;
            centers.set(curId, rightC - (rightW / 2 + curW / 2 + gap));
          }
          for (let i = mid + 1; i < n; i += 1) {
            const leftId = orderedIds[i - 1];
            const curId = orderedIds[i];
            const leftC = centers.get(leftId);
            const leftW = layoutSize(leftId).w;
            const curW = layoutSize(curId).w;
            centers.set(curId, leftC + (leftW / 2 + curW / 2 + gap));
          }
          return centers;
        }

        const lm = n / 2 - 1;
        const rm = n / 2;
        const leftMid = orderedIds[lm];
        const rightMid = orderedIds[rm];
        const leftMidW = layoutSize(leftMid).w;
        const rightMidW = layoutSize(rightMid).w;
        centers.set(leftMid, parentCenterX - gap / 2 - leftMidW / 2);
        centers.set(rightMid, parentCenterX + gap / 2 + rightMidW / 2);

        for (let i = lm - 1; i >= 0; i -= 1) {
          const rightId = orderedIds[i + 1];
          const curId = orderedIds[i];
          const rightC = centers.get(rightId);
          const rightW = layoutSize(rightId).w;
          const curW = layoutSize(curId).w;
          centers.set(curId, rightC - (rightW / 2 + curW / 2 + gap));
        }
        for (let i = rm + 1; i < n; i += 1) {
          const leftId = orderedIds[i - 1];
          const curId = orderedIds[i];
          const leftC = centers.get(leftId);
          const leftW = layoutSize(leftId).w;
          const curW = layoutSize(curId).w;
          centers.set(curId, leftC + (leftW / 2 + curW / 2 + gap));
        }
        return centers;
      };

      for (let idx = 0; idx < levelAsc.length - 1; idx += 1) {
        const lv = levelAsc[idx];
        const nextLv = levelAsc[idx + 1];
        if (lockedLevels.has(nextLv)) continue;

        const parents = idsOfLevel(lv);
        const proposals = new Map();
        const immediateInCount = new Map();
        const immediateOutCount = new Map();

        parents.forEach((pid) => {
          const parentPos = nodePos.get(pid);
          if (!parentPos) return;
          const targets = [...(outAdj.get(pid) || [])]
            .filter((cid) => nodePos.has(cid) && visualLevelByNode.get(cid) === nextLv);
          immediateOutCount.set(pid, targets.length);
          targets.forEach((cid) => {
            immediateInCount.set(cid, (immediateInCount.get(cid) || 0) + 1);
          });
          if (!targets.length) return;

          targets.sort((a, b) => {
            const xDiff = nodePos.get(a).x - nodePos.get(b).x;
            if (Math.abs(xDiff) > 0.001) return xDiff;
            return String(nodeMetaById.get(a)?.node.label || a).localeCompare(String(nodeMetaById.get(b)?.node.label || b));
          });
          const parentCenter = parentPos.x + layoutSize(pid).w / 2;
          const desiredCenters = centersByRule(targets, parentCenter);
          targets.forEach((cid) => {
            const desiredCenter = desiredCenters.get(cid) ?? parentCenter;
            if (!proposals.has(cid)) proposals.set(cid, []);
            proposals.get(cid).push(desiredCenter);
          });
        });

        idsOfLevel(nextLv).forEach((cid) => {
          const p = nodePos.get(cid);
          if (!p) return;
          const wanted = proposals.get(cid);
          if (wanted && wanted.length) {
            const avgCenter = wanted.reduce((a, b) => a + b, 0) / wanted.length;
            const w = layoutSize(cid).w;
            p.x = clampNodeX(avgCenter - w / 2, w);
          }
        });

        // Multi-source centroid rule on this immediate layer pair.
        idsOfLevel(nextLv).forEach((cid) => {
          const src = [...(inAdj.get(cid) || [])].filter((sid) => visualLevelByNode.get(sid) === lv && nodePos.has(sid));
          if (src.length < 2) return;
          const center = src
            .map((sid) => nodePos.get(sid).x + layoutSize(sid).w / 2)
            .reduce((a, b) => a + b, 0) / src.length;
          const w = layoutSize(cid).w;
          const desiredLeft = clampNodeX(center - w / 2, w);
          const p = nodePos.get(cid);
          if (!p) return;
          p.x = desiredLeft;
          hardLockX.set(cid, desiredLeft);
        });

        // Strict immediate 1-to-1 center alignment on this layer pair.
        parents.forEach((sid) => {
          const targets = [...(outAdj.get(sid) || [])].filter((tid) => visualLevelByNode.get(tid) === nextLv && nodePos.has(tid));
          if (targets.length !== 1) return;
          const tid = targets[0];
          const inFromLv = [...(inAdj.get(tid) || [])].filter((pid) => visualLevelByNode.get(pid) === lv && nodePos.has(pid));
          if (inFromLv.length !== 1) return;
          const sPos = nodePos.get(sid);
          const tPos = nodePos.get(tid);
          if (!sPos || !tPos) return;
          const sCenter = sPos.x + layoutSize(sid).w / 2;
          const tW = layoutSize(tid).w;
          const desiredLeft = clampNodeX(sCenter - tW / 2, tW);
          tPos.x = desiredLeft;
          hardLockX.set(tid, desiredLeft);
        });

        applyLayerGap(nextLv);
        lockedLevels.add(nextLv);
      }

      // Apply a final global spread pass for non-backbone columns so the
      // spread control always has a visible effect even on sparse graphs.
      const spreadT = (effectiveL1 - L1_MIN) / Math.max(1, L1_MAX - L1_MIN);
      const spreadFactor = 0.82 + spreadT * 0.68; // [tight..loose] => [0.82..1.50]
      nodePos.forEach((pos, id) => {
        if (!pos) return;
        if (backboneSet.has(id)) return;
        const nodeW = layoutSize(id).w;
        const center = pos.x + nodeW / 2;
        const shiftedCenter = centerX + (center - centerX) * spreadFactor;
        pos.x = clampNodeX(shiftedCenter - nodeW / 2, nodeW);
      });

      allLevels.forEach((lv) => applyLayerGap(lv));
    }

    const centerOf = (pos) => ({ x: pos.x + NODE_W / 2, y: pos.y + NODE_H / 2 });
    const pointSegDistance = (px, py, x1, y1, x2, y2) => {
      const vx = x2 - x1;
      const vy = y2 - y1;
      const len2 = vx * vx + vy * vy;
      if (len2 <= 0.0001) {
        return Math.hypot(px - x1, py - y1);
      }
      const t = Math.max(0, Math.min(1, ((px - x1) * vx + (py - y1) * vy) / len2));
      const qx = x1 + t * vx;
      const qy = y1 + t * vy;
      return Math.hypot(px - qx, py - qy);
    };

    const nodeIds = [...nodePos.keys()];
    const neighbors = new Map(nodeIds.map((id) => [id, new Set()]));
    edges.forEach((e) => {
      if (e.source === e.target) return;
      if (!neighbors.has(e.source) || !neighbors.has(e.target)) return;
      neighbors.get(e.source).add(e.target);
      neighbors.get(e.target).add(e.source);
    });

    if (layoutProfile.useCrossingOptimizers) {
      // Heuristic 1: shorten average edge length by moving nodes toward neighbor barycenter.
      for (let pass = 0; pass < 6; pass += 1) {
        const updates = new Map();
        nodeIds.forEach((id) => {
          const pos = nodePos.get(id);
          const near = [...(neighbors.get(id) || [])];
          if (!pos || !near.length) return;
          const selfCenter = centerOf(pos);
          let sum = 0;
          let cnt = 0;
          near.forEach((nid) => {
            const np = nodePos.get(nid);
            if (!np) return;
            sum += centerOf(np).x;
            cnt += 1;
          });
          if (!cnt) return;
          const targetCenterX = sum / cnt;
          const targetLeftX = targetCenterX - NODE_W / 2;
          const anchorStrength = backboneSet.has(id) ? 0.18 : 0.38;
          const blended = pos.x * (1 - anchorStrength) + targetLeftX * anchorStrength;
          updates.set(id, clampNodeX(blended));
        });
        updates.forEach((x, id) => {
          const p = nodePos.get(id);
          if (hardLockX.has(id)) return;
          if (p) p.x = x;
        });

        // Keep nodes in same layer separated after each smoothing pass.
        allLevels.forEach((lv) => {
          const layer = (groupedByLevel.get(lv) || []).map((m) => m.node.id).filter((id) => nodePos.has(id));
          const ordered = layer.sort((a, b) => nodePos.get(a).x - nodePos.get(b).x);
          const minGap = NODE_W + L1_MIN;
          for (let i = 1; i < ordered.length; i += 1) {
            const leftId = ordered[i - 1];
            const rightId = ordered[i];
            const left = nodePos.get(leftId);
            const right = nodePos.get(rightId);
            if (!left || !right) continue;
            const wantRight = left.x + minGap;
            if (right.x < wantRight) {
              right.x = clampNodeX(wantRight);
            }
          }
        });
      }

      // Heuristic 2: if an edge passes through another node, nudge that node aside.
      for (let pass = 0; pass < 3; pass += 1) {
        let moved = false;
        edges.forEach((edge) => {
          if (edge.source === edge.target) return;
          const s = nodePos.get(edge.source);
          const t = nodePos.get(edge.target);
          if (!s || !t) return;
          const sc = centerOf(s);
          const tc = centerOf(t);

          nodeIds.forEach((id) => {
            if (id === edge.source || id === edge.target) return;
            const p = nodePos.get(id);
            if (!p) return;
            if (hardLockX.has(id)) return;
            const c = centerOf(p);
            const d = pointSegDistance(c.x, c.y, sc.x, sc.y, tc.x, tc.y);
            const withinY = c.y >= Math.min(sc.y, tc.y) - NODE_H && c.y <= Math.max(sc.y, tc.y) + NODE_H;
            if (d < NODE_H * 0.75 && withinY) {
              const dir = c.x >= (sc.x + tc.x) / 2 ? 1 : -1;
              const nextX = clampNodeX(p.x + dir * 24);
              if (Math.abs(nextX - p.x) > 0.5) {
                p.x = nextX;
                moved = true;
              }
            }
          });
        });
        if (!moved) break;
      }
    }

    const nonLoopEdges = edges.filter((e) => e.source !== e.target);
    const segCross = (a1, a2, b1, b2) => {
      const orient = (p, q, r) => (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y);
      const o1 = orient(a1, a2, b1);
      const o2 = orient(a1, a2, b2);
      const o3 = orient(b1, b2, a1);
      const o4 = orient(b1, b2, a2);
      return ((o1 > 0 && o2 < 0) || (o1 < 0 && o2 > 0)) && ((o3 > 0 && o4 < 0) || (o3 < 0 && o4 > 0));
    };

    const layoutCost = () => {
      let cost = 0;
      const lenWeight = 1;
      const crossWeight = 260;
      const backboneDriftWeight = 3;

      nonLoopEdges.forEach((e) => {
        const s = nodePos.get(e.source);
        const t = nodePos.get(e.target);
        if (!s || !t) return;
        const sc = centerOf(s);
        const tc = centerOf(t);
        cost += Math.hypot(tc.x - sc.x, tc.y - sc.y) * lenWeight;
      });

      for (let i = 0; i < nonLoopEdges.length; i += 1) {
        const e1 = nonLoopEdges[i];
        const s1 = nodePos.get(e1.source);
        const t1 = nodePos.get(e1.target);
        if (!s1 || !t1) continue;
        const p1 = centerOf(s1);
        const p2 = centerOf(t1);

        for (let j = i + 1; j < nonLoopEdges.length; j += 1) {
          const e2 = nonLoopEdges[j];
          if (e1.source === e2.source || e1.source === e2.target || e1.target === e2.source || e1.target === e2.target) {
            continue;
          }
          const s2 = nodePos.get(e2.source);
          const t2 = nodePos.get(e2.target);
          if (!s2 || !t2) continue;
          const q1 = centerOf(s2);
          const q2 = centerOf(t2);
          if (segCross(p1, p2, q1, q2)) {
            cost += crossWeight;
          }
        }
      }

      backbone.forEach((id) => {
        const p = nodePos.get(id);
        if (!p) return;
        cost += Math.abs(p.x - centerX) * backboneDriftWeight;
      });

      return cost;
    };

    if (layoutProfile.useCrossingOptimizers) {
      // Local optimizer: swap adjacent same-level nodes when it lowers total layout cost.
      for (let pass = 0; pass < 4; pass += 1) {
        let improved = false;
        allLevels.forEach((lv) => {
          const ids = (groupedByLevel.get(lv) || []).map((m) => m.node.id).filter((id) => nodePos.has(id));
          ids.sort((a, b) => nodePos.get(a).x - nodePos.get(b).x);

          for (let i = 0; i < ids.length - 1; i += 1) {
            const a = ids[i];
            const b = ids[i + 1];
            if (backboneSet.has(a) || backboneSet.has(b)) continue;
            if (hardLockX.has(a) || hardLockX.has(b)) continue;
            if (visualLevelByNode.get(a) !== visualLevelByNode.get(b)) continue;

            const pa = nodePos.get(a);
            const pb = nodePos.get(b);
            if (!pa || !pb) continue;

            const before = layoutCost();
            const ax = pa.x;
            pa.x = pb.x;
            pb.x = ax;
            const after = layoutCost();
            if (after + 0.1 < before) {
              improved = true;
            } else {
              pb.x = pa.x;
              pa.x = ax;
            }
          }
        });
        if (!improved) break;
      }
    }

    if (layoutProfile.useCrossingOptimizers) {
      // Re-apply hard locks after all optimizers so symmetric fan-out does not drift.
      hardLockX.forEach((x, id) => {
        const pos = nodePos.get(id);
        if (!pos) return;
        pos.x = clampNodeX(x);
      });

      allLevels.forEach((lv) => {
        const ids = (groupedByLevel.get(lv) || []).map((m) => m.node.id).filter((id) => nodePos.has(id));
        ids.sort((a, b) => nodePos.get(a).x - nodePos.get(b).x);
        const minGap = NODE_W + L1_MIN;
        for (let i = 1; i < ids.length; i += 1) {
          const leftId = ids[i - 1];
          const rightId = ids[i];
          const left = nodePos.get(leftId);
          const right = nodePos.get(rightId);
          if (!left || !right) continue;
          if (right.x < left.x + minGap) {
            if (hardLockX.has(rightId) && !hardLockX.has(leftId)) {
              left.x = clampNodeX(right.x - minGap);
            } else {
              right.x = clampNodeX(left.x + minGap);
            }
          }
        }
      });
    }

    if (layoutEngine === "elk") {
      const cachedLayout = elkPositionCache.get(elkSignature);
      if (cachedLayout && cachedLayout.positions && cachedLayout.positions.size) {
        graphWidth = Math.max(graphWidth, Number(cachedLayout.canvasWidth) || GRAPH_WIDTH);
        graphHeight = Math.max(graphHeight, Number(cachedLayout.canvasHeight) || GRAPH_HEIGHT);
        applyGraphDimensions(graphWidth, graphHeight);

        nodePos.forEach((pos, id) => {
          const elkPos = cachedLayout.positions.get(id);
          if (!elkPos) return;
          const size = layoutSize(id);
          pos.x = clampNodeX(elkPos.x, size.w);
          pos.y = clampNodeY(elkPos.y, size.h);
        });

        // Bring the expanded ELK canvas back into view after coordinates are applied.
        if (layoutEngine === "elk") {
          requestAnimationFrame(() => {
            fitToView();
          });
        }
      } else {
        requestElkLayoutIfNeeded(elkSignature, nodes, edges, activeRevision);
      }
    }

    // Apply persisted manual node offsets (from drag interaction) after layout convergence.
    nodePos.forEach((pos, id) => {
      const offset = nodeManualOffsets.get(id);
      if (!offset) return;
      const size = layoutSize(id);
      pos.x = clampNodeX(pos.x + offset.dx, size.w);
      pos.y = clampNodeY(pos.y + offset.dy, size.h);
    });

    const enforceIndirectPathOutsidePlacement = () => {
      // Pattern: A -> C (direct) and A -> B -> C (indirect via B).
      // Keep B on branch outer side for visual consistency.
      const processed = new Set();

      nodePos.forEach((aPos, aId) => {
        if (!aPos) return;
        const aOut = outAdj.get(aId);
        if (!aOut || !aOut.size) return;

        aOut.forEach((cId) => {
          if (cId === aId) return;
          const cPos = nodePos.get(cId);
          if (!cPos) return;

          const intermediates = [...aOut].filter((bId) => {
            if (bId === aId || bId === cId) return false;
            const bOut = outAdj.get(bId);
            if (!bOut || !bOut.has(cId)) return false;

            // Scope to simple relay nodes to avoid broad topology drift.
            const inCount = (inAdj.get(bId) || new Set()).size;
            const outCount = bOut.size;
            return inCount === 1 && outCount === 1;
          });

          if (!intermediates.length) return;

          const aSize = layoutSize(aId);
          const cSize = layoutSize(cId);
          const aCenterX = aPos.x + aSize.w / 2;
          const aCenterY = aPos.y + aSize.h / 2;
          const cCenterX = cPos.x + cSize.w / 2;
          const cCenterY = cPos.y + cSize.h / 2;
          const dx = cCenterX - aCenterX;
          const dy = cCenterY - aCenterY;
          const directAngle = Math.atan2(Math.abs(dy), dx) * 180 / Math.PI;
          // Swapped rule kept: if direct-link angle > 90, place relay left; else right.
          const branchSign = directAngle > 90 ? -1 : 1;

          intermediates.forEach((bId) => {
            const pairKey = `${aId}->${bId}->${cId}`;
            if (processed.has(pairKey)) return;
            processed.add(pairKey);

            if (nodeManualOffsets.has(bId)) return;
            const bPos = nodePos.get(bId);
            if (!bPos) return;
            const bSize = layoutSize(bId);

            const minCenterGap = (cSize.w + bSize.w) / 2 + Math.max(16, Math.round(getEffectiveL1() * 0.2));
            const bCenterX = bPos.x + bSize.w / 2;
            const desiredCenterX = branchSign > 0
              ? Math.max(bCenterX, cCenterX + minCenterGap)
              : Math.min(bCenterX, cCenterX - minCenterGap);

            bPos.x = clampNodeX(desiredCenterX - bSize.w / 2, bSize.w);
          });
        });
      });
    };

    const indirectOutsideEnabledModes = new Set(["upstream", "downstream", "both", "full"]);
    if (indirectOutsideEnabledModes.has(currentTraceMode)) {
      enforceIndirectPathOutsidePlacement();
    }

    const allNodeY = [...nodePos.values()].map((p) => p.y);
    const minNodeY = allNodeY.length ? Math.min(...allNodeY) : 0;
    const maxNodeY = allNodeY.length ? Math.max(...allNodeY) : 0;
    const hiddenNodeIds = new Set();

    nodeMeta.forEach((meta) => {
      const identity = getNodeHiddenIdentity(meta.node);
      if (identity.key && hiddenObjectKeys.has(identity.key)) {
        hiddenNodeIds.add(meta.node.id);
      }
    });

    nodeMeta.forEach((meta) => {
      const pos = nodePos.get(meta.node.id);
      if (!pos) return;
      const el = document.createElement("button");
      el.type = "button";
      el.className = `node node-type-${meta.node.type} node-icon-${meta.iconKind.toLowerCase()}`;
      const rawLabel = String(meta.node?.label || meta.node?.id || "");
      const isDatasourceNode = normalizeTypeCategory(meta.node?.type) === "datasource";
      const dsLabel = isDatasourceNode ? splitDatasourceLabel(meta.node) : null;
      if (dsLabel) {
        el.classList.add("node-datasource-two-line");
        el.innerHTML = `<span class="node-label-line1">${esc(dsLabel.line1)}</span><span class="node-label-line2">${esc(dsLabel.line2)}</span>`;
      } else {
        el.textContent = rawLabel;
      }
      el.style.left = `${pos.x}px`;
      el.style.top = `${pos.y}px`;
      el.style.background = resolveNodeColor(meta.node);
      el.dataset.id = meta.node.id;
      el.dataset.techName = String(meta.node?.id || rawLabel || "");
      el.dataset.hoverText = String(meta.node?.object_name || "");
      const identity = getNodeHiddenIdentity(meta.node);
      el.dataset.hiddenBwObject = identity.bwObject;
      el.dataset.hiddenSourcesys = identity.sourcesys;
      el.dataset.hiddenKey = identity.key;
      el.dataset.isUserHidden = hiddenNodeIds.has(meta.node.id) ? "1" : "0";
      if (isFocusNode(meta.node)) {
        el.classList.add("seed-focus");
        el.classList.add("seed-focus-label-below");
        el.setAttribute("title", "基础模型");
      }
      flowNodes.appendChild(el);
    });

    const renderedSizeById = new Map();
    flowNodes.querySelectorAll(".node").forEach((el) => {
      const id = el.dataset.id;
      if (!id) return;
      renderedSizeById.set(id, {
        w: el.offsetWidth || NODE_W,
        h: el.offsetHeight || NODE_H
      });
    });
    const nodeSize = (id) => renderedSizeById.get(id) || { w: NODE_W, h: NODE_H };

    if (isDownstreamLayout) {
      // Final geometry pass: strict 1-to-1 adjacent-layer center alignment using
      // actual rendered widths (not fixed NODE_W).
      const immediateOut = new Map();
      const immediateIn = new Map();
      edges.forEach((e) => {
        if (e.source === e.target) return;
        const sLv = visualLevelByNode.get(e.source);
        const tLv = visualLevelByNode.get(e.target);
        if (typeof sLv !== "number" || typeof tLv !== "number") return;
        if (tLv !== sLv + 1) return;
        if (!immediateOut.has(e.source)) immediateOut.set(e.source, []);
        immediateOut.get(e.source).push(e.target);
        immediateIn.set(e.target, (immediateIn.get(e.target) || 0) + 1);
      });

      immediateOut.forEach((targets, sid) => {
        if (targets.length !== 1) return;
        const tid = targets[0];
        if ((immediateIn.get(tid) || 0) !== 1) return;
        const sPos = nodePos.get(sid);
        const tPos = nodePos.get(tid);
        if (!sPos || !tPos) return;

        const sSize = nodeSize(sid);
        const tSize = nodeSize(tid);
        const sourceCenterX = sPos.x + sSize.w / 2;
        const targetLeft = clampNodeX(sourceCenterX - tSize.w / 2);
        tPos.x = targetLeft;

        const targetEl = flowNodes.querySelector(`.node[data-id="${CSS.escape(tid)}"]`);
        if (targetEl) {
          targetEl.style.left = `${targetLeft}px`;
        }
      });
    }

    if (collisionAvoidanceEnabled) {
      // Final collision pass runs after all alignment rules and uses rendered sizes.
      const ids = [...nodePos.keys()];
      const maxPasses = currentTraceMode === "full" ? 28 : 18;
      const minGap = Math.max(12, Math.round(getEffectiveL1() * (currentTraceMode === "full" ? 0.18 : 0.14)));

      for (let pass = 0; pass < maxPasses; pass += 1) {
        let moved = false;

        for (let i = 0; i < ids.length; i += 1) {
          const aId = ids[i];
          const a = nodePos.get(aId);
          if (!a) continue;
          const aSize = nodeSize(aId);
          const aCx = a.x + aSize.w / 2;
          const aCy = a.y + aSize.h / 2;

          for (let j = i + 1; j < ids.length; j += 1) {
            const bId = ids[j];
            const b = nodePos.get(bId);
            if (!b) continue;
            const bSize = nodeSize(bId);
            const bCx = b.x + bSize.w / 2;
            const bCy = b.y + bSize.h / 2;

            const overlapX = aSize.w / 2 + bSize.w / 2 + minGap - Math.abs(aCx - bCx);
            const overlapY = aSize.h / 2 + bSize.h / 2 + minGap - Math.abs(aCy - bCy);
            if (overlapX <= 0 || overlapY <= 0) continue;

            const dxSign = aCx >= bCx ? 1 : -1;
            const dySign = aCy >= bCy ? 1 : -1;

            // Prefer horizontal split for label chips; fallback to vertical when needed.
            if (overlapX <= overlapY * 1.35) {
              const shift = overlapX / 2 + 0.8;
              a.x = clampNodeX(a.x + dxSign * shift, aSize.w);
              b.x = clampNodeX(b.x - dxSign * shift, bSize.w);
            } else {
              const shift = overlapY / 2 + 0.8;
              a.y = clampNodeY(a.y + dySign * shift, aSize.h);
              b.y = clampNodeY(b.y - dySign * shift, bSize.h);
            }
            moved = true;
          }
        }

        if (!moved) break;
      }

      flowNodes.querySelectorAll(".node").forEach((el) => {
        const id = el.dataset.id;
        if (!id) return;
        const pos = nodePos.get(id);
        if (!pos) return;
        el.style.left = `${pos.x}px`;
        el.style.top = `${pos.y}px`;
      });
    }

    const enforceDatasourceBottomRows = () => {
      const datasourceIds = nodeMeta
        .filter((meta) => normalizeTypeCategory(meta.node?.type) === "datasource")
        .map((meta) => meta.node.id)
        .filter((id) => nodePos.has(id));
      if (!datasourceIds.length) return;

      const nonDatasourceIds = nodeMeta
        .filter((meta) => normalizeTypeCategory(meta.node?.type) !== "datasource")
        .map((meta) => meta.node.id)
        .filter((id) => nodePos.has(id));
      const directUpperIds = new Set();

      const datasourceSet = new Set(datasourceIds);
      const anchorXById = new Map();
      datasourceIds.forEach((id) => {
        const linkedCenters = [];
        edges.forEach((edge) => {
          if (edge.source === id && !datasourceSet.has(edge.target) && nodePos.has(edge.target)) {
            const p = nodePos.get(edge.target);
            const s = nodeSize(edge.target);
            linkedCenters.push(p.x + s.w / 2);
            directUpperIds.add(edge.target);
          } else if (edge.target === id && !datasourceSet.has(edge.source) && nodePos.has(edge.source)) {
            const p = nodePos.get(edge.source);
            const s = nodeSize(edge.source);
            linkedCenters.push(p.x + s.w / 2);
            directUpperIds.add(edge.source);
          }
        });

        if (linkedCenters.length) {
          anchorXById.set(id, linkedCenters.reduce((a, b) => a + b, 0) / linkedCenters.length);
        } else {
          const p = nodePos.get(id);
          const s = nodeSize(id);
          anchorXById.set(id, (p?.x ?? 0) + s.w / 2);
        }
      });

      datasourceIds.sort((a, b) => {
        const ax = anchorXById.get(a) ?? (nodePos.get(a)?.x ?? 0);
        const bx = anchorXById.get(b) ?? (nodePos.get(b)?.x ?? 0);
        return ax - bx;
      });

      const gapX = Math.max(12, Math.round(getEffectiveL1() * 0.2));
      const rowGapY = Math.max(8, Math.round(NODE_H * 0.38));
      let clusterGapY = Math.max(18, Math.round(NODE_H * 0.72));
      const packedWidth = (ids) => {
        if (!ids.length) return 0;
        return ids.reduce((sum, id) => sum + (nodeSize(id).w || NODE_W), 0) + gapX * Math.max(0, ids.length - 1);
      };

      let clusterCenterX = graphWidth / 2;
      let clusterWidth = Math.max(320, graphWidth - sidePadding * 2);
      let clusterBottom = topPadding + Math.round((graphHeight - topPadding - bottomPadding) * 0.72);

      if (nonDatasourceIds.length) {
        let minX = Infinity;
        let maxX = -Infinity;
        let maxY = -Infinity;
        const rowCenters = [];
        nonDatasourceIds.forEach((id) => {
          const p = nodePos.get(id);
          if (!p) return;
          const size = nodeSize(id);
          minX = Math.min(minX, p.x);
          maxX = Math.max(maxX, p.x + size.w);
          maxY = Math.max(maxY, p.y + size.h);
          rowCenters.push(p.y + size.h / 2);
        });
        if (Number.isFinite(minX) && Number.isFinite(maxX) && Number.isFinite(maxY)) {
          clusterCenterX = (minX + maxX) / 2;
          clusterWidth = Math.max(320, maxX - minX + Math.round(getEffectiveL1() * 0.9));
          clusterBottom = maxY;

          // Estimate current layer rhythm from existing rows and use it as datasource offset.
          const snapped = [...new Set(rowCenters.map((v) => Math.round(v / 8) * 8))].sort((a, b) => a - b);
          if (snapped.length >= 2) {
            const gaps = [];
            for (let i = 1; i < snapped.length; i += 1) {
              const d = snapped[i] - snapped[i - 1];
              if (d > 6) gaps.push(d);
            }
            if (gaps.length) {
              const avgGap = gaps.reduce((a, b) => a + b, 0) / gaps.length;
              clusterGapY = Math.max(24, Math.min(96, Math.round(avgGap * 0.9)));
            }
          }
        }
      }

      const maxInnerWidth = Math.max(320, Math.min(graphWidth - sidePadding * 2, clusterWidth));
      const oneRowWidth = packedWidth(datasourceIds);
      const useTwoRows = datasourceIds.length >= 6 && oneRowWidth > maxInnerWidth * 1.5;
      const rows = useTwoRows ? [[], []] : [[]];

      if (useTwoRows) {
        const splitAt = Math.ceil(datasourceIds.length / 2);
        rows[0] = datasourceIds.slice(0, splitAt);
        rows[1] = datasourceIds.slice(splitAt);
      } else {
        rows[0] = datasourceIds.slice();
      }

      const widestRow = Math.max(...rows.map((row) => packedWidth(row)));
      const neededWidth = Math.max(graphWidth, Math.ceil(widestRow + sidePadding * 2));
      if (neededWidth > graphWidth) {
        graphWidth = neededWidth;
        applyGraphDimensions(graphWidth, graphHeight);
      }

      const firstRowTop = clampNodeY(clusterBottom + clusterGapY, NODE_H);
      const lastRowBottom = firstRowTop + (rows.length - 1) * (NODE_H + rowGapY) + NODE_H;
      const neededHeight = Math.max(graphHeight, Math.ceil(lastRowBottom + bottomPadding + 4));
      if (neededHeight > graphHeight) {
        graphHeight = neededHeight;
        applyGraphDimensions(graphWidth, graphHeight);
      }

      rows.forEach((row, idx) => {
        if (!row.length) return;
        const y = clampNodeY(firstRowTop + idx * (NODE_H + rowGapY), NODE_H);
        const minLeft = sidePadding;
        const maxRight = graphWidth - sidePadding;
        const placements = [];
        let prevRight = minLeft - gapX;

        row.forEach((id) => {
          const w = nodeSize(id).w || NODE_W;
          const anchor = anchorXById.get(id) ?? clusterCenterX;
          const desiredLeft = anchor - w / 2;
          let left = Math.max(minLeft, Math.min(maxRight - w, desiredLeft));
          left = Math.max(left, prevRight + gapX);
          if (left > maxRight - w) {
            left = maxRight - w;
          }
          placements.push({ id, left, w });
          prevRight = left + w;
        });

        if (placements.length) {
          const overflow = placements[placements.length - 1].left + placements[placements.length - 1].w - maxRight;
          if (overflow > 0) {
            placements.forEach((p) => {
              p.left -= overflow;
            });
          }

          if (placements[0].left < minLeft) {
            const rowWidth = packedWidth(row);
            let x = Math.max(minLeft, Math.min(maxRight - rowWidth, clusterCenterX - rowWidth / 2));
            placements.forEach((p) => {
              p.left = x;
              x += p.w + gapX;
            });
          }
        }

        placements.forEach((p) => {
          const pos = nodePos.get(p.id);
          if (!pos) return;
          pos.x = clampNodeX(p.left, p.w);
          pos.y = y;
        });
      });

      // Final pass: align datasource layer center with the directly connected upper layer center.
      if (directUpperIds.size) {
        const upperCenters = [...directUpperIds]
          .map((id) => {
            const p = nodePos.get(id);
            if (!p) return null;
            const s = nodeSize(id);
            return p.x + s.w / 2;
          })
          .filter((v) => Number.isFinite(v));

        const dsLeft = Math.min(...datasourceIds.map((id) => nodePos.get(id).x));
        const dsRight = Math.max(...datasourceIds.map((id) => nodePos.get(id).x + (nodeSize(id).w || NODE_W)));
        const dsCenter = (dsLeft + dsRight) / 2;
        const upperCenter = upperCenters.length
          ? upperCenters.reduce((a, b) => a + b, 0) / upperCenters.length
          : dsCenter;

        const shift = upperCenter - dsCenter;

        if (Math.abs(shift) > 0.5) {
          datasourceIds.forEach((id) => {
            const p = nodePos.get(id);
            if (!p) return;
            p.x += shift;
          });
        }
      }

      flowNodes.querySelectorAll(".node").forEach((el) => {
        const id = el.dataset.id;
        if (!id) return;
        const pos = nodePos.get(id);
        if (!pos) return;
        el.style.left = `${pos.x}px`;
        el.style.top = `${pos.y}px`;
      });
    };

    enforceDatasourceBottomRows();

    const enforceNearestToDatasourceSingleLayer = () => {
      const datasourceSet = new Set(
        nodeMeta
          .filter((meta) => normalizeTypeCategory(meta.node?.type) === "datasource")
          .map((meta) => meta.node.id)
          .filter((id) => nodePos.has(id))
      );
      if (!datasourceSet.size) return;

      const linkedNonDatasource = new Set();
      edges.forEach((edge) => {
        const srcIsDs = datasourceSet.has(edge.source);
        const tgtIsDs = datasourceSet.has(edge.target);
        if (!srcIsDs && !tgtIsDs) return;

        const nearId = srcIsDs ? edge.target : edge.source;
        if (datasourceSet.has(nearId)) return;
        if (!nodePos.has(nearId)) return;
        linkedNonDatasource.add(nearId);
      });
      if (!linkedNonDatasource.size) return;

      const dsTopY = Math.min(...[...datasourceSet].map((id) => nodePos.get(id).y));
      const nearCenters = [...linkedNonDatasource].map((id) => {
        const p = nodePos.get(id);
        const s = nodeSize(id);
        return p.y + s.h / 2;
      });
      const snapped = [...new Set(nearCenters.map((v) => Math.round(v / 8) * 8))].sort((a, b) => a - b);
      let avgLayerGap = NODE_H + 26;
      if (snapped.length >= 2) {
        const diffs = [];
        for (let i = 1; i < snapped.length; i += 1) {
          const d = snapped[i] - snapped[i - 1];
          if (d > 6) diffs.push(d);
        }
        if (diffs.length) {
          avgLayerGap = Math.round(diffs.reduce((a, b) => a + b, 0) / diffs.length);
        }
      }

      const targetY = clampNodeY(dsTopY - Math.max(28, Math.round(avgLayerGap * 0.95)), NODE_H);
      linkedNonDatasource.forEach((id) => {
        const pos = nodePos.get(id);
        if (!pos) return;
        pos.y = targetY;
      });

      flowNodes.querySelectorAll(".node").forEach((el) => {
        const id = el.dataset.id;
        if (!id) return;
        const pos = nodePos.get(id);
        if (!pos) return;
        el.style.top = `${pos.y}px`;
      });
    };

    enforceNearestToDatasourceSingleLayer();

    const recenterGraphHorizontally = () => {
      const ids = [...nodePos.keys()];
      if (!ids.length) return;

      const bounds = () => {
        let minX = Infinity;
        let maxX = -Infinity;
        ids.forEach((id) => {
          const p = nodePos.get(id);
          if (!p) return;
          const w = nodeSize(id).w || NODE_W;
          minX = Math.min(minX, p.x);
          maxX = Math.max(maxX, p.x + w);
        });
        if (!Number.isFinite(minX) || !Number.isFinite(maxX)) {
          return { minX: 0, maxX: graphWidth };
        }
        return { minX, maxX };
      };

      let { minX, maxX } = bounds();
      const contentW = Math.max(1, maxX - minX);
      const requiredW = Math.max(GRAPH_WIDTH, Math.ceil(contentW + sidePadding * 2));
      if (requiredW > graphWidth) {
        graphWidth = requiredW;
        applyGraphDimensions(graphWidth, graphHeight);
      }

      const targetMinX = (graphWidth - contentW) / 2;
      const dx = targetMinX - minX;
      if (Math.abs(dx) > 0.5) {
        ids.forEach((id) => {
          const p = nodePos.get(id);
          if (!p) return;
          p.x += dx;
        });
      }

      ({ minX, maxX } = bounds());
      const leftOverflow = sidePadding - minX;
      const rightOverflow = maxX - (graphWidth - sidePadding);
      let fix = 0;
      if (leftOverflow > 0) {
        fix = leftOverflow;
      } else if (rightOverflow > 0) {
        fix = -rightOverflow;
      }
      if (Math.abs(fix) > 0.5) {
        ids.forEach((id) => {
          const p = nodePos.get(id);
          if (!p) return;
          p.x += fix;
        });
      }

      flowNodes.querySelectorAll(".node").forEach((el) => {
        const id = el.dataset.id;
        if (!id) return;
        const pos = nodePos.get(id);
        if (!pos) return;
        el.style.left = `${pos.x}px`;
      });
    };

    recenterGraphHorizontally();

    // Render additional object-name text near each node without changing key text.
    flowNodes.querySelectorAll(".node-extra-text").forEach((el) => el.remove());
    nodeMeta.forEach((meta) => {
      const objectName = String(meta.node?.object_name || "").trim();
      if (!objectName) return;
      const pos = nodePos.get(meta.node.id);
      if (!pos) return;
      const size = nodeSize(meta.node.id);
      const tag = document.createElement("div");
      tag.className = "node-extra-text";
      tag.dataset.id = meta.node.id;
      const wrapped = wrapByWordCount(objectName, 25);
      tag.textContent = wrapped;
      const lineCount = wrapped ? wrapped.split("\n").length : 1;
      if (lineCount > 1) tag.classList.add("is-multiline");
      tag.style.left = `${pos.x + size.w / 2}px`;
      flowNodes.appendChild(tag);

      const textHeight = measureHiddenElementHeight(tag);
      tag.style.top = `${pos.y - textHeight}px`;
    });

    lastRenderContext = {
      edges,
      nodePos,
      nodeSize,
      hiddenNodeIds
    };
    const renderedEdgeCount = applyHiddenVisibilityToRenderedGraph();
    updateHiddenToggleButtonState();

    const renderedNodeCount = flowNodes.querySelectorAll(".node").length;
    if (renderedNodeCount !== nodes.length || renderedEdgeCount !== edges.length) {
      console.error("[flow strict check] render count mismatch", {
        sourceNodeCount: nodes.length,
        sourceEdgeCount: edges.length,
        renderedNodeCount,
        renderedEdgeCount
      });
    }

    flowNodes.querySelectorAll(".node").forEach((el) => {
      el.addEventListener("mouseenter", () => {
        const id = el.dataset.id;
        if (!id) return;
        const textEl = flowNodes.querySelector(`.node-extra-text[data-id="${CSS.escape(id)}"]`);
        if (textEl) textEl.classList.add("hover-visible");
      });

      el.addEventListener("mouseleave", () => {
        const id = el.dataset.id;
        if (!id) return;
        const textEl = flowNodes.querySelector(`.node-extra-text[data-id="${CSS.escape(id)}"]`);
        if (textEl) textEl.classList.remove("hover-visible");
      });

      el.addEventListener("click", () => {
        if (el.dataset.suppressClick === "1") {
          el.dataset.suppressClick = "";
          return;
        }
        toggleNodeFlowHighlight(el.dataset.id || "");
      });

      el.addEventListener("contextmenu", (event) => {
        event.preventDefault();
        event.stopPropagation();
        showNodeContextMenu(event.clientX, event.clientY, {
          nodeId: el.dataset.id,
          techName: el.dataset.techName,
          hoverText: el.dataset.hoverText,
          bwObject: el.dataset.hiddenBwObject,
          sourcesys: el.dataset.hiddenSourcesys
        });
      });

      el.addEventListener("pointerdown", (event) => {
        if (event.pointerType === "mouse" && event.button !== 0) {
          return;
        }
        // Node drag is independent from viewport pan.
        event.preventDefault();
        event.stopPropagation();

        const id = el.dataset.id;
        if (!id) return;

        const startLeft = Number.parseFloat(el.style.left || "0") || 0;
        const startTop = Number.parseFloat(el.style.top || "0") || 0;
        const startX = event.clientX;
        const startY = event.clientY;
        let moved = false;

        el.setPointerCapture(event.pointerId);
        el.classList.add("dragging");

        const onMove = (moveEvent) => {
          const dx = moveEvent.clientX - startX;
          const dy = moveEvent.clientY - startY;
          if (Math.abs(dx) > 3 || Math.abs(dy) > 3) moved = true;
          el.style.left = `${startLeft + dx}px`;
          el.style.top = `${startTop + dy}px`;
          syncGraphAfterManualDrag(false);
        };

        const onUp = (upEvent) => {
          const dx = upEvent.clientX - startX;
          const dy = upEvent.clientY - startY;
          const existing = nodeManualOffsets.get(id) || { dx: 0, dy: 0 };
          nodeManualOffsets.set(id, { dx: existing.dx + dx, dy: existing.dy + dy });

          el.classList.remove("dragging");
          if (el.hasPointerCapture(upEvent.pointerId)) {
            el.releasePointerCapture(upEvent.pointerId);
          }
          el.removeEventListener("pointermove", onMove);
          el.removeEventListener("pointerup", onUp);
          el.removeEventListener("pointercancel", onUp);

          if (moved) {
            el.dataset.suppressClick = "1";
          }

          // Keep manual drag position exactly as-is and only refresh overlays.
          syncGraphAfterManualDrag(true);
        };

        el.addEventListener("pointermove", onMove);
        el.addEventListener("pointerup", onUp);
        el.addEventListener("pointercancel", onUp);
      });
    });

    renderMiniMap();
  }

  function resolveNodeIconKind(node) {
    const typeCode = String(node?.type || "").trim().toUpperCase();
    if (["RSDS", "TRCS", "ADSO", "IOBJ", "HCPR", "ELEM", "DEST"].includes(typeCode)) {
      return typeCode;
    }

    const raw = String(node?.label || node?.id || "").toUpperCase();
    const prefix = raw.split(/[_\-\s]/, 1)[0] || raw;

    const prefixMap = {
      RSDS: "RSDS",
      TRCS: "TRCS",
      ADSO: "ADSO",
      IOBJ: "IOBJ",
      HCPR: "HCPR",
      ELEM: "ELEM",
      DEST: "DEST"
    };
    if (prefixMap[prefix]) {
      return prefixMap[prefix];
    }

    return "UNKNOWN";
  }

  function getDefaultTypeList() {
    return [...FIXED_TYPES, ...OPTIONAL_TYPES];
  }

  function buildSelectedTypes(optionalTypes) {
    return [...new Set([...FIXED_TYPES, ...optionalTypes])];
  }

  function renderIncludeFilters(initialTypes) {
    if (!includeToggles) return;

    const filterItems = [
      { key: "datasource", label: "数据源", fixed: false },
      { key: "infosource", label: "信息源", fixed: true },
      { key: "adso", label: "ADSO", fixed: true },
      { key: "cp", label: "CP", fixed: false },
      { key: "report", label: "报表", fixed: false }
    ];

    const defaults = initialTypes.length ? initialTypes : getDefaultTypeList();
    const optionalDefaults = OPTIONAL_TYPES.filter((key) => defaults.includes(key));
    selectedTypes = buildSelectedTypes(optionalDefaults);

    includeToggles.innerHTML = filterItems
      .map((item) => {
        const checked = selectedTypes.includes(item.key) ? "checked" : "";
        const disabled = item.fixed ? "disabled" : "";
        return `<label class="include-item ${item.fixed ? "fixed" : ""}"><input type="checkbox" value="${esc(item.key)}" ${checked} ${disabled} /><span>${item.label}</span></label>`;
      })
      .join("");

    includeToggles.addEventListener("change", () => {
      const optionalSelected = [...includeToggles.querySelectorAll("input:checked:not(:disabled)")].map((node) => node.value);
      selectedTypes = buildSelectedTypes(optionalSelected);
      buildGraphView(selectedTypes);
      fitToView();
    });
  }

  function renderLegend() {
    if (!legendList) return;

    const legendItems = [
      { label: "[RSDS] 数据源", icon: "DataSource.png" },
      { label: "[TRCS] 信息源", icon: "InfoSource.png" },
      { label: "[ADSO] 模型", icon: "ADSO.png" },
      { label: "[IOBJ] 主数据", icon: "MasterData.png" },
      { label: "[HCPR] CP", icon: "CompositeProvider.png" },
      { label: "[ELEM] BW Query", icon: "BW_Query.png" },
      { label: "[DEST] OpenHub目标", icon: "OpenHub.png" },
      { label: "[TRAN] 转换有逻辑", icon: "transformation.png" }
    ];

    legendList.innerHTML = legendItems
      .map((item) => {
        const iconSrc = `./Assets/Icons/${item.icon}`;
        return `<li><img class="legend-icon" src="${esc(iconSrc)}" alt="${esc(item.label)}" /><span>${item.label}</span></li>`;
      })
      .join("");
  }

  function setupSideDrawers() {
    const drawerEntries = [
      { drawer: legendDrawer, toggle: legendToggle },
      { drawer: zoomDrawer, toggle: zoomToggle },
      { drawer: includeDrawer, toggle: includeToggle }
    ].filter((item) => item.drawer && item.toggle);
    const hideTimers = new WeakMap();

    const clearHideTimer = (drawer) => {
      const timerId = hideTimers.get(drawer);
      if (timerId) {
        window.clearTimeout(timerId);
      }
      hideTimers.delete(drawer);
    };

    const showPeek = (drawer) => {
      const peek = drawer?.querySelector(".drawer-peek");
      if (peek) {
        peek.classList.remove("peek-hidden");
      }
    };

    const hidePeek = (drawer) => {
      const peek = drawer?.querySelector(".drawer-peek");
      if (peek) {
        peek.classList.add("peek-hidden");
      }
    };

    const layoutDrawers = () => {
      if (!legendDrawer || !zoomDrawer || !includeDrawer) return;

      const legendTop = DRAWER_TOP_START;
      legendDrawer.style.top = `${legendTop}px`;

      const legendHeight = legendDrawer.getBoundingClientRect().height;
      const legendWidth = legendDrawer.getBoundingClientRect().width;
      includeDrawer.style.width = `${Math.ceil(legendWidth)}px`;

      const includeTop = legendTop + legendHeight + DRAWER_GAP;
      includeDrawer.style.top = `${includeTop}px`;

      const includeHeight = includeDrawer.getBoundingClientRect().height;
      const zoomTop = includeTop + includeHeight + DRAWER_GAP;
      zoomDrawer.style.top = `${zoomTop}px`;
    };

    const setCollapsed = (drawer, collapsed) => {
      if (!drawer) return;
      clearHideTimer(drawer);
      drawer.classList.toggle("collapsed", collapsed);
      showPeek(drawer);
      layoutDrawers();
      if (!collapsed) {
        schedulePeekAutoHide(drawer);
      }
    };

    const schedulePeekAutoHide = (drawer) => {
      if (!drawer) return;
      if (drawer.classList.contains("collapsed")) return;

      clearHideTimer(drawer);
      const timerId = window.setTimeout(() => {
        if (drawer.matches(":hover")) {
          schedulePeekAutoHide(drawer);
          return;
        }
        hidePeek(drawer);
      }, 2000);
      hideTimers.set(drawer, timerId);
    };

    drawerEntries.forEach(({ drawer, toggle }) => {
      drawer.addEventListener("mouseenter", () => {
        showPeek(drawer);
        clearHideTimer(drawer);
      });

      drawer.addEventListener("mouseleave", () => {
        schedulePeekAutoHide(drawer);
      });

      toggle.addEventListener("click", () => {
        const willOpen = drawer.classList.contains("collapsed");
        setCollapsed(drawer, !willOpen);
      });

      // Default visibility: zoom shown, legend/include hidden.
      const shouldCollapseByDefault = drawer !== zoomDrawer;
      setCollapsed(drawer, shouldCollapseByDefault);
    });

    window.addEventListener("resize", layoutDrawers);
    layoutDrawers();
  }

  function renderMiniMapBase(ctx, pxW, pxH, s, offX, offY) {
    ctx.clearRect(0, 0, pxW, pxH);
    ctx.fillStyle = "rgba(10, 18, 30, 0.92)";
    ctx.fillRect(0, 0, pxW, pxH);

    const nodes = [...flowNodes.querySelectorAll(".node")];
    nodes.forEach((el) => {
      const left = Number.parseFloat(el.style.left || "0") || 0;
      const top = Number.parseFloat(el.style.top || "0") || 0;
      const w = el.offsetWidth || 110;
      const h = el.offsetHeight || 34;
      const x = offX + left * s;
      const y = offY + top * s;
      const rw = Math.max(2, w * s);
      const rh = Math.max(2, h * s);
      const r = Math.max(1, Math.min(rh / 2, rw / 3));

      ctx.fillStyle = "rgba(119, 173, 255, 0.75)";
      ctx.beginPath();
      ctx.moveTo(x + r, y);
      ctx.lineTo(x + rw - r, y);
      ctx.quadraticCurveTo(x + rw, y, x + rw, y + r);
      ctx.lineTo(x + rw, y + rh - r);
      ctx.quadraticCurveTo(x + rw, y + rh, x + rw - r, y + rh);
      ctx.lineTo(x + r, y + rh);
      ctx.quadraticCurveTo(x, y + rh, x, y + rh - r);
      ctx.lineTo(x, y + r);
      ctx.quadraticCurveTo(x, y, x + r, y);
      ctx.closePath();
      ctx.fill();
    });
  }

  function updateMiniMapViewport() {
    if (!miniMapCanvas) return;
    const ctx = miniMapCanvas.getContext("2d");
    if (!ctx) return;
    const { scale: s, offsetX: offX, offsetY: offY, canvasWidth: pxW, canvasHeight: pxH } = miniMapTransform;
    if (!s || !pxW || !pxH) return;

    renderMiniMapBase(ctx, pxW, pxH, s, offX, offY);

    const rawViewLeft = -offsetX / Math.max(scale, 0.0001);
    const rawViewTop = -offsetY / Math.max(scale, 0.0001);
    const viewW = Math.min(currentGraphWidth, flowCanvas.clientWidth / Math.max(scale, 0.0001));
    const viewH = Math.min(currentGraphHeight, flowCanvas.clientHeight / Math.max(scale, 0.0001));
    const viewLeft = Math.max(0, Math.min(currentGraphWidth - viewW, rawViewLeft));
    const viewTop = Math.max(0, Math.min(currentGraphHeight - viewH, rawViewTop));

    const x = offX + viewLeft * s;
    const y = offY + viewTop * s;
    const w = Math.max(6, viewW * s);
    const h = Math.max(6, viewH * s);

    ctx.strokeStyle = "rgba(255, 225, 140, 0.95)";
    ctx.lineWidth = 2;
    ctx.strokeRect(x, y, w, h);
    ctx.fillStyle = "rgba(255, 225, 140, 0.12)";
    ctx.fillRect(x, y, w, h);
  }

  function renderMiniMap() {
    if (!miniMapCanvas) return;
    const ctx = miniMapCanvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const cssW = miniMapCanvas.clientWidth || 220;
    const cssH = miniMapCanvas.clientHeight || 132;
    const pxW = Math.max(1, Math.round(cssW * dpr));
    const pxH = Math.max(1, Math.round(cssH * dpr));
    if (miniMapCanvas.width !== pxW || miniMapCanvas.height !== pxH) {
      miniMapCanvas.width = pxW;
      miniMapCanvas.height = pxH;
    }

    const worldW = Math.max(1, currentGraphWidth);
    const worldH = Math.max(1, currentGraphHeight);
    const s = Math.min(pxW / worldW, pxH / worldH);
    const offX = (pxW - worldW * s) / 2;
    const offY = (pxH - worldH * s) / 2;
    miniMapTransform = { scale: s, offsetX: offX, offsetY: offY, canvasWidth: pxW, canvasHeight: pxH };

    renderMiniMapBase(ctx, pxW, pxH, s, offX, offY);
    updateMiniMapViewport();
  }

  function setupMiniMapDrawer() {
    if (!miniMapDrawer || !miniMapToggle || !miniMapCanvas) return;

    let hideTimer = null;
    const clearTimer = () => {
      if (hideTimer) window.clearTimeout(hideTimer);
      hideTimer = null;
    };
    const showPeek = () => miniMapToggle.classList.remove("peek-hidden");
    const hidePeek = () => miniMapToggle.classList.add("peek-hidden");
    const setCollapsed = (collapsed) => {
      clearTimer();
      miniMapDrawer.classList.toggle("collapsed", collapsed);
      showPeek();
      if (!collapsed) {
        hideTimer = window.setTimeout(() => {
          if (miniMapDrawer.matches(":hover")) return;
          hidePeek();
        }, 2000);
      }
    };

    miniMapDrawer.addEventListener("mouseenter", () => {
      showPeek();
      clearTimer();
    });
    miniMapDrawer.addEventListener("mouseleave", () => {
      if (miniMapDrawer.classList.contains("collapsed")) return;
      clearTimer();
      hideTimer = window.setTimeout(() => hidePeek(), 2000);
    });
    miniMapToggle.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      setCollapsed(!miniMapDrawer.classList.contains("collapsed"));
    });

    miniMapDrawer.addEventListener("pointerdown", (event) => {
      if (!miniMapDrawer.classList.contains("collapsed")) return;
      if (event.target.closest("#miniMapToggle")) return;
      if (miniMapCanvas.contains(event.target)) return;
      event.preventDefault();
      event.stopPropagation();
      setCollapsed(false);
    });

    const focusByMiniMapPointer = (event) => {
      const rect = miniMapCanvas.getBoundingClientRect();
      const x = (event.clientX - rect.left) * (miniMapCanvas.width / Math.max(1, rect.width));
      const y = (event.clientY - rect.top) * (miniMapCanvas.height / Math.max(1, rect.height));
      const { scale: s, offsetX: offX, offsetY: offY } = miniMapTransform;
      if (!s) return;
      const worldX = Math.max(0, Math.min(currentGraphWidth, (x - offX) / s));
      const worldY = Math.max(0, Math.min(currentGraphHeight, (y - offY) / s));
      offsetX = flowCanvas.clientWidth / 2 - worldX * scale;
      offsetY = flowCanvas.clientHeight / 2 - worldY * scale;
      applyViewportTransform();
    };

    miniMapCanvas.addEventListener("pointerdown", (event) => {
      if (miniMapDrawer.classList.contains("collapsed")) return;

      // Minimap interaction should not trigger viewport panning on the parent canvas.
      event.preventDefault();
      event.stopPropagation();

      focusByMiniMapPointer(event);
      miniMapCanvas.setPointerCapture(event.pointerId);

      const onMove = (moveEvent) => {
        moveEvent.preventDefault();
        moveEvent.stopPropagation();
        focusByMiniMapPointer(moveEvent);
      };

      const onEnd = (endEvent) => {
        endEvent.preventDefault();
        endEvent.stopPropagation();
        if (miniMapCanvas.hasPointerCapture(endEvent.pointerId)) {
          miniMapCanvas.releasePointerCapture(endEvent.pointerId);
        }
        miniMapCanvas.removeEventListener("pointermove", onMove);
        miniMapCanvas.removeEventListener("pointerup", onEnd);
        miniMapCanvas.removeEventListener("pointercancel", onEnd);
      };

      miniMapCanvas.addEventListener("pointermove", onMove);
      miniMapCanvas.addEventListener("pointerup", onEnd);
      miniMapCanvas.addEventListener("pointercancel", onEnd);
    });

    window.addEventListener("resize", () => {
      renderMiniMap();
      updateMiniMapViewport();
    });

    setCollapsed(true);
    renderMiniMap();
  }

  function clampScale(nextScale) {
    return Math.max(MIN_SCALE, Math.min(MAX_SCALE, nextScale));
  }

  function applyViewportTransform() {
    flowViewport.style.transform = `translate3d(${offsetX}px, ${offsetY}px, 0) scale(${scale})`;
    zoomSlider.value = String(Math.round(scale * 100));
    if (zoomValue) {
      zoomValue.textContent = `${Math.round(scale * 100)}%`;
    }
    updateMiniMapViewport();
  }

  function setScaleAt(nextScale, anchorX, anchorY) {
    const newScale = clampScale(nextScale);
    const worldX = (anchorX - offsetX) / scale;
    const worldY = (anchorY - offsetY) / scale;
    scale = newScale;
    offsetX = anchorX - worldX * scale;
    offsetY = anchorY - worldY * scale;
    applyViewportTransform();
  }

  function fitToView() {
    const rect = flowCanvas.getBoundingClientRect();
    const nodes = [...flowNodes.querySelectorAll(".node")];
    let minX = 0;
    let minY = 0;
    let maxX = currentGraphWidth;
    let maxY = currentGraphHeight;

    if (nodes.length) {
      minX = Infinity;
      minY = Infinity;
      maxX = -Infinity;
      maxY = -Infinity;

      nodes.forEach((nodeEl) => {
        const left = Number.parseFloat(nodeEl.style.left || "0") || 0;
        const top = Number.parseFloat(nodeEl.style.top || "0") || 0;
        const w = nodeEl.offsetWidth || 110;
        const h = nodeEl.offsetHeight || 34;
        minX = Math.min(minX, left);
        minY = Math.min(minY, top);
        maxX = Math.max(maxX, left + w);
        maxY = Math.max(maxY, top + h);
      });

      if (!Number.isFinite(minX) || !Number.isFinite(minY) || !Number.isFinite(maxX) || !Number.isFinite(maxY)) {
        minX = 0;
        minY = 0;
        maxX = currentGraphWidth;
        maxY = currentGraphHeight;
      }
    }

    const pad = 24;
    const worldW = Math.max(1, maxX - minX + pad * 2);
    const worldH = Math.max(1, maxY - minY + pad * 2);
    scale = clampScale(Math.min((rect.width - 24) / worldW, (rect.height - 24) / worldH));
    offsetX = (rect.width - worldW * scale) / 2 - (minX - pad) * scale;
    offsetY = (rect.height - worldH * scale) / 2 - (minY - pad) * scale;
    applyViewportTransform();
  }

  function getDistance(a, b) {
    const dx = a.x - b.x;
    const dy = a.y - b.y;
    return Math.hypot(dx, dy);
  }

  function getMidpoint(a, b) {
    return {
      x: (a.x + b.x) / 2,
      y: (a.y + b.y) / 2
    };
  }

  function setupViewportInteraction() {
    if (!flowCanvas || !flowViewport) return;

    const toLocalPoint = (event) => {
      const rect = flowCanvas.getBoundingClientRect();
      return {
        x: event.clientX - rect.left,
        y: event.clientY - rect.top
      };
    };

    flowCanvas.addEventListener("pointerdown", (event) => {
      if (event.pointerType === "mouse" && event.button !== 0) {
        return;
      }
      if (
        event.target.closest(".side-drawer")
        || event.target.closest(".mini-map-drawer")
        || event.target.closest(".reset-view")
        || event.target.closest(".flow-title")
        || event.target.closest(".layout-spread-control")
        || event.target.closest(".node")
      ) {
        return;
      }

      pointers.set(event.pointerId, toLocalPoint(event));
      flowCanvas.setPointerCapture(event.pointerId);

      if (pointers.size === 2) {
        const [p1, p2] = [...pointers.values()];
        const mid = getMidpoint(p1, p2);
        pinchStartDistance = getDistance(p1, p2);
        pinchStartScale = scale;
        pinchWorldX = (mid.x - offsetX) / scale;
        pinchWorldY = (mid.y - offsetY) / scale;
        isDragging = false;
        flowCanvas.classList.remove("dragging");
        return;
      }

      isDragging = true;
      dragStartX = event.clientX;
      dragStartY = event.clientY;
      startOffsetX = offsetX;
      startOffsetY = offsetY;
      flowCanvas.classList.add("dragging");
    });

    flowCanvas.addEventListener("contextmenu", (event) => {
      if (event.target.closest(".node")) return;
      event.preventDefault();
      hideNodeContextMenu();
    });

    flowCanvas.addEventListener("pointermove", (event) => {
      if (pointers.has(event.pointerId)) {
        pointers.set(event.pointerId, toLocalPoint(event));
      }

      if (pointers.size === 2) {
        const [p1, p2] = [...pointers.values()];
        const mid = getMidpoint(p1, p2);
        const distance = getDistance(p1, p2);
        if (pinchStartDistance > 0) {
          scale = clampScale(pinchStartScale * (distance / pinchStartDistance));
          offsetX = mid.x - pinchWorldX * scale;
          offsetY = mid.y - pinchWorldY * scale;
          applyViewportTransform();
        }
        return;
      }

      if (!isDragging) return;
      offsetX = startOffsetX + (event.clientX - dragStartX);
      offsetY = startOffsetY + (event.clientY - dragStartY);
      applyViewportTransform();
    });

    const endPointer = (event) => {
      pointers.delete(event.pointerId);
      if (flowCanvas.hasPointerCapture(event.pointerId)) {
        flowCanvas.releasePointerCapture(event.pointerId);
      }
      if (pointers.size < 2) {
        pinchStartDistance = 0;
      }
      if (pointers.size === 0) {
        isDragging = false;
        flowCanvas.classList.remove("dragging");
      }
    };

    flowCanvas.addEventListener("pointerup", endPointer);
    flowCanvas.addEventListener("pointercancel", endPointer);

    flowCanvas.addEventListener(
      "wheel",
      (event) => {
        // Keep right-click interactions from triggering zoom side effects.
        if ((event.buttons & 2) === 2) {
          return;
        }
        event.preventDefault();
        const rect = flowCanvas.getBoundingClientRect();
        const anchorX = event.clientX - rect.left;
        const anchorY = event.clientY - rect.top;
        const factor = event.deltaY < 0 ? 1.08 : 0.92;
        setScaleAt(scale * factor, anchorX, anchorY);
      },
      { passive: false }
    );

    zoomInBtn.addEventListener("click", () => {
      const rect = flowCanvas.getBoundingClientRect();
      setScaleAt(scale * 1.1, rect.width / 2, rect.height / 2);
    });

    zoomOutBtn.addEventListener("click", () => {
      const rect = flowCanvas.getBoundingClientRect();
      setScaleAt(scale * 0.9, rect.width / 2, rect.height / 2);
    });

    zoomSlider.addEventListener("input", () => {
      const rect = flowCanvas.getBoundingClientRect();
      const nextScale = Number(zoomSlider.value) / 100;
      setScaleAt(nextScale, rect.width / 2, rect.height / 2);
    });

    resetViewBtn.addEventListener("click", fitToView);
    window.addEventListener("resize", fitToView);
  }

  function setupHiddenToggleButton() {
    if (!hiddenToggleBtn) return;
    updateHiddenToggleButtonState();
    hiddenToggleBtn.addEventListener("click", () => {
      if (!hiddenCountInCurrentView) {
        showFlowToast("无隐藏对象");
        return;
      }

      hiddenPreviewOpen = !hiddenPreviewOpen;
      updateHiddenToggleButtonState();
      applyHiddenVisibilityToRenderedGraph();
      if (hiddenPreviewOpen) {
        showFlowToast(`显示${hiddenCountInCurrentView}个隐藏对象`);
      } else {
        showFlowToast(`隐藏了${hiddenCountInCurrentView}个对象`);
      }
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
    const params = getQueryParams();
    layoutSpacingL1 = parseLayoutL1(params.l1) ?? L1_MIN;
    layoutEngine = "elk";
    currentTraceMode = String(params.mode || "both").trim().toLowerCase();
    syncFlowModeClass();
    focusStartName = (params.start || "").trim();
    focusStartSource = (params.startSource || "").trim();
    focusStartType = (params.startType || "").trim();
    syncBackLink(params);
    setupBackHomeAction();
    const optionalFromParams = params.types.filter((type) => OPTIONAL_TYPES.includes(type));
    selectedTypes = params.types.length ? buildSelectedTypes(optionalFromParams) : getDefaultTypeList();
    loadHiddenObjectKeys()
      .catch(() => {
        hiddenObjectKeys = new Set();
      })
      .then(() => loadGraphData(params.start, params.mode, params.startSource, params.startType))
      .then((status) => {
        if (!status) return;
        if (status.resolvedStartName) {
          // Ensure seed highlight anchors to the actual node id returned by backend.
          focusStartName = status.resolvedStartName;
        }
        if (status.noStart) {
          if (flowTitleEl) {
            flowTitleEl.textContent = "数据流图（未选择起点对象）";
          }
          showFlowStatusOverlay("请先在主页搜索并选择起点对象，再生成数据流图。", { variant: "info" });
          return;
        }
        if (status.downgraded) {
          currentTraceMode = status.effectiveMode;
          syncFlowModeClass();
          syncBackLink({ start: focusStartName, startSource: focusStartSource, startType: focusStartType, mode: currentTraceMode, l1: String(getEffectiveL1()) });
          if (flowTitleEl) {
            flowTitleEl.textContent = "数据流图（已自动切换为向下追溯）";
          }
        } else if (flowTitleEl) {
          if (status.effectiveMode === "upstream") {
            flowTitleEl.textContent = "数据流图（向上追溯）";
          } else if (status.effectiveMode === "downstream") {
            flowTitleEl.textContent = "数据流图（向下追溯）";
          } else if (status.effectiveMode === "both") {
            flowTitleEl.textContent = "数据流图（向上+向下）";
          } else if (status.effectiveMode === "full") {
            flowTitleEl.textContent = "数据流图（全量数据流）";
          }
        }
      })
      .catch((error) => {
        graphData = { nodes: [], edges: [] };
        graphColorMap = MockData.colorMap || {};
        if (flowTitleEl) {
          flowTitleEl.textContent = "数据流图（未登录或后端不可用）";
        }
        const status = Number(error?.status || 0);
        const detail = String(error?.detail || "").trim();
        const message = String(error?.message || "");
        const isAuthError = status === 401 || status === 403 || message.includes("401") || message.includes("403");
        const isNetworkError = error?.code === "network_error" || /failed to fetch|network/i.test(message);
        if (isAuthError) {
          showFlowStatusOverlay("当前登录已失效，2 秒后返回主页。请重新登录后再打开数据流图。", { variant: "warning", autoBackMs: 2000 });
        } else if (isNetworkError) {
          showFlowStatusOverlay(`无法连接后端服务，请确认 API 服务已启动并可访问（${flowApiBase}）。`, { variant: "danger" });
        } else if (status >= 500) {
          showFlowStatusOverlay(`后端服务异常（HTTP ${status}）。请查看后端日志。${detail ? ` 详情：${detail}` : ""}`, { variant: "danger" });
        } else if (status >= 400) {
          showFlowStatusOverlay(`请求失败（HTTP ${status}）。${detail ? ` 详情：${detail}` : ""}`, { variant: "warning" });
        } else {
          showFlowStatusOverlay(`后端服务不可用或请求失败，当前未加载真实数据。请确认 API 服务已启动（${flowApiBase}）。${detail ? ` 详情：${detail}` : ""}`, { variant: "danger" });
        }
      })
      .finally(() => {
        buildGraphView(selectedTypes);
        renderLegend();
        renderIncludeFilters(selectedTypes);
        setupSideDrawers();
        setupMiniMapDrawer();
        setupLayoutSpreadControl();
        setupTextToggle();
        setupHiddenToggleButton();
        setupViewportInteraction();
        fitToView();
        startBackgroundAnimation();
      });
  }

  init();
})();
