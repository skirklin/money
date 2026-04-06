/**
 * Money Data Collector — Chrome Extension Background Service Worker
 *
 * Architecture:
 *   Per-institution handlers in ./institutions/*.js define how to capture data
 *   from each financial site. This service worker is the router — it matches
 *   URLs to handlers and provides a context object with capture tools.
 *
 * Data flow:
 *   1. User navigates to a financial site
 *   2. webNavigation.onCompleted fires → URL matched to a handler
 *   3. Handler's onPageLoad(ctx) is called with a context providing:
 *      - captureCookies()     — read cookies via chrome.cookies API, send to server
 *      - startNetworkRecording() — inject page interceptor to capture fetch/XHR
 *      - executeInPage(fn)    — inject and run a function in the page's JS context
 *      - flushNetworkLog()    — send buffered network entries to server
 *      - getLastSync()        — ask server for last known data date
 *   4. Server receives data at POST /cookies or POST /network-log
 *   5. Server triggers institution-specific sync/parse logic
 *
 * Handler types:
 *   - Cookie-based (betterment, wealthfront, capital_one): capture cookies,
 *     server replays API calls using those cookies
 *   - Network-log (chase, morgan_stanley): record API responses from the page,
 *     server parses the captured responses directly
 *   - Hybrid (ally): record network traffic + inject fetch calls using a
 *     captured auth token, server parses the captured responses
 *
 * The popup (popup.js) provides manual cookie/recording controls as fallback.
 */

import ally from "./institutions/ally.js";
import betterment from "./institutions/betterment.js";
import capital_one from "./institutions/capital_one.js";
import chase from "./institutions/chase.js";
import fidelity from "./institutions/fidelity.js";
import morgan_stanley from "./institutions/morgan_stanley.js";
import wealthfront from "./institutions/wealthfront.js";

const DEFAULT_PORT = 5555;

// ── Institution registry ─────────────────────────────────────────────

const HANDLERS = {
  ally,
  betterment,
  capital_one,
  chase,
  fidelity,
  morgan_stanley,
  wealthfront,
};

function getInstitutionForUrl(url) {
  if (!url) return null;
  for (const [id, handler] of Object.entries(HANDLERS)) {
    for (const domain of handler.domains) {
      if (url.includes(domain.replace(/^\./, ""))) return id;
    }
  }
  return null;
}

// ── Server communication ─────────────────────────────────────────────

async function getServerUrl() {
  const result = await chrome.storage.local.get("serverPort");
  const port = result.serverPort || DEFAULT_PORT;
  return `http://localhost:${port}`;
}

async function sendToServer(endpoint, data) {
  const baseUrl = await getServerUrl();
  try {
    const response = await fetch(`${baseUrl}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    });
    if (!response.ok) {
      const text = await response.text();
      return { success: false, error: `Server returned ${response.status}: ${text}` };
    }
    return { success: true, data: await response.json() };
  } catch (err) {
    return { success: false, error: `Connection failed: ${err.message}` };
  }
}

async function checkServerHealth() {
  const baseUrl = await getServerUrl();
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 3000);
    const response = await fetch(`${baseUrl}/health`, { signal: controller.signal });
    clearTimeout(timeout);
    return response.ok;
  } catch {
    return false;
  }
}

// ── Badge & logging ──────────────────────────────────────────────────

function setBadge(text, color = "#34d399") {
  chrome.action.setBadgeText({ text });
  chrome.action.setBadgeBackgroundColor({ color });
}

async function logActivity(message) {
  const result = await chrome.storage.local.get("activityLog");
  const log = result.activityLog || [];
  log.unshift({ time: new Date().toISOString(), message });
  await chrome.storage.local.set({ activityLog: log.slice(0, 20) });
}

// ── Cookie capture ───────────────────────────────────────────────────

async function getAllCookiesForDomains(domains) {
  const allCookies = [];
  const seen = new Set();
  for (const domain of domains) {
    const byDomain = await chrome.cookies.getAll({ domain });
    const byUrl = await chrome.cookies.getAll({ url: `https://${domain.replace(/^\./, "")}` });
    for (const cookie of [...byDomain, ...byUrl]) {
      const key = `${cookie.domain}|${cookie.name}|${cookie.path}`;
      if (!seen.has(key)) {
        seen.add(key);
        allCookies.push(cookie);
      }
    }
  }
  return allCookies;
}

async function sendCookiesToServer(institution, cookies) {
  if (cookies.length === 0) return { success: false, error: "No cookies" };
  const result = await sendToServer("/cookies", {
    institution,
    cookies,
    captured_at: new Date().toISOString(),
  });
  // Propagate sync_id from server response
  if (result.success && result.data?.sync_id) {
    result.sync_id = result.data.sync_id;
  }
  return result;
}

// ── Network recording state ──────────────────────────────────────────

const networkBuffer = {};    // institution -> [entries]
const recordingTabs = {};    // tabId -> institution

function updateRecordingBadge() {
  const institutions = Object.values(recordingTabs);
  if (institutions.length === 0) { setBadge(""); return; }
  let total = 0;
  for (const inst of institutions) {
    total += (networkBuffer[inst] || []).length;
  }
  setBadge(String(total), total > 0 ? "#818cf8" : "#34d399");
}

async function flushInstitution(institution) {
  const entries = networkBuffer[institution] || [];
  if (entries.length === 0) return null;
  const result = await sendToServer("/network-log", {
    institution, entries, captured_at: new Date().toISOString(),
  });
  delete networkBuffer[institution];
  return result;
}

// ── Handler context ──────────────────────────────────────────────────
// Passed to each handler's onPageLoad(ctx).

function makeContext(institution, tabId) {
  const handler = HANDLERS[institution];

  return {
    institution,
    tabId,

    async captureCookies() {
      const cookies = await getAllCookiesForDomains(handler.domains);
      const result = await sendCookiesToServer(institution, cookies);
      return { type: "cookies", count: cookies.length, ...result };
    },

    async startNetworkRecording() {
      networkBuffer[institution] = [];
      recordingTabs[tabId] = institution;
      try {
        await chrome.tabs.sendMessage(tabId, { type: "START_RECORDING" });
        updateRecordingBadge();
        logActivity(`Started recording for ${institution}`);
      } catch (err) {
        console.warn(`[Money] Could not start recording for ${institution}:`, err);
      }
    },

    async executeInPage(fn, args = undefined) {
      // Inject as a <script> tag so it runs with the page's full session.
      await chrome.scripting.executeScript({
        target: { tabId },
        func: (fnSource, fnArgs) => {
          const script = document.createElement("script");
          script.textContent = `(${fnSource})(${JSON.stringify(fnArgs)});`;
          document.documentElement.appendChild(script);
          script.remove();
        },
        args: [fn.toString(), args],
        world: "MAIN",
      });
    },

    findCapturedToken() {
      const entries = networkBuffer[institution] || [];
      for (const entry of entries) {
        const body = entry.responseBody;
        if (!body || typeof body !== "object") continue;
        const token = body?.data?.data?.json_data?.access_token;
        if (token) return token;
        if (body?.data?.access_token) return body.data.access_token;
      }
      return null;
    },

    async getLastSync() {
      const baseUrl = await getServerUrl();
      try {
        const resp = await fetch(`${baseUrl}/api/last-sync?institution=${institution}`);
        if (resp.ok) return await resp.json();
      } catch { /* server not available */ }
      return null;
    },

    getBufferedEntries() {
      return networkBuffer[institution] || [];
    },

    async flushNetworkLog() {
      const entries = networkBuffer[institution] || [];
      if (entries.length === 0) {
        return { type: "network_log", count: 0, success: true };
      }
      const toSend = [...entries];
      networkBuffer[institution] = [];
      const result = await sendToServer("/network-log", {
        institution,
        entries: toSend,
        captured_at: new Date().toISOString(),
      });
      if (result.success) {
        updateRecordingBadge();
        logActivity(`Flushed ${toSend.length} network entries for ${institution}`);
      }
      // Propagate sync_id from server response
      if (result.success && result.data?.sync_id) {
        result.sync_id = result.data.sync_id;
      }
      return { type: "network_log", count: toSend.length, ...result };
    },

    async pollSyncResult(syncId, { timeout = 120000, interval = 2000 } = {}) {
      const baseUrl = await getServerUrl();
      const deadline = Date.now() + timeout;
      while (Date.now() < deadline) {
        try {
          const resp = await fetch(`${baseUrl}/api/sync/${syncId}`);
          if (resp.ok) {
            const data = await resp.json();
            if (data.status === "complete" || data.status === "error") {
              return data;
            }
          }
        } catch { /* server unavailable, keep polling */ }
        await new Promise(resolve => setTimeout(resolve, interval));
      }
      return { status: "timeout" };
    },
  };
}

// ── Navigation handler (the router) ──────────────────────────────────

const lastCapture = {};
const COOLDOWN_MS = 30_000;
const activeHandlers = new Set();

async function handleNavigation(details) {
  if (details.frameId !== 0) return;

  const institution = getInstitutionForUrl(details.url);
  if (!institution) return;

  const handler = HANDLERS[institution];
  if (!handler) return;

  if (handler.authPattern && !handler.authPattern.test(details.url)) return;

  const now = Date.now();
  if (lastCapture[institution] && now - lastCapture[institution] < COOLDOWN_MS) return;

  const ctx = makeContext(institution, details.tabId);
  activeHandlers.add(institution);
  try {
    const result = await handler.onPageLoad(ctx);

    // Only set cooldown on success — failed attempts (e.g. login page) should retry
    if (result?.status === "complete" || result?.success !== false) {
      lastCapture[institution] = Date.now();
    }

    if (result?.status === "error") {
      setBadge("!", "#f87171");
      logActivity(`${institution}: sync failed — ${result.error_message || "unknown error"}`);
    } else if (result?.status === "complete") {
      setBadge("✓");
      const parts = [];
      if (result.accounts) parts.push(`${result.accounts} accounts`);
      if (result.transactions) parts.push(`${result.transactions} new transactions`);
      if (result.balances) parts.push(`${result.balances} new balances`);
      if (result.holdings) parts.push(`${result.holdings} new holdings`);
      const desc = parts.length > 0
        ? `${institution}: ${parts.join(", ")}`
        : `${institution}: synced (no new data)`;
      logActivity(desc);
      console.log(`[Money] ${desc}`);
    } else if (result?.status === "timeout") {
      setBadge("?", "#fbbf24");
      logActivity(`${institution}: sync timed out`);
    } else if (result && result.success !== false) {
      setBadge("✓");
      const desc = result.count
        ? `Captured ${result.count} entries for ${institution}`
        : `Completed ${institution}`;
      logActivity(desc);
      console.log(`[Money] ${desc}`);
    } else {
      setBadge("!", "#f87171");
      logActivity(`Failed for ${institution}: ${result?.error || "unknown"}`);
    }
  } catch (err) {
    setBadge("!", "#f87171");
    console.error(`[Money] Handler error for ${institution}:`, err);
    logActivity(`Error for ${institution}: ${err.message}`);
  } finally {
    activeHandlers.delete(institution);
  }
}

// Full page loads
chrome.webNavigation.onCompleted.addListener(handleNavigation);

// SPA route changes (History API pushState/replaceState) — needed for sites
// like Capital One where login redirects to /accountSummary via client-side routing
chrome.webNavigation.onHistoryStateUpdated.addListener(handleNavigation);

// ── Periodic network flush (for passive recording like Chase) ────────

setInterval(async () => {
  for (const [tabId, institution] of Object.entries(recordingTabs)) {
    if (activeHandlers.has(institution)) continue;

    const entries = networkBuffer[institution];
    if (!entries || entries.length === 0) continue;

    const toSend = [...entries];
    networkBuffer[institution] = [];

    const result = await sendToServer("/network-log", {
      institution,
      entries: toSend,
      captured_at: new Date().toISOString(),
    });
    if (result.success) {
      updateRecordingBadge();
      logActivity(`Flushed ${toSend.length} network entries for ${institution}`);
    } else {
      setBadge("!", "#f87171");
    }
  }
}, 30_000);

// ── Tab navigation/close → flush buffered network data ───────────────

chrome.tabs.onUpdated.addListener(async (tabId, changeInfo) => {
  if (changeInfo.url === undefined) return;
  const institution = recordingTabs[tabId];
  if (!institution) return;
  if (getInstitutionForUrl(changeInfo.url) === institution) return;

  delete recordingTabs[tabId];
  await flushInstitution(institution);
  updateRecordingBadge();
});

chrome.tabs.onRemoved.addListener(async (tabId) => {
  const institution = recordingTabs[tabId];
  if (!institution) return;

  delete recordingTabs[tabId];
  await flushInstitution(institution);
  updateRecordingBadge();
});

// ── Message handler (popup + content scripts) ────────────────────────

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "CHECK_HEALTH") {
    checkServerHealth().then(healthy => sendResponse({ healthy }));
    return true;
  }

  // Manual cookie capture from popup
  if (message.type === "CAPTURE_COOKIES") {
    const handler = HANDLERS[message.institution];
    if (!handler) {
      sendResponse({ success: false, error: `Unknown institution: ${message.institution}` });
      return true;
    }
    getAllCookiesForDomains(handler.domains).then(async cookies => {
      const result = await sendCookiesToServer(message.institution, cookies);
      sendResponse({ ...result, count: cookies.length });
    });
    return true;
  }

  // Network entry from page interceptor (content script relay)
  if (message.type === "NETWORK_REQUEST") {
    const tabId = sender.tab?.id;
    const institution = tabId ? recordingTabs[tabId] : null;
    if (institution && message.entry) {
      const handler = HANDLERS[institution];
      const url = message.entry.url || "";
      if (!handler?.captureFilter || handler.captureFilter.test(url)) {
        if (!networkBuffer[institution]) networkBuffer[institution] = [];
        networkBuffer[institution].push(message.entry);
        updateRecordingBadge();
      }
    }
    sendResponse({ buffered: true });
    return true;
  }

  // Manual recording controls from popup
  if (message.type === "START_NETWORK_RECORDING") {
    const { tabId, institution } = message;
    networkBuffer[institution] = [];
    recordingTabs[tabId] = institution;
    chrome.scripting.executeScript({
      target: { tabId },
      files: ["content/network_recorder.js"],
    }).then(() => chrome.tabs.sendMessage(tabId, { type: "START_RECORDING" }))
      .then(sendResponse)
      .catch(err => sendResponse({ started: false, error: err.message }));
    return true;
  }

  if (message.type === "STOP_NETWORK_RECORDING") {
    const { tabId, institution } = message;
    delete recordingTabs[tabId];
    chrome.tabs.sendMessage(tabId, { type: "STOP_RECORDING" }).then(async () => {
      const entries = networkBuffer[institution] || [];
      const result = await sendToServer("/network-log", {
        institution, entries, captured_at: new Date().toISOString(),
      });
      delete networkBuffer[institution];
      sendResponse({ ...result, count: entries.length });
    }).catch(async () => {
      const entries = networkBuffer[institution] || [];
      const result = await sendToServer("/network-log", {
        institution, entries, captured_at: new Date().toISOString(),
      });
      delete networkBuffer[institution];
      sendResponse({ ...result, count: entries.length });
    });
    return true;
  }

  if (message.type === "GET_RECORDING_STATUS") {
    const { tabId } = message;
    const institution = recordingTabs[tabId] || null;
    const count = institution ? (networkBuffer[institution] || []).length : 0;
    sendResponse({ recording: !!institution, institution, count });
    return true;
  }

  // Content script checking if it should auto-start recording
  if (message.type === "CHECK_RECORDING_FOR_TAB") {
    const tabId = sender.tab?.id;
    const url = sender.tab?.url || "";

    // Already recording this tab
    if (tabId && tabId in recordingTabs) {
      sendResponse({ shouldRecord: true });
      return true;
    }

    // Auto-start recording for institutions with autoRecord: true
    const institution = getInstitutionForUrl(url);
    if (institution && tabId) {
      const handler = HANDLERS[institution];
      if (handler?.autoRecord) {
        networkBuffer[institution] = networkBuffer[institution] || [];
        recordingTabs[tabId] = institution;
        updateRecordingBadge();
        sendResponse({ shouldRecord: true });
        return true;
      }
    }

    sendResponse({ shouldRecord: false });
    return true;
  }
});
