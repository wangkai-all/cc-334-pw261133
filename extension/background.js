// Chrome MV3 service worker. The portal page posts a message with the task
// descriptor (task_id, token, filter, portal_url); this worker performs the
// real download via fetch + chrome.downloads, then runs the shared pipeline.
'use strict';

import { runDownload } from './core.js';

const DEFAULTS = { portalUrl: 'http://127.0.0.1:8080', collectorUrl: 'http://127.0.0.1:8090' };

async function cfg() {
  return { ...DEFAULTS, ...(await chrome.storage.local.get(Object.keys(DEFAULTS))) };
}

function nowIso() {
  return new Date().toISOString();
}

const adapter = {
  nowIso,
  async postJson(url, body) {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { status: r.status, body: await r.json().catch(() => ({})) };
  },
  async fetchDownload(url) {
    const r = await fetch(url, { credentials: 'omit' });
    if (!r.ok) throw new Error(`download http ${r.status}`);
    const buf = await r.arrayBuffer();
    const disp = r.headers.get('Content-Disposition') || '';
    const m = /filename="?([^"]+)"?/.exec(disp);
    return {
      bytes: new Uint8Array(buf),
      headers: r.headers,
      filename: m ? m[1] : 'report.csv',
    };
  },
  async persistDownload(manifest, bytes) {
    // Real browser download: hand the self-describing bytes to chrome.downloads.
    const blob = new Blob([bytes], { type: 'text/csv' });
    const objectUrl = URL.createObjectURL(blob);
    await new Promise((resolve, reject) => {
      chrome.downloads.download(
        { url: objectUrl, filename: `report-portal/${manifest.declared_filename}`,
          conflictAction: 'uniquify', saveAs: false },
        (id) => {
          if (chrome.runtime.lastError) reject(new Error(chrome.runtime.lastError.message));
          else resolve(id);
        });
    });
    // The object URL is revoked later by the downloads.onChanged listener.
    await chrome.storage.local.get(['objectUrls']).then((s) => {
      const urls = s.objectUrls || [];
      urls.push(objectUrl);
      return chrome.storage.local.set({ objectUrls: urls });
    });
  },
  async postMultipart(url, boundary, bytes) {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}` },
      body: bytes,
    });
    return { status: r.status, body: await r.json().catch(() => ({})) };
  },
};

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type !== 'START_REPORT_DOWNLOAD') return false;
  cfg().then(async (c) => {
    try {
      const out = await runDownload(adapter, {
        ...msg.task,
        portal_url: msg.task.portal_url || c.portalUrl,
      }, c.collectorUrl, msg.faultHooks || {});
      await recordHistory({ ok: true, ...out });
      sendResponse({ ok: true, ...out });
    } catch (err) {
      await recordHistory({ ok: false, error: String(err && err.message || err), at: nowIso() });
      sendResponse({ ok: false, error: String(err && err.message || err) });
    }
  });
  return true; // async response
});

async function recordHistory(entry) {
  const { history = [] } = await chrome.storage.local.get('history');
  history.unshift(entry);
  await chrome.storage.local.set({ history: history.slice(0, 50) });
}

// Revoke blob URLs once their download completes.
chrome.downloads.onChanged.addListener((delta) => {
  if (delta.state && delta.state.current !== 'in_progress') {
    chrome.storage.local.get(['objectUrls']).then((s) => {
      for (const u of s.objectUrls || []) URL.revokeObjectURL(u);
      chrome.storage.local.set({ objectUrls: [] });
    });
  }
});
