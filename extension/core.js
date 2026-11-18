// Shared browser-side core: used by both the real Chrome MV3 service worker
// (extension/background.js) and the headless virtual browser (vbrowser/).
// All correlation is by task_id + token + download_id — never by filename or
// by "the most recent click".
'use strict';

export const MANIFEST_TRAILER_PREFIX = '# __MANIFEST__=';

export function uuid() {
  if (globalThis.crypto && globalThis.crypto.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  // Node fallback (no webcrypto.randomUUID in some builds)
  const b = require('node:crypto').randomBytes(16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

export async function sha256Hex(bytes) {
  if (globalThis.crypto && globalThis.crypto.subtle) {
    const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
    return Array.from(new Uint8Array(digest)).map((x) => x.toString(16).padStart(2, '0')).join('');
  }
  return require('node:crypto').createHash('sha256').update(Buffer.from(bytes)).digest('hex');
}

export function canonicalJson(obj) {
  // Match Python: sort_keys, compact separators, ensure_ascii=False
  const sort = (v) => {
    if (Array.isArray(v)) return v.map(sort);
    if (v && typeof v === 'object') {
      return Object.keys(v).sort().reduce((acc, k) => {
        acc[k] = sort(v[k]);
        return acc;
      }, {});
    }
    return v;
  };
  return JSON.stringify(sort(obj));
}

function b64url(jsonStr) {
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(jsonStr, 'utf8').toString('base64url');
  }
  const bytes = new TextEncoder().encode(jsonStr);
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

// TextEncoder is available in both service workers and Node 18+.
const enc = new TextEncoder();

export function appendManifestTrailer(bodyBytes, manifest) {
  // The report body normally ends in '\n'; add one if missing so the marker
  // always begins its own line. Stripping reproduces the exact original bytes
  // by cutting at the newline that precedes the marker.
  const line = MANIFEST_TRAILER_PREFIX + b64url(JSON.stringify(manifest));
  const needsNl = bodyBytes.length === 0 || bodyBytes[bodyBytes.length - 1] !== 0x0a;
  const prefix = needsNl ? enc.encode('\n') : new Uint8Array(0);
  const trailer = enc.encode(line);
  const out = new Uint8Array(bodyBytes.length + prefix.length + trailer.length);
  out.set(bodyBytes, 0);
  out.set(prefix, bodyBytes.length);
  out.set(trailer, bodyBytes.length + prefix.length);
  return out;
}

// Minimal multipart/form-data encoder (binary safe).
export function buildMultipart(boundary, fields, files) {
  const chunks = [];
  for (const [name, value] of Object.entries(fields)) {
    chunks.push(enc.encode(
      `--${boundary}\r\nContent-Disposition: form-data; name="${name}"\r\n\r\n${value}\r\n`));
  }
  for (const [name, file] of Object.entries(files)) {
    chunks.push(enc.encode(
      `--${boundary}\r\nContent-Disposition: form-data; name="${name}"; ` +
      `filename="${file.filename}"\r\nContent-Type: ${file.contentType}\r\n\r\n`));
    chunks.push(file.bytes);
    chunks.push(enc.encode('\r\n'));
  }
  chunks.push(enc.encode(`--${boundary}--\r\n`));
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.length;
  }
  return out;
}

/**
 * Execute one download end to end.
 *
 * adapter abstracts the platform:
 *   { fetchJson(method,url,body), fetchDownload(url, headers) ->
 *       {bytes, headers:{get(k)}, filename}, nowIso() }
 *
 * faultHooks (all optional):
 *   crashBeforePost(): if true, save the self-describing file then stop
 *                      (simulates browser exiting before the upload channel).
 *   onSpool(bytes, manifest): override where the self-describing file is kept.
 */
export async function runDownload(adapter, task, collectorUrl, faultHooks = {}) {
  const downloadId = uuid();
  const url = `${task.portal_url}/api/downloads/${task.task_id}?token=${encodeURIComponent(task.token)}`;
  const startedAt = adapter.nowIso();

  // Event channel: "created" notification, keyed by download_id.
  await adapter.postJson(`${collectorUrl}/api/events`, {
    download_id: downloadId,
    kind: 'created',
    task_id: task.task_id,
    at: startedAt,
  });

  let fetched;
  try {
    fetched = await adapter.fetchDownload(url, { 'X-Task-Id': task.task_id });
  } catch (err) {
    await adapter.postJson(`${collectorUrl}/api/events`, {
      download_id: downloadId, kind: 'interrupted',
      task_id: task.task_id, reason: String(err && err.message || err), at: adapter.nowIso(),
    });
    throw err;
  }

  const headerToken = (fetched.headers.get('X-Task-Token') || '').trim();
  const headerTaskId = (fetched.headers.get('X-Task-Id') || '').trim();
  // Defensive cross-check: the response must identify the same task we asked for.
  if (headerTaskId && headerTaskId !== task.task_id) {
    throw new Error(`task id mismatch: requested ${task.task_id}, got ${headerTaskId}`);
  }
  if (headerToken && headerToken !== task.token) {
    throw new Error(`token mismatch on download response for ${task.task_id}`);
  }

  const bodyHash = await sha256Hex(fetched.bytes);
  const finishedAt = adapter.nowIso();
  const manifest = {
    manifest_version: 1,
    download_id: downloadId,
    task_id: task.task_id,
    token: task.token,
    portal_url: task.portal_url,
    filter: task.filter,
    declared_filename: task.filename || fetched.filename,
    size_bytes: fetched.bytes.length,
    sha256: bodyHash,  // canonical report body (trailer excluded on the worker side)
    download_started_at: startedAt,
    download_finished_at: finishedAt,
  };

  // Self-describing file: enables recovery if the browser exits before POSTing.
  const finalBytes = appendManifestTrailer(fetched.bytes, manifest);
  // Transport hash covers the exact bytes posted to the collector; the content
  // hash (manifest.sha256) covers the canonical body the portal signed.
  manifest.transport_sha256 = await sha256Hex(finalBytes);
  await adapter.persistDownload(manifest, finalBytes);

  if (faultHooks.crashBeforePost && faultHooks.crashBeforePost(manifest)) {
    await adapter.postJson(`${collectorUrl}/api/events`, {
      download_id: downloadId, kind: 'interrupted', task_id: task.task_id,
      reason: 'browser_exit_before_envelope_post', at: adapter.nowIso(),
    });
    return { download_id: downloadId, crashed: true, manifest };
  }

  // Event channel first (allowed to arrive before the file channel),
  // then the file channel. The collector tolerates either order.
  await adapter.postJson(`${collectorUrl}/api/events`, {
    download_id: downloadId, kind: 'completed',
    task_id: task.task_id, manifest, at: adapter.nowIso(),
  });

  const boundary = `----rb${uuid().replace(/-/g, '')}`;
  const multipart = buildMultipart(boundary, { manifest: JSON.stringify(manifest) }, {
    file: { filename: manifest.declared_filename, contentType: 'text/csv', bytes: finalBytes },
  });
  const resp = await adapter.postMultipart(
    `${collectorUrl}/api/envelopes`, boundary, multipart);
  return { download_id: downloadId, crashed: false, manifest, collector: resp };
}
