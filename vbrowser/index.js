/*
 * Headless "virtual browser": a Node process that runs the EXACT same
 * extension/core.js pipeline as the real Chrome extension, plus a control API
 * for fault injection (browser exit before upload, interrupted download,
 * restart-and-recover). Files are really downloaded to disk via fetch.
 */
'use strict';

const http = require('node:http');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

const CORE = path.join(__dirname, '..', 'extension', 'core.js');

async function loadCore() {
  // core.js is an ES module; Node 24 supports dynamic import of absolute paths.
  return import(path.resolve(CORE) + `?v=${Date.now()}`);
}

function nowIso() {
  return new Date().toISOString();
}

class VirtualBrowser {
  constructor(opts) {
    this.portalUrl = opts.portalUrl;
    this.collectorUrl = opts.collectorUrl;
    this.downloadDir = opts.downloadDir;
    this.auto = !!opts.auto;               // follow SSE task.ready automatically
    this.running = new Map();              // download_id -> descriptor
    this.history = [];
    this.server = null;
    this.sseAbort = null;
    this.seenReady = new Set();
    fs.mkdirSync(this.downloadDir, { recursive: true });
  }

  adapter(crashBeforePost) {
    const self = this;
    return {
      nowIso,
      async postJson(url, body) {
        const r = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        return { status: r.status, body: await r.json().catch(() => ({})), headers: r.headers };
      },
      async fetchDownload(url) {
        const r = await fetch(url);
        if (!r.ok) throw new Error(`download http ${r.status}`);
        const buf = new Uint8Array(await r.arrayBuffer());
        const disp = r.headers.get('Content-Disposition') || '';
        const m = /filename="?([^"]+)"?/.exec(disp);
        return { bytes: buf, headers: r.headers, filename: m ? m[1] : 'report.csv' };
      },
      async persistDownload(manifest, bytes) {
        // Real on-disk artifact; same-name downloads get a unique id suffix so
        // nothing on the browser side is overwritten either.
        const safe = manifest.declared_filename.replace(/[^A-Za-z0-9._-]/g, '_');
        const target = path.join(self.downloadDir, `${manifest.download_id}__${safe}`);
        await fsp.writeFile(target, bytes);
        self.running.set(manifest.download_id, { manifest, saved_path: target });
        return target;
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
  }

  async startDownload(task, faultHooks = {}) {
    const core = await loadCore();
    const descriptor = { ...task, portal_url: this.portalUrl };
    const out = await core.runDownload(this.adapter(), descriptor, this.collectorUrl, faultHooks);
    this.history.unshift({ at: nowIso(), ...out });
    this.history = this.history.slice(0, 100);
    return out;
  }

  // Simulate the browser being killed mid-flow: a crash leaves the saved,
  // self-describing file on disk and sends nothing more to the collector.
  async crashBeforePost(task) {
    const core = await loadCore();
    return core.runDownload(this.adapter(), { ...task, portal_url: this.portalUrl },
      this.collectorUrl, { crashBeforePost: () => true });
  }

  // After restart: re-scan the download directory. Every file carries its own
  // manifest trailer, so no filename pairing is involved.
  async restartAndRecover() {
    const core = await loadCore();
    const recovered = [];
    const entries = await fsp.readdir(this.downloadDir);
    for (const name of entries) {
      const fp = path.join(this.downloadDir, name);
      const manifest = readTrailerManifest(fp);
      if (!manifest) continue;
      const bytes = new Uint8Array(await fsp.readFile(fp));
      // Replay only the file channel (events may already exist); collector is idempotent.
      const boundary = `----rb${crypto.randomBytes(12).toString('hex')}`;
      const multipart = core.buildMultipart(
        boundary, { manifest: JSON.stringify(manifest) },
        { file: { filename: manifest.declared_filename, contentType: 'text/csv', bytes } });
      const r = await fetch(`${this.collectorUrl}/api/envelopes`, {
        method: 'POST',
        headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}` },
        body: multipart,
      });
      recovered.push({ download_id: manifest.download_id, status: r.status,
                       body: await r.json().catch(() => ({})) });
    }
    this.history.unshift({ at: nowIso(), restart_recovered: recovered });
    return recovered;
  }

  async startSSE() {
    if (!this.auto || this.sseAbort) return;
    this.sseAbort = new AbortController();
    const listen = async () => {
      try {
        const r = await fetch(`${this.portalUrl}/api/events/stream`,
          { signal: this.sseAbort.signal, headers: { Accept: 'text/event-stream' } });
        let event = null;
        const reader = r.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          let idx;
          while ((idx = buf.indexOf('\n\n')) >= 0) {
            const block = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            let dataLine = '';
            for (const line of block.split('\n')) {
              if (line.startsWith('event:')) event = line.slice(6).trim();
              else if (line.startsWith('data:')) dataLine += line.slice(5).trim();
            }
            if (event === 'task.ready' && dataLine) {
              const payload = JSON.parse(dataLine);
              if (this.seenReady.has(payload.task_id)) continue; // duplicate SSE: ignore
              this.seenReady.add(payload.task_id);
              const task = await this._taskDescriptor(payload.task_id);
              if (task) this.startDownload(task).catch((e) =>
                this.history.unshift({ at: nowIso(), error: String(e) }));
            }
          }
        }
      } catch (e) {
        if (!this.sseAbort || !this.sseAbort.signal.aborted) {
          setTimeout(listen, 1000); // reconnect like a real browser
        }
      }
    };
    listen();
  }

  async _taskDescriptor(taskId) {
    const r = await fetch(`${this.portalUrl}/api/tasks/${taskId}`);
    if (!r.ok) return null;
    const t = await r.json();
    return { task_id: t.task_id, token: t.token, filter: t.filter, filename: t.filename };
  }

  listen(port) {
    this.server = http.createServer((req, res) => this.handle(req, res));
    this.server.listen(port, '127.0.0.1');
    if (this.auto) this.startSSE();
    return this.server;
  }

  async handle(req, res) {
    const json = (code, obj) => {
      const body = JSON.stringify(obj);
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(body);
    };
    const readBody = () => new Promise((resolve) => {
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks)) : {}); }
        catch { resolve({}); }
      });
    });
    const url = new URL(req.url, 'http://x');
    try {
      if (req.method === 'GET' && url.pathname === '/status') {
        return json(200, {
          running: [...this.running.values()], history: this.history.slice(0, 20),
          auto: this.auto, download_dir: this.downloadDir,
        });
      }
      if (req.method === 'POST' && url.pathname === '/download') {
        const body = await readBody();
        const task = body.task;
        if (!task || !task.task_id || !task.token) return json(400, { error: 'need task{task_id,token,...}' });
        const out = await this.startDownload(task, body.fault_hooks || {});
        return json(200, out);
      }
      if (req.method === 'POST' && url.pathname === '/crash-before-post') {
        const body = await readBody();
        const out = await this.crashBeforePost(body.task);
        return json(200, { crashed: true, ...out });
      }
      if (req.method === 'POST' && url.pathname === '/restart-recover') {
        const out = await this.restartAndRecover();
        return json(200, { recovered: out });
      }
      if (req.method === 'POST' && url.pathname === '/reset') {
        await fsp.rm(this.downloadDir, { recursive: true, force: true });
        fs.mkdirSync(this.downloadDir, { recursive: true });
        this.running.clear();
        this.history = [];
        this.seenReady.clear();
        return json(200, { ok: true });
      }
      return json(404, { error: 'not found' });
    } catch (e) {
      return json(500, { error: String(e && e.message || e) });
    }
  }
}

function readTrailerManifest(fp) {
  const buf = fs.readFileSync(fp);
  const marker = Buffer.from('\n# __MANIFEST__=');
  const idx = buf.lastIndexOf(marker);
  if (idx === -1) return null;
  const line = buf.slice(idx + 1).toString('utf8').trim();
  const b64 = line.slice('# __MANIFEST__='.length);
  try {
    return JSON.parse(Buffer.from(b64, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
}

function main() {
  const args = process.argv.slice(2);
  const opt = (k, d) => {
    const i = args.indexOf(`--${k}`);
    return i >= 0 ? args[i + 1] : d;
  };
  const vb = new VirtualBrowser({
    portalUrl: opt('portal', 'http://127.0.0.1:8080'),
    collectorUrl: opt('collector', 'http://127.0.0.1:8090'),
    downloadDir: opt('download-dir', '/workspace/runtime/vbrowser-downloads'),
    auto: args.includes('--auto'),
  });
  const port = Number(opt('port', '8095'));
  vb.listen(port);
  console.log(`virtual browser control API on http://127.0.0.1:${port}/status (auto=${vb.auto})`);
}

if (require.main === module) main();
module.exports = { VirtualBrowser, readTrailerManifest };
