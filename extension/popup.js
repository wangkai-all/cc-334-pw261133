'use strict';

const $ = (id) => document.getElementById(id);

async function load() {
  const cfg = await chrome.storage.local.get(['portalUrl', 'collectorUrl', 'history']);
  $('portalUrl').value = cfg.portalUrl || 'http://127.0.0.1:8080';
  $('collectorUrl').value = cfg.collectorUrl || 'http://127.0.0.1:8090';
  $('history').textContent = JSON.stringify(cfg.history || [], null, 2);
}

$('save').addEventListener('click', async () => {
  await chrome.storage.local.set({
    portalUrl: $('portalUrl').value.trim(),
    collectorUrl: $('collectorUrl').value.trim(),
  });
  $('saved').textContent = '已保存';
  setTimeout(() => ($('saved').textContent = ''), 1500);
});

load();
