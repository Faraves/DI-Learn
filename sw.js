/**
 * DataDojo: Financial & ETL Optimizer
 * Service Worker (sw.js) — 100% Offline-First Cache-First Strategy
 * Stores App Shell, Challenge Catalog, Evaluator Python AST script, and WebAssembly binaries (DuckDB-Wasm & Pyodide).
 */

const CACHE_VERSION = 'datadojo-v1.2.0';
const APP_SHELL_CACHE = `${CACHE_VERSION}-shell`;
const WASM_CDN_CACHE = `${CACHE_VERSION}-wasm-cdn`;

const CORE_ASSETS = [
  './',
  './index.html',
  './manifest.json',
  './assets/js/seed_data.js',
  './assets/js/engine.js',
  './assets/js/evaluator.py',
  './challenges/catalog.json',
  './assets/icons/icon-192.svg',
  './assets/icons/icon-512.svg'
];

// External CDN assets to pre-cache best-effort during installation
const CDN_PRECACHE_ASSETS = [
  'https://cdn.tailwindcss.com',
  'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.css',
  'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/theme/dracula.min.css',
  'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/sql/sql.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/python/python.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/r/r.min.js'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    (async () => {
      const shellCache = await caches.open(APP_SHELL_CACHE);
      await shellCache.addAll(CORE_ASSETS);

      const cdnCache = await caches.open(WASM_CDN_CACHE);
      await Promise.allSettled(
        CDN_PRECACHE_ASSETS.map(async (url) => {
          try {
            const response = await fetch(url, { mode: 'cors' });
            if (response.ok) {
              await cdnCache.put(url, response);
            }
          } catch (err) {
            // Non-blocking if offline during install update
            console.warn('[SW] Precache CDN skipped:', url, err);
          }
        })
      );

      await self.skipWaiting();
    })()
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys.map((key) => {
          if (key !== APP_SHELL_CACHE && key !== WASM_CDN_CACHE) {
            return caches.delete(key);
          }
          return null;
        })
      );
      await self.clients.claim();
    })()
  );
});

/**
 * Determines if a URL is a WebAssembly / Pyodide / DuckDB / CDN dependency
 */
function isWasmOrCdnAsset(url) {
  return (
    url.pathname.endsWith('.wasm') ||
    url.pathname.endsWith('.whl') ||
    url.pathname.endsWith('.zip') ||
    url.pathname.endsWith('.tar') ||
    url.hostname.includes('jsdelivr.net') ||
    url.hostname.includes('cdnjs.cloudflare.com') ||
    url.hostname.includes('tailwindcss.com') ||
    url.hostname.includes('unpkg.com') ||
    url.hostname.includes('fonts.googleapis.com') ||
    url.hostname.includes('fonts.gstatic.com')
  );
}

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  const url = new URL(request.url);

  // Cache-First strategy for both App Shell and WASM/CDN binaries
  event.respondWith(
    (async () => {
      const cachedResponse = await caches.match(request, { ignoreSearch: false });
      if (cachedResponse) {
        return cachedResponse;
      }

      try {
        const networkResponse = await fetch(request);
        if (
          networkResponse &&
          (networkResponse.status === 200 || networkResponse.type === 'opaque')
        ) {
          const targetCacheName = isWasmOrCdnAsset(url) ? WASM_CDN_CACHE : APP_SHELL_CACHE;
          const cache = await caches.open(targetCacheName);
          cache.put(request, networkResponse.clone());
        }
        return networkResponse;
      } catch (error) {
        // Fallback to index.html for navigation requests when in airplane mode
        if (request.mode === 'navigate') {
          const fallback = await caches.match('./index.html');
          if (fallback) return fallback;
        }
        throw error;
      }
    })()
  );
});

// Listen for explicit cache-warmup or skipWaiting messages from UI
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
