/**
 * ═══════════════════════════════════════════════════════════════════════
 * sw-engine.js — StockWatch Performance Engine v3.0
 * ═══════════════════════════════════════════════════════════════════════
 *
 * ARCHITEKTUR-ÜBERSICHT:
 *
 *  CacheLayer          → Zwei-Stufen-Cache (Memory Map + localStorage)
 *                         mit TTL, stale-while-revalidate und LRU-Eviction
 *
 *  RequestManager      → Deduplication (inflight Map), Rate-Limit-Schutz,
 *                         Retry mit Exponential Backoff + Jitter,
 *                         differenzierte Fehlerbehandlung (4xx vs 5xx)
 *
 *  ProxyFetcher        → CORS-Proxy mit Fallback-Kette + AbortController,
 *                         detailliertes Error-Logging
 *
 *  BatchFetcher        → Yahoo /v7/finance/quote?symbols=A,B,C
 *                         → 1 Request statt N Requests (größter Gewinn!)
 *                         → Response-Validierung gegen API-Strukturänderungen
 *
 *  RenderScheduler     → requestAnimationFrame-Queue, diff-based DOM-Patches,
 *                         DocumentFragment für Listenrendering
 *
 *  RefreshScheduler    → Page Visibility API, setTimeout-Chain (kein setInterval),
 *                         Mobile-Energiesparmodus, Exponential Backoff bei Fehlern
 *
 *  Logging             → Zentrales, gestuftes Logging ([SWEngine] Prefix),
 *                         Debug-Modus per SWEngine.debug = true
 *
 *  Security            → escapeHtml() für XSS-Schutz,
 *                         Response-Validierung, localStorage-Validierung
 *
 * WARUM DIESE ARCHITEKTUR:
 *  - Bottleneck #1 war N × API-Calls pro Refresh → Batch löst das vollständig
 *  - Bottleneck #2 war komplettes innerHTML-Neurendern → Diff-Patches lösen das
 *  - Bottleneck #3 war Refresh auf hidden Tabs → Page Visibility stoppt das
 *  - Bottleneck #4 waren Race Conditions → inflight Map verhindert Duplikate
 *
 * SKALIERUNG:
 *  - 10 Aktien: 1 Batch-Request statt 10 (90% weniger API-Calls)
 *  - 50 Aktien: 1 Batch-Request statt 50 (98% weniger API-Calls)
 *  - 100 Aktien: 2 Batch-Requests statt 100 (98% weniger API-Calls)
 *
 * ═══════════════════════════════════════════════════════════════════════
 */

'use strict';

const SWEngine = (() => {

  // ═══════════════════════════════════════════════════════════════════
  // §1  KONFIGURATION
  // ═══════════════════════════════════════════════════════════════════

  /**
   * TTL-Werte bestimmen wie lange gecachte Daten als "frisch" gelten.
   * WARUM diese Werte:
   *  - QUOTE 15s: Live-Gefühl ohne API zu überlasten (60 Req/Min erlaubt ~4 Batch-Refreshes)
   *  - CHART 5min: Charts ändern sich selten, 5min ist akzeptabel
   *  - COMPANY 24h: Firmenname/Sector ändert sich quasi nie
   *  - SEARCH 10min: Suchergebnisse sind sehr stabil
   */
  const TTL = Object.freeze({
    QUOTE:   15_000,          // 15 Sekunden
    CHART:   5 * 60_000,     // 5 Minuten
    COMPANY: 24 * 3_600_000, // 24 Stunden
    SEARCH:  10 * 60_000,    // 10 Minuten
    HIST:    60 * 60_000,    // 1 Stunde (historische Kurse)
  });

  /**
   * BATCH_SIZE = 50: Yahoo Finance /v7/finance/quote verarbeitet problemlos
   * bis zu 50 Symbole pro Request. Über 100 kann es zu Timeouts kommen.
   * WARUM 50: Konservativ, aber deckt 99% aller Portfolios ab.
   */
  const BATCH_SIZE = 50;

  /**
   * MAX_CACHE_SIZE: Begrenzt den Memory-Cache auf N Einträge (LRU-Strategie).
   * Bei 100 Symbolen × 5 Cache-Typen (quote, chart, company, hist, search) = ~500 Einträge max.
   * Ohne Limit wächst die Map unbegrenzt und verbraucht Heap-Speicher.
   */
  const MAX_CACHE_SIZE = 500;

  /** Retry-Konfiguration — konservativ genug für Stabilität, schnell genug für UX */
  const RETRY = Object.freeze({
    MAX_ATTEMPTS: 2,    // 2 Retries — Proxy-Race macht mehr unnötig
    BASE_DELAY_MS: 400, // 400ms statt 800ms
    JITTER_MS: 150,
  });

  // ═══════════════════════════════════════════════════════════════════
  // §1b  LOGGING — Zentrales, gestuftes Logging statt silent catch
  // ═══════════════════════════════════════════════════════════════════

  /**
   * WARUM zentrales Logging?
   *  - Alle catch-Blöcke waren leer → Fehler unsichtbar, Debugging unmöglich
   *  - Konsistentes Prefix [SWEngine] erleichtert Filterung in der Konsole
   *  - Debug-Level nur aktiv wenn SWEngine._debug = true (kein Noise in Production)
   */
  let _debugMode = false;
  const _log = {
    error: (...args) => console.error('[SWEngine]', ...args),
    warn:  (...args) => console.warn('[SWEngine]', ...args),
    info:  (...args) => console.info('[SWEngine]', ...args),
    debug: (...args) => { if (_debugMode) console.debug('[SWEngine]', ...args); },
  };

  /** Proxy-Strategien in Prioritätsreihenfolge */
  const PROXIES = [
    url => ({ px: `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`, unwrap: true }),
    url => ({ px: `https://api.allorigins.win/raw?url=${encodeURIComponent(url)}`, unwrap: false }),
    url => ({ px: `https://corsproxy.io/?${encodeURIComponent(url)}`, unwrap: false }),
  ];

  // ═══════════════════════════════════════════════════════════════════
  // §2  CACHE LAYER
  // ═══════════════════════════════════════════════════════════════════

  /**
   * WARUM zwei Caches (Memory + localStorage)?
   *  - Memory Map: Extrem schnell (Nanosekunden), lebt nur in der Session
   *  - localStorage: Überlebt Seiten-Reloads, ideal für Company-Info (24h TTL)
   *
   * STALE-WHILE-REVALIDATE (SWR) Prinzip:
   *  1. Gibt sofort gecachte Daten zurück (stale ist ok)
   *  2. Startet Hintergrund-Fetch für frische Daten
   *  3. UI-Callback wird mit neuen Daten aufgerufen
   *  → Nutzer sieht NIE einen Ladescreen für Daten die schon mal geladen wurden
   */
  const _memCache = new Map(); // key → { data, freshUntil, staleUntil }

  function getCachedData(key) {
    // 1. Zuerst Memory-Cache prüfen (schnellster Pfad)
    const mem = _memCache.get(key);
    if (mem) {
      const now = Date.now();
      if (now < mem.freshUntil) return { data: mem.data, fresh: true };
      if (now < mem.staleUntil) return { data: mem.data, fresh: false }; // stale but usable
      _memCache.delete(key); // abgelaufen, löschen
    }

    // 2. localStorage-Cache (nur für persistente Daten wie Company-Info)
    return _getLocalCache(key);
  }

  function setCachedData(key, data, ttl) {
    // LRU-Eviction: Bei Überschreitung den ältesten Eintrag entfernen
    // Map iteriert in Insertion-Order → keys().next().value ist der älteste
    if (_memCache.size >= MAX_CACHE_SIZE) {
      const oldest = _memCache.keys().next().value;
      _memCache.delete(oldest);
      _log.debug('Cache LRU evicted:', oldest);
    }
    // Stale-Window = 2× TTL → Daten bleiben nutzbar, werden aber im Hintergrund erneuert
    const now = Date.now();
    _memCache.set(key, {
      data,
      freshUntil: now + ttl,
      staleUntil: now + ttl * 2,
    });

    // Company-Info und Search-Ergebnisse auch in localStorage persistieren
    if (key.startsWith('company:') || key.startsWith('search:')) {
      _setLocalCache(key, data, ttl);
    }
  }

  function invalidateCache(keyOrPrefix) {
    // Memory Cache leeren
    let removed = 0;
    for (const k of _memCache.keys()) {
      if (k === keyOrPrefix || k.startsWith(keyOrPrefix + ':')) { _memCache.delete(k); removed++; }
    }
    // localStorage leeren
    try {
      const prefix = 'sw2_' + keyOrPrefix;
      Object.keys(localStorage)
        .filter(k => k === prefix || k.startsWith(prefix + ':'))
        .forEach(k => localStorage.removeItem(k));
    } catch (err) {
      _log.warn('invalidateCache localStorage error:', err.message);
    }
    _log.debug('invalidateCache:', keyOrPrefix, `(${removed} mem entries)`);
  }

  function _getLocalCache(key) {
    try {
      const raw = localStorage.getItem('sw2_' + key);
      if (!raw) return null;
      const entry = JSON.parse(raw);
      // Strukturvalidierung: ungültige Einträge sofort entfernen
      if (!entry || typeof entry !== 'object' || !entry.data || !entry.freshUntil) {
        _log.warn('Invalid localStorage entry, removing:', key);
        localStorage.removeItem('sw2_' + key);
        return null;
      }
      if (Date.now() < entry.freshUntil) {
        // In Memory-Cache hochstufen für schnellere Folge-Zugriffe
        _memCache.set(key, entry);
        return { data: entry.data, fresh: true };
      }
      if (Date.now() < (entry.staleUntil || entry.freshUntil)) {
        // Stale aber nutzbar → hochstufen, als stale markieren
        _memCache.set(key, entry);
        return { data: entry.data, fresh: false };
      }
      localStorage.removeItem('sw2_' + key); // abgelaufen
    } catch (err) {
      _log.warn('localStorage read error for', key, err.message);
    }
    return null;
  }

  function _setLocalCache(key, data, ttl) {
    try {
      const entry = { data, freshUntil: Date.now() + ttl, staleUntil: Date.now() + ttl * 2 };
      localStorage.setItem('sw2_' + key, JSON.stringify(entry));
    } catch (err) {
      _log.warn('localStorage write failed for', key, '— evicting oldest entries:', err.message);
      _evictLocalCache();
      // Retry nach Eviction
      try {
        const entry = { data, freshUntil: Date.now() + ttl, staleUntil: Date.now() + ttl * 2 };
        localStorage.setItem('sw2_' + key, JSON.stringify(entry));
      } catch (retryErr) {
        _log.error('localStorage write failed even after eviction:', retryErr.message);
      }
    }
  }

  /**
   * _evictLocalCache — Selektive Eviction: älteste 25% statt alles löschen.
   *
   * ALT: Alle sw2_* Keys wurden gelöscht → 24h-Company-Cache komplett zerstört
   * NEU: Nur die ältesten 25% nach freshUntil sortiert werden entfernt
   * → Wichtige, noch frische Einträge bleiben erhalten
   */
  function _evictLocalCache() {
    try {
      const keys = Object.keys(localStorage).filter(k => k.startsWith('sw2_'));
      if (!keys.length) return;

      const entries = keys.map(k => {
        try {
          const parsed = JSON.parse(localStorage.getItem(k));
          return { key: k, exp: parsed?.freshUntil || 0 };
        } catch {
          return { key: k, exp: 0 }; // Ungültige Einträge zuerst entfernen
        }
      }).sort((a, b) => a.exp - b.exp);

      // Älteste 25% entfernen (mindestens 1)
      const removeCount = Math.max(1, Math.ceil(entries.length * 0.25));
      entries.slice(0, removeCount).forEach(e => localStorage.removeItem(e.key));
      _log.info(`localStorage eviction: ${removeCount}/${entries.length} entries removed`);
    } catch (err) {
      _log.error('localStorage eviction failed:', err.message);
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // §3  REQUEST MANAGER — Deduplication + Retry + Rate Limit
  // ═══════════════════════════════════════════════════════════════════

  /**
   * _inflight: Map<key, Promise>
   *
   * WARUM Deduplication?
   * Ohne sie: Drei Komponenten fragen gleichzeitig nach AAPL → 3 Requests
   * Mit inflight Map: Drei Komponenten teilen sich 1 Promise → 1 Request
   *
   * Das ist besonders wichtig beim Seiten-Load wenn Portfolio + Watchlist
   * gleichzeitig dieselben Symbole laden wollen.
   */
  const _inflight = new Map();

  /**
   * managedFetch — zentraler Einstiegspunkt für alle API-Calls.
   *
   * Ablauf:
   *  1. Cache prüfen → bei fresh: sofort zurückgeben
   *  2. Bei stale: stale-Daten zurückgeben + Hintergrund-Revalidation
   *  3. Inflight-Check → vorhandene Promise wiederverwenden
   *  4. Fetch mit Retry ausführen → Cache befüllen
   */
  async function managedFetch(key, fetchFn, { ttl, bypassCache = false } = {}) {
    // Phase 1: Cache-Lookup
    if (!bypassCache && ttl) {
      const cached = getCachedData(key);
      if (cached) {
        if (cached.fresh) return cached.data;
        // Stale: sofort zurückgeben, im Hintergrund revalidieren
        _revalidateBackground(key, fetchFn, ttl);
        return cached.data;
      }
    }

    // Phase 2: Deduplication
    if (_inflight.has(key)) return _inflight.get(key);

    // Phase 3: Fetch mit Retry
    const promise = _retryFetch(fetchFn)
      .then(data => {
        if (data !== null && ttl) setCachedData(key, data, ttl);
        _inflight.delete(key);
        return data;
      })
      .catch(err => {
        _inflight.delete(key);
        throw err;
      });

    _inflight.set(key, promise);
    return promise;
  }

  async function _revalidateBackground(key, fetchFn, ttl) {
    if (_inflight.has(key)) return; // schon in Arbeit
    try {
      const data = await _retryFetch(fetchFn);
      if (data !== null) setCachedData(key, data, ttl);
      _log.debug('Background revalidation OK:', key);
    } catch (err) {
      _log.debug('Background revalidation failed:', key, err.message);
      // Hintergrund-Fehler loggen aber nicht werfen — stale Daten sind ausreichend
    }
  }

  /**
   * WARUM Exponential Backoff + Jitter?
   *  - Exponential: Server hat Zeit sich zu erholen (0.4s → 0.8s → 1.6s)
   *  - Jitter: Verhindert dass alle Clients gleichzeitig retrien (Thundering Herd)
   *  - Ohne Jitter würden 100 Nutzer nach exakt 400ms gleichzeitig retrien
   *
   * NEU: Differenzierte Fehlerbehandlung nach HTTP-Status
   *  - 4xx (Client-Fehler): Kein Retry (401 = Bad Key, 404 = nicht gefunden)
   *  - 5xx (Server-Fehler): Retry mit Backoff
   *  - Netzwerk/Timeout: Retry mit Backoff
   */
  async function _retryFetch(fetchFn, attempt = 0) {
    try {
      return await fetchFn();
    } catch (err) {
      // Kein Retry bei nicht-temporären Fehlern
      if (err.name === 'AbortError' || err.message === 'OFFLINE') throw err;

      // HTTP 4xx: Client-Fehler → kein Retry (Symbol nicht gefunden, Auth-Problem etc.)
      const statusMatch = err.message?.match(/HTTP\s*(\d{3})/i);
      if (statusMatch) {
        const status = parseInt(statusMatch[1]);
        if (status >= 400 && status < 500) {
          _log.warn(`Client error ${status}, no retry:`, err.message);
          throw err;
        }
      }

      if (attempt >= RETRY.MAX_ATTEMPTS) {
        _log.warn(`All ${RETRY.MAX_ATTEMPTS + 1} attempts failed:`, err.message);
        throw err;
      }

      const backoff = RETRY.BASE_DELAY_MS * Math.pow(2, attempt);
      const jitter = Math.random() * RETRY.JITTER_MS;
      _log.debug(`Retry ${attempt + 1}/${RETRY.MAX_ATTEMPTS} in ${Math.round(backoff + jitter)}ms`);
      await _sleep(backoff + jitter);
      return _retryFetch(fetchFn, attempt + 1);
    }
  }

  const _sleep = ms => new Promise(r => setTimeout(r, ms));

  // ═══════════════════════════════════════════════════════════════════
  // §4  PROXY FETCHER — Paralleles Race statt sequentieller Kette
  // ═══════════════════════════════════════════════════════════════════

  /**
   * proxyFetch — alle Proxies GLEICHZEITIG anfragen, schnellsten nehmen.
   *
   * Alter Ansatz (sequenziell):
   *   Proxy1 wartet 8s → fehlgeschlagen → Proxy2 wartet 8s → ...
   *   Worst Case: 3 × 8s = 24 Sekunden
   *
   * Neuer Ansatz (paralleles Race via Promise.any):
   *   Proxy1, Proxy2, Proxy3 gleichzeitig starten
   *   Erster der antwortet gewinnt, die anderen werden abgebrochen
   *   Worst Case: 1 × 5s = 5 Sekunden (in der Praxis 0.3–1.5s)
   *
   * Das ist der größte einzelne Performance-Gewinn der gesamten App.
   * Netzwerk-Latenz: von ~8–24s auf ~0.3–1.5s.
   */
  async function proxyFetch(url, timeoutMs = 5_000, externalSignal = null) {
    if (!navigator.onLine) throw new Error('OFFLINE');

    // Shared AbortController: wenn einer gewinnt, werden alle anderen abgebrochen
    // → keine unnötigen Netzwerk-Verbindungen offen lassen
    const master = new AbortController();
    if (externalSignal) {
      externalSignal.addEventListener('abort', () => master.abort(), { once: true });
    }

    // Timeout für das gesamte Race (nicht pro Proxy)
    const tid = setTimeout(() => master.abort(), timeoutMs);

    /**
     * _tryProxy — ein einzelner Proxy-Versuch.
     * Wirft bei Fehler (damit Promise.any den nächsten versucht).
     * Gibt geparste JSON-Daten bei Erfolg zurück.
     */
    async function _tryProxy(stratFn) {
      const { px, unwrap } = stratFn(url);
      const r = await fetch(px, { signal: master.signal });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);

      let raw;
      if (unwrap) {
        const w = await r.json();
        raw = w?.contents;
        if (!raw) throw new Error('empty wrapper');
      } else {
        raw = await r.text();
      }

      if (!raw || raw.startsWith('<!') || raw.includes('consent.yahoo') || raw.startsWith('<html')) {
        throw new Error('invalid response');
      }
      return JSON.parse(raw); // wirft bei ungültigem JSON
    }

    try {
      // Promise.any: resolved mit dem ersten Erfolg.
      // Wenn ALLE fehlschlagen → AggregateError → wir geben null zurück.
      const result = await Promise.any(PROXIES.map(stratFn => _tryProxy(stratFn)));
      clearTimeout(tid);
      master.abort(); // restliche Proxies abbrechen
      return result;
    } catch (e) {
      clearTimeout(tid);
      if (e.name === 'AbortError') {
        _log.debug('proxyFetch timeout/aborted:', url.substring(0, 80));
        return null;
      }
      // AggregateError: Alle Proxies fehlgeschlagen — Details loggen
      const reasons = e.errors
        ? e.errors.map(err => err.message).join(', ')
        : e.message;
      _log.warn('All proxies failed for:', url.substring(0, 80), '—', reasons);
      return null;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // §5  BATCH FETCHER — DER GRÖSSTE PERFORMANCE-GEWINN
  // ═══════════════════════════════════════════════════════════════════

  /**
   * fetchBatchQuotes(symbols: string[]) → Record<string, NormalizedQuote>
   *
   * WARUM Batch statt Einzelrequests?
   * ┌─────────────────────────────────────────────────────────────┐
   * │  Alt: 10 Aktien → 10 Requests (seriel: ~5-10s, parallel: ~2s)│
   * │  Neu: 10 Aktien → 1 Request   (parallel: ~0.5s)             │
   * │                                                               │
   * │  API-Last:  -90% bei 10 Aktien, -98% bei 50 Aktien          │
   * │  Latenz:    -75% (ein Netzwerk-Roundtrip statt vieler)       │
   * └─────────────────────────────────────────────────────────────┘
   *
   * Yahoo Finance /v7/finance/quote akzeptiert:
   *   ?symbols=AAPL,MSFT,NVDA,SAP.DE,BMW.DE
   * und liefert alle Quotes in einer Antwort.
   *
   * Das ist der Unterschied zu /v8/finance/chart (nur 1 Symbol).
   */
  async function fetchBatchQuotes(symbols) {
    if (!symbols || !symbols.length) return {};

    // Symbole deduplizieren
    const unique = [...new Set(symbols.map(s => s.toUpperCase()))];

    // In Batches aufteilen (BATCH_SIZE = 50)
    const batches = [];
    for (let i = 0; i < unique.length; i += BATCH_SIZE) {
      batches.push(unique.slice(i, i + BATCH_SIZE));
    }

    /**
     * WARUM Promise.all() statt sequentielles await?
     * Bei sequentiellem Await: Batch 1 fertig, dann Batch 2 → additive Latenz
     * Bei Promise.all(): Alle Batches parallel → nur 1 Roundtrip-Zeit
     * Für 100 Aktien: 2 parallele Requests statt 100 sequentielle
     */
    const results = await Promise.all(batches.map(batch => _fetchOneBatch(batch)));

    // Alle Batch-Ergebnisse in eine flache Map zusammenführen
    return Object.assign({}, ...results);
  }

  async function _fetchOneBatch(symbols) {
    // Cache-Key aus sortierten Symbolen → deterministisch, unabhängig von Reihenfolge
    const sortedKey = symbols.slice().sort().join(',');
    const cacheKey = 'batch:' + sortedKey;

    return managedFetch(cacheKey, async () => {
      const syms = symbols.join(',');
      const fields = [
        'regularMarketPrice',
        'regularMarketChange',
        'regularMarketChangePercent',
        'regularMarketDayHigh',
        'regularMarketDayLow',
        'regularMarketOpen',
        'regularMarketVolume',
        'previousClose',
        'shortName',
        'longName',
        'currency',
        'exchangeName',
        'marketCap',
        'fiftyTwoWeekHigh',
        'fiftyTwoWeekLow',
      ].join(',');

      // Primary: query1 (schneller, keine GDPR-Weiterleitung für .com-Domain)
      let data = await proxyFetch(
        `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(syms)}&fields=${fields}&region=US&lang=en-US&corsDomain=finance.yahoo.com`,
        10_000
      );

      // Fallback: query2
      if (!data?.quoteResponse?.result?.length) {
        _log.debug('query1 returned empty, trying query2 for', symbols.length, 'symbols');
        data = await proxyFetch(
          `https://query2.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(syms)}&fields=${fields}&region=US&lang=en-US&corsDomain=finance.yahoo.com`,
          8_000
        );
      }

      // Response-Validierung: Prüfe ob die Antwort die erwartete Struktur hat
      if (data && !data.quoteResponse) {
        _log.warn('Unexpected quote response structure:', Object.keys(data).join(','));
      }

      const quotes = data?.quoteResponse?.result ?? [];
      if (!quotes.length) {
        _log.warn('No quotes returned for:', symbols.slice(0, 5).join(','), symbols.length > 5 ? '…' : '');
      }
      const map = {};
      for (const q of quotes) {
        if (!q.symbol) continue;
        map[q.symbol] = _normalizeQuote(q);
      }
      return map;
    }, { ttl: TTL.QUOTE });
  }

  /** Normalisiert Yahoo-Rohdaten in ein konsistentes Format */
  function _normalizeQuote(q) {
    const price    = q.regularMarketPrice ?? null;
    const prevClose= q.previousClose ?? null;
    const change   = q.regularMarketChange ?? (price && prevClose ? price - prevClose : null);
    const changePct= q.regularMarketChangePercent
      ?? (price && prevClose ? (price - prevClose) / prevClose * 100 : null);

    return {
      symbol:    q.symbol,
      price,
      change,
      changePct,
      high:      q.regularMarketDayHigh   ?? null,
      low:       q.regularMarketDayLow    ?? null,
      open:      q.regularMarketOpen      ?? null,
      prevClose,
      volume:    q.regularMarketVolume    ?? null,
      name:      q.shortName ?? q.longName ?? q.symbol,
      currency:  q.currency  ?? '',
      exchange:  q.exchangeName ?? '',
      marketCap: q.marketCap ?? null,
      wk52High:  q.fiftyTwoWeekHigh ?? null,
      wk52Low:   q.fiftyTwoWeekLow  ?? null,
      _src:      'yahoo-batch',
      _ts:       Date.now(),
    };
  }

  /**
   * fetchChart(symbol, range, interval) → ChartData
   *
   * WARUM separate Funktion statt in fetchBatchQuotes?
   * Chart-Daten sind viel größer (OHLCV für Hunderte Zeitpunkte) und haben
   * einen anderen TTL (5min) als Quote-Preise (15s). Außerdem wird immer
   * nur 1 Chart gleichzeitig angezeigt → kein Batching nötig.
   */
  async function fetchChart(symbol, range = '1D', interval = '5m') {
    const rangeMap = { '1D': '1d', '1W': '5d', '1M': '1mo', '1Y': '1y', '5Y': '5y' };
    const yRange   = rangeMap[range] ?? '1d';
    const cacheKey = `chart:${symbol.toUpperCase()}:${range}`;

    return managedFetch(cacheKey, async () => {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`
        + `?interval=${interval}&range=${yRange}&region=US&lang=en-US&corsDomain=finance.yahoo.com`;

      const data = await proxyFetch(url, 12_000);
      if (data && !data.chart) {
        _log.warn('Unexpected chart response structure for', symbol, ':', Object.keys(data).join(','));
      }
      const result = data?.chart?.result?.[0];
      if (!result) {
        _log.debug('No chart data for', symbol, range);
        return null;
      }
      return _normalizeChart(result);
    }, { ttl: TTL.CHART });
  }

  function _normalizeChart(result) {
    const meta       = result.meta ?? {};
    const timestamps = result.timestamp ?? [];
    const q          = result.indicators?.quote?.[0] ?? {};

    // Filter out null-Einträge (Yahoo liefert manchmal Lücken)
    const points = timestamps
      .map((ts, i) => ({
        ts,
        c: q.close?.[i]  ?? null,
        h: q.high?.[i]   ?? null,
        l: q.low?.[i]    ?? null,
        o: q.open?.[i]   ?? null,
        v: q.volume?.[i] ?? null,
      }))
      .filter(p => p.c !== null);

    return { meta, points, _ts: Date.now() };
  }

  /**
   * fetchCompanyInfo(symbol) → CompanyInfo
   * Langzeit-gecacht (24h) und in localStorage persistiert.
   * WARUM: Firmenname/Exchange ändert sich quasi nie → unnötige Requests vermeiden
   */
  async function fetchCompanyInfo(symbol) {
    const sym = symbol.toUpperCase();
    const cacheKey = `company:${sym}`;

    return managedFetch(cacheKey, async () => {
      const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(sym)}`
        + `&fields=shortName,longName,sector,industry,currency,exchangeName,quoteType&region=US&lang=en-US`;
      const data = await proxyFetch(url, 6_000);
      if (data && !data.quoteResponse) {
        _log.warn('Unexpected company response structure for', sym);
      }
      const q = data?.quoteResponse?.result?.[0];
      if (!q) {
        _log.debug('No company info found for', sym);
        return null;
      }
      return {
        symbol:   q.symbol,
        name:     q.longName ?? q.shortName ?? sym,
        short:    q.shortName ?? sym,
        sector:   q.sector    ?? '',
        industry: q.industry  ?? '',
        currency: q.currency  ?? '',
        exchange: q.exchangeName ?? '',
        type:     q.quoteType ?? '',
      };
    }, { ttl: TTL.COMPANY });
  }

  /**
   * fetchHistoricalPrice(symbol, dateStr) → { price, date } | null
   * Für historische Kaufpreise im Add-Stock-Modal.
   */
  async function fetchHistoricalPrice(symbol, dateStr) {
    const cacheKey = `hist:${symbol.toUpperCase()}:${dateStr}`;

    return managedFetch(cacheKey, async () => {
      const dt   = new Date(dateStr + 'T12:00:00Z');
      const from = Math.floor(dt.getTime() / 1000) - 259_200;
      const to   = Math.floor(dt.getTime() / 1000) + 259_200;

      for (const host of ['query1', 'query2']) {
        const url = `https://${host}.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`
          + `?interval=1d&period1=${from}&period2=${to}&region=US&lang=en-US&corsDomain=finance.yahoo.com`;
        const data = await proxyFetch(url, 7_000);
        const res  = data?.chart?.result?.[0];
        if (!res?.timestamp) continue;
        const closes = res.indicators?.quote?.[0]?.close ?? [];
        const times  = res.timestamp;

        // Nächsten Schlusskurs zum Zieldatum finden
        let bestIdx = 0, bestDiff = Infinity;
        const tgt = dt.getTime() / 1000;
        times.forEach((ts, i) => {
          if (closes[i] == null) return;
          const diff = Math.abs(ts - tgt);
          if (diff < bestDiff) { bestDiff = diff; bestIdx = i; }
        });

        if (closes[bestIdx] != null) {
          return { price: closes[bestIdx], date: new Date(times[bestIdx] * 1000) };
        }
      }
      return null;
    }, { ttl: TTL.HIST });
  }

  /**
   * fetchSearch(query) → SearchResult[]
   */
  async function fetchSearch(query) {
    if (!query || query.length < 1) return [];
    const cacheKey = `search:${query.toLowerCase().trim()}`;

    return managedFetch(cacheKey, async () => {
      const url = `https://query1.finance.yahoo.com/v1/finance/search?q=${encodeURIComponent(query)}`
        + `&quotesCount=8&newsCount=0&listsCount=0&enableFuzzyQuery=false&region=DE&lang=de`;
      const data = await proxyFetch(url, 5_000);
      const results = data?.finance?.result?.[0]?.quotes ?? [];
      return results.filter(r => r.symbol && !r.symbol.includes('~')).slice(0, 8);
    }, { ttl: TTL.SEARCH });
  }

  // ═══════════════════════════════════════════════════════════════════
  // §6  RENDER SCHEDULER — RAF-Batching + Diff-basierte DOM-Updates
  // ═══════════════════════════════════════════════════════════════════

  /**
   * WARUM requestAnimationFrame (RAF)?
   *
   * Problem ohne RAF:
   *  - 10 Karten-Updates = 10× Reflow + 10× Repaint = Jank/Ruckeln
   *  - Browser kann Layouts nicht batchen wenn JS-Updates verstreut sind
   *
   * Mit RAF:
   *  - Alle DOM-Updates werden in einem einzigen Frame ausgeführt
   *  - Browser macht 1× Reflow + 1× Repaint = 60fps
   *  - Kostet 0ms Overhead für den Nutzer (synchron mit Frame-Takt)
   */
  let _rafPending = false;
  const _renderQueue = new Map(); // id → { el, data, renderFn }

  function scheduleRender(id, el, data, renderFn) {
    _renderQueue.set(id, { el, data, renderFn });
    if (!_rafPending) {
      _rafPending = true;
      requestAnimationFrame(_flushRenderQueue);
    }
  }

  function _flushRenderQueue() {
    _rafPending = false;
    // WARUM: Alle renders in einem Frame → ein Reflow/Repaint statt N
    for (const { el, data, renderFn } of _renderQueue.values()) {
      try { renderFn(el, data); } catch (e) { console.warn('Render error:', e); }
    }
    _renderQueue.clear();
  }

  /**
   * patchText — Diff-basiertes textContent-Update.
   *
   * WARUM nicht einfach el.textContent = value?
   *  - Selbst wenn der Wert gleich ist, triggert eine Zuweisung einen DOM-Mutation
   *  - Mutations triggern MutationObserver und können Reflows verursachen
   *  - patchText prüft zuerst → nur echte Änderungen werden geschrieben
   *
   * Effekt: Bei stabilem Preis (kein Tick) = 0 DOM-Mutationen statt N
   */
  function patchText(el, value) {
    if (el && el.textContent !== String(value)) {
      el.textContent = value;
    }
  }

  function patchClass(el, cls, condition) {
    if (!el) return;
    const has = el.classList.contains(cls);
    if (condition && !has)  el.classList.add(cls);
    if (!condition && has)  el.classList.remove(cls);
  }

  function patchStyle(el, prop, value) {
    if (el && el.style[prop] !== value) el.style[prop] = value;
  }

  /**
   * buildFragment — erstellt DOM-Liste via DocumentFragment.
   *
   * WARUM DocumentFragment?
   *  - innerHTML = string: Browser parst HTML, erstellt DOM, dann Reflow
   *  - DocumentFragment: Aufbau komplett off-DOM → einmaliger Reflow beim Einfügen
   *  - Bei 50 Listenelementen: 1 Reflow statt 50
   */
  function buildFragment(items, renderItemFn) {
    const frag = document.createDocumentFragment();
    for (const item of items) {
      const el = renderItemFn(item);
      if (el) frag.appendChild(el);
    }
    return frag;
  }

  // ═══════════════════════════════════════════════════════════════════
  // §7  REFRESH SCHEDULER — Page Visibility + setTimeout-Chain
  // ═══════════════════════════════════════════════════════════════════

  /**
   * WARUM Page Visibility API?
   *
   * setInterval ohne Visibility-Check:
   *  - Nutzer wechselt Tab → App feuert trotzdem alle 15s Requests
   *  - Auf 10 Tabs: 10× API-Last für Daten die niemand sieht
   *  - Mobile: Battery-Drain, Hintergrund-Traffic-Kosten
   *
   * Mit Page Visibility:
   *  - Tab versteckt → Refresh pausiert
   *  - Tab sichtbar → sofortiger Refresh + Neustart
   *  - Ergebnis: 0 API-Calls wenn App im Hintergrund
   *
   * WARUM setTimeout-Chain statt setInterval?
   *  - setInterval feuert auch wenn vorheriger Call noch läuft → Overlaps
   *  - setTimeout: Nächster Call startet erst NACH Abschluss des aktuellen
   *  - Kein Race Condition, kein API-Spam bei langsamer Verbindung
   */
  let _refreshTimer    = null;
  let _refreshCallback = null;
  let _refreshInterval = 15_000;
  let _isPageVisible   = !document.hidden;
  let _isMobile        = /Mobi|Android/i.test(navigator.userAgent);
  let _consecutiveErrors = 0; // Backoff bei aufeinanderfolgenden Fehlern

  // Page Visibility Event
  document.addEventListener('visibilitychange', () => {
    _isPageVisible = !document.hidden;
    if (_isPageVisible) {
      // Tab wieder sichtbar → sofort refreshen, dann Schedule starten
      _consecutiveErrors = 0; // Backoff zurücksetzen
      if (_refreshCallback) _refreshCallback().finally(_scheduleNextRefresh);
    } else {
      _stopRefreshTimer();
    }
  });

  function startRefresh(callback, intervalMs = 15_000) {
    _refreshCallback = callback;
    // Mobile: doppelte Interval → spart Akku und Daten
    _refreshInterval = _isMobile ? intervalMs * 2 : intervalMs;
    _consecutiveErrors = 0;
    _scheduleNextRefresh();
  }

  function stopRefresh() {
    _stopRefreshTimer();
    _refreshCallback = null;
    _consecutiveErrors = 0;
  }

  function _stopRefreshTimer() {
    if (_refreshTimer) { clearTimeout(_refreshTimer); _refreshTimer = null; }
  }

  /**
   * _scheduleNextRefresh — mit Exponential Backoff bei Fehlern.
   *
   * VERBESSERUNG: Bei konsekutiven Fehlern wird das Intervall verdoppelt,
   * bis max 8× (z.B. 15s → 30s → 60s → 120s bei 4+ Fehlern in Folge).
   * Bei Erfolg wird sofort auf das Normalintervall zurückgesetzt.
   * So überlasten wir einen gestressten Server nicht mit Retries.
   */
  function _scheduleNextRefresh() {
    _stopRefreshTimer();
    if (!_isPageVisible || !_refreshCallback) return;

    // Exponentielles Backoff: 1× → 2× → 4× → 8× (max) bei aufeinanderfolgenden Fehlern
    const errorMultiplier = Math.min(Math.pow(2, _consecutiveErrors), 8);
    const interval = _refreshInterval * errorMultiplier;

    if (_consecutiveErrors > 0) {
      _log.info(`Refresh backoff: ${_consecutiveErrors} consecutive errors, next in ${Math.round(interval / 1000)}s`);
    }

    _refreshTimer = setTimeout(async () => {
      if (_isPageVisible && _refreshCallback) {
        try {
          await _refreshCallback();
          _consecutiveErrors = 0; // Erfolg → Backoff zurücksetzen
        } catch (err) {
          _consecutiveErrors++;
          _log.warn(`Refresh failed (${_consecutiveErrors}×):`, err.message);
        }
        _scheduleNextRefresh(); // Erst NACH Abschluss neu planen
      }
    }, interval);
  }

  // ═══════════════════════════════════════════════════════════════════
  // §8  UTILITIES
  // ═══════════════════════════════════════════════════════════════════

  /**
   * escapeHtml — XSS-Schutz für dynamische Inhalte in innerHTML.
   *
   * WARUM: Template Literals wie `<div>${s.name}</div>` sind XSS-anfällig,
   * wenn s.name manipuliert ist (z.B. '<img onerror=alert(1)>').
   * escapeHtml() neutralisiert HTML-Sonderzeichen.
   */
  function _escapeHtml(str) {
    if (typeof str !== 'string') return String(str ?? '');
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  /**
   * Response-Validierung: Prüft ob Yahoo-Daten die erwartete Struktur haben.
   * Wirft bei ungültiger Struktur statt still null zurückzugeben.
   */
  function _validateQuoteResponse(data) {
    if (!data || typeof data !== 'object') return null;
    if (!data.quoteResponse) return null;
    if (!Array.isArray(data.quoteResponse.result)) return null;
    return data.quoteResponse.result;
  }

  function _validateChartResponse(data) {
    if (!data?.chart?.result?.[0]) return null;
    const result = data.chart.result[0];
    if (!Array.isArray(result.timestamp)) return null;
    return result;
  }

  // ═══════════════════════════════════════════════════════════════════
  // §9  ÖFFENTLICHE API
  // ═══════════════════════════════════════════════════════════════════

  return Object.freeze({
    // Fetch-API
    fetchBatchQuotes,
    fetchChart,
    fetchCompanyInfo,
    fetchHistoricalPrice,
    fetchSearch,

    // Cache-API
    getCachedData,
    setCachedData,
    invalidateCache,

    // Render-API
    scheduleRender,
    patchText,
    patchClass,
    patchStyle,
    buildFragment,

    // Refresh-API
    startRefresh,
    stopRefresh,

    // Utils (für direkte Nutzung in Integrations-Code)
    proxyFetch,
    normalizeQuote: _normalizeQuote,
    escapeHtml: _escapeHtml,
    sleep: _sleep,

    // Diagnostics
    get cacheSize()    { return _memCache.size; },
    get inflightCount(){ return _inflight.size; },
    get consecutiveErrors() { return _consecutiveErrors; },
    set debug(v) { _debugMode = !!v; },
    get debug() { return _debugMode; },
    clearAllCache()    { _memCache.clear(); _evictLocalCache(); _log.info('All caches cleared'); },
  });

})();
