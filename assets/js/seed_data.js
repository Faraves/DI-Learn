/**
 * DataDojo: Financial & ETL Optimizer
 * Deterministic Synthetic Banking Dataset Generator (seed_data.js)
 *
 * CONFIDENTIALITY NOTICE:
 * 100% synthetic schemas and camouflaged entities designed for algorithmic training.
 *
 * Schemas & Tables created in DuckDB-Wasm:
 * 1. tesoreria.tasas_cambio_oficial (LOCF weekend/holiday gaps + 0.00 rate traps)
 * 2. crm.historico_segmento_cliente (Mid-month segment change duplication traps)
 * 3. pasivos.catalogo_cuentas & pasivos.saldos_diarios_snapshot (Daily photos, sparse days, De Morgan traps)
 * 4. pasivos.depositos_vigencia_intervalo (Half-open intervals [fecha_apertura, fecha_vencimiento) with day-15 renewals & EOM maturities)
 */

(function (global) {
  'use strict';

  /**
   * Helper to format YYYY-MM-DD
   */
  function fmtDate(year, month, day) {
    const m = String(month).padStart(2, '0');
    const d = String(day).padStart(2, '0');
    return `${year}-${m}-${d}`;
  }

  /**
   * Day of week (0 = Sun, 6 = Sat) in UTC
   */
  function getDayOfWeek(dateStr) {
    const [y, m, d] = dateStr.split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d)).getUTCDay();
  }

  /**
   * 1. GENERATE tesoreria.tasas_cambio_oficial
   * Includes intentional gaps on weekends & holidays, and 0.00 corruption traps on specific days.
   */
  function generateTasasCambio() {
    const rows = [];
    const periods = [
      { year: 2023, month: 11, days: 30, baseEur: 1.08, baseCop: 0.00025 },
      { year: 2023, month: 12, days: 31, baseEur: 1.09, baseCop: 0.00025 },
      { year: 2024, month: 11, days: 30, baseEur: 1.05, baseCop: 0.00023 },
      { year: 2024, month: 12, days: 31, baseEur: 1.06, baseCop: 0.00023 },
      { year: 2025, month: 11, days: 30, baseEur: 1.10, baseCop: 0.00024 },
      { year: 2025, month: 12, days: 31, baseEur: 1.12, baseCop: 0.00025 },
      { year: 2026, month: 8, days: 31, baseEur: 1.14, baseCop: 0.00025 },
      { year: 2026, month: 9, days: 30, baseEur: 1.15, baseCop: 0.00025 }
    ];

    const holidays = new Set([
      '2023-12-08', '2023-12-25',
      '2024-12-08', '2024-12-25',
      '2025-12-08', '2025-12-25',
      '2026-09-15'
    ]);

    const zeroRateTraps = new Set([
      '2025-12-10',
      '2026-09-10'
    ]);

    for (const p of periods) {
      for (let d = 1; d <= p.days; d++) {
        const dateStr = fmtDate(p.year, p.month, d);
        const dow = getDayOfWeek(dateStr);

        // Omit weekends and banking holidays to force LOCF (Last Observation Carried Forward)
        if (dow === 0 || dow === 6 || holidays.has(dateStr)) {
          continue;
        }

        if (zeroRateTraps.has(dateStr)) {
          // Trap: 0.00 rate on a weekday to force NULLIF(tasa, 0)
          rows.push({
            fecha_tasa: dateStr,
            tasa_eur_usd: 0.0,
            tasa_cop_usd: 0.0
          });
        } else {
          // Deterministic rate progression
          const eurRate = Number((p.baseEur + ((d % 5) * 0.005)).toFixed(6));
          const copRate = Number((p.baseCop).toFixed(6));
          rows.push({
            fecha_tasa: dateStr,
            tasa_eur_usd: eurRate,
            tasa_cop_usd: copRate
          });
        }
      }
    }

    // Override Dec 2025 and Sep 2026 valid weekday rates to clean constant reference numbers for transparent mental & audit math:
    // In Dec 2025: every valid weekday has tasa_eur_usd = 1.100000, tasa_cop_usd = 0.000250 (except 2025-12-10 which is 0.000000)
    // In Sep 2026: every valid weekday has tasa_eur_usd = 1.200000, tasa_cop_usd = 0.000250 (except 2026-09-10 which is 0.000000)
    for (const r of rows) {
      if (r.fecha_tasa.startsWith('2025-11') || r.fecha_tasa.startsWith('2025-12')) {
        if (!zeroRateTraps.has(r.fecha_tasa)) {
          r.tasa_eur_usd = 1.100000;
          r.tasa_cop_usd = 0.000250;
        }
      } else if (r.fecha_tasa.startsWith('2026-08') || r.fecha_tasa.startsWith('2026-09')) {
        if (!zeroRateTraps.has(r.fecha_tasa)) {
          r.tasa_eur_usd = 1.200000;
          r.tasa_cop_usd = 0.000250;
        }
      }
    }

    return rows;
  }

  /**
   * 2. GENERATE crm.historico_segmento_cliente
   * Includes mid-month segment changes for CLI-1001 and CLI-1002 to punish un-deduplicated JOINs.
   */
  function generateHistoricoSegmento() {
    return [
      // CLI-1001: Changed segment mid-month in Dec 2025 and Sep 2026!
      { id_cliente: 'CLI-1001', fecha_corte: '2023-12-01', segmento_comercial: 'BANCA_EMPRESAS', codigo_oficina: 101 },
      { id_cliente: 'CLI-1001', fecha_corte: '2025-12-03', segmento_comercial: 'BANCA_PYME', codigo_oficina: 101 },
      { id_cliente: 'CLI-1001', fecha_corte: '2025-12-18', segmento_comercial: 'BANCA_CORPORATIVA', codigo_oficina: 105 },
      { id_cliente: 'CLI-1001', fecha_corte: '2026-09-20', segmento_comercial: 'BANCA_CORPORATIVA', codigo_oficina: 105 },

      // CLI-1002: Changed segment twice in Dec 2025!
      { id_cliente: 'CLI-1002', fecha_corte: '2024-06-15', segmento_comercial: 'BANCA_PREFERENTE', codigo_oficina: 202 },
      { id_cliente: 'CLI-1002', fecha_corte: '2025-12-05', segmento_comercial: 'BANCA_PREFERENTE', codigo_oficina: 202 },
      { id_cliente: 'CLI-1002', fecha_corte: '2025-12-22', segmento_comercial: 'BANCA_PRIVADA', codigo_oficina: 210 },

      // CLI-1003: Single clean record
      { id_cliente: 'CLI-1003', fecha_corte: '2024-01-10', segmento_comercial: 'BANCA_PYME', codigo_oficina: 304 },

      // CLI-1004: Changed segment mid-month in Dec 2025
      { id_cliente: 'CLI-1004', fecha_corte: '2025-12-01', segmento_comercial: 'BANCA_PERSONAS', codigo_oficina: 401 },
      { id_cliente: 'CLI-1004', fecha_corte: '2025-12-19', segmento_comercial: 'BANCA_PREFERENTE', codigo_oficina: 401 },

      // CLI-1005: Institutional client
      { id_cliente: 'CLI-1005', fecha_corte: '2023-01-01', segmento_comercial: 'BANCA_INSTITUCIONAL', codigo_oficina: 500 },
      { id_cliente: 'CLI-1005', fecha_corte: '2025-12-12', segmento_comercial: 'BANCA_CORPORATIVA', codigo_oficina: 500 }
    ];
  }

  /**
   * 3. GENERATE pasivos.catalogo_cuentas
   */
  function generateCatalogoCuentas() {
    return [
      { id_cuenta: 'CTA-2001', id_cliente: 'CLI-1001', desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'USD', estado_cuenta: 'ACTIVA' },
      { id_cuenta: 'CTA-2002', id_cliente: 'CLI-1002', desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'USD', estado_cuenta: 'ACTIVA' },
      { id_cuenta: 'CTA-2003', id_cliente: 'CLI-1003', desc_producto: 'CUENTA DE AHORRO TRADICIONAL', moneda: 'USD', estado_cuenta: 'ACTIVA' },
      { id_cuenta: 'CTA-2004', id_cliente: 'CLI-1004', desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'EUR', estado_cuenta: 'ACTIVA' },
      { id_cuenta: 'CTA-2005', id_cliente: 'CLI-1005', desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'COP', estado_cuenta: 'ACTIVA' },
      { id_cuenta: 'CTA-9001', id_cliente: 'CLI-1001', desc_producto: 'CUENTA TRANSITORIA INTERNA', moneda: 'USD', estado_cuenta: 'INTERNA' },
      { id_cuenta: 'CTA-9002', id_cliente: 'CLI-1002', desc_producto: 'GARANTIA BLOQUEADA JUDICIAL', moneda: 'USD', estado_cuenta: 'BLOQUEADA' }
    ];
  }

  /**
   * 4. GENERATE pasivos.saldos_diarios_snapshot
   * Covers Dec 2023, Dec 2024, Nov 2025 (trap out-of-range), Dec 2025, and Sep 2026.
   */
  function generateSaldosDiariosSnapshot() {
    const rows = [];

    // Helper to push full month daily photos
    function addMonthPhotos(year, month, daysInMonth, accountsConfig) {
      for (const acc of accountsConfig) {
        const activeDays = acc.activeDays || Array.from({ length: daysInMonth }, (_, i) => i + 1);
        for (const d of activeDays) {
          const fecha = fmtDate(year, month, d);
          const monto = typeof acc.montoFn === 'function' ? acc.montoFn(d, daysInMonth) : acc.monto;
          rows.push({
            id_cuenta: acc.id_cuenta,
            id_cliente: acc.id_cliente,
            fecha_foto: fecha,
            desc_producto: acc.desc_producto,
            moneda: acc.moneda,
            monto_original: Number(monto.toFixed(2))
          });
        }
      }
    }

    // --- DEC 2023 (31 days) ---
    // CTA-2001: $10,000 daily (days 1..30), $13,100 on day 31 -> Sum = 300,000 + 13,100 = 313,100 -> Avg = 10,100.00, Closing = 13,100.00
    // CTA-2002: $5,000 daily all 31 days -> Sum = 155,000 -> Avg = 5,000.00, Closing = 5,000.00
    addMonthPhotos(2023, 12, 31, [
      {
        id_cuenta: 'CTA-2001', id_cliente: 'CLI-1001',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'USD',
        montoFn: (d) => (d === 31 ? 13100.00 : 10000.00)
      },
      {
        id_cuenta: 'CTA-2002', id_cliente: 'CLI-1002',
        desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'USD',
        monto: 5000.00
      }
    ]);

    // --- DEC 2024 (31 days) ---
    // CTA-2001: $12,000 daily (days 1..30), $15,100 on day 31 -> Sum = 375,100 -> Avg = 12,100.00, Closing = 15,100.00
    // CTA-2002: $6,000 daily all 31 days -> Sum = 186,000 -> Avg = 6,000.00, Closing = 6,000.00
    addMonthPhotos(2024, 12, 31, [
      {
        id_cuenta: 'CTA-2001', id_cliente: 'CLI-1001',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'USD',
        montoFn: (d) => (d === 31 ? 15100.00 : 12000.00)
      },
      {
        id_cuenta: 'CTA-2002', id_cliente: 'CLI-1002',
        desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'USD',
        monto: 6000.00
      }
    ]);

    // --- NOV 2025 (Off-period rows to test WHERE date range filtering) ---
    addMonthPhotos(2025, 11, 30, [
      {
        id_cuenta: 'CTA-2001', id_cliente: 'CLI-1001',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'USD',
        activeDays: [28, 29, 30],
        monto: 99999.00
      }
    ]);

    // --- DEC 2025 (31 days) - Core Period for Mode 1 & Mode 2 ---
    // CTA-2001 (CLI-1001, USD): $20,000 on days 1..30, $23,100 on day 31 -> Sum = 623,100 -> Avg = 20,100.00, Closing = 23,100.00
    // CTA-2002 (CLI-1002, USD): $10,000 on all 31 days -> Sum = 310,000 -> Avg = 10,000.00, Closing = 10,000.00
    // CTA-2003 (CLI-1003, USD): SPARSE ACCOUNT TRAP! Active only on days 22..31 (10 days) at $3,100/day -> Sum = 31,000 -> True Calendar Avg (31 days) = 1,000.00 (Naive AVG() gives 3,100.00!), Closing = 3,100.00
    // CTA-2004 (CLI-1004, EUR): 10,000 EUR on all 31 days -> Sum = 310,000 EUR -> Avg = 10,000 EUR (11,000 USD at LOCF 1.10), Closing = 10,000 EUR (11,000 USD)
    // CTA-2005 (CLI-1005, COP): 40,000,000 COP on all 31 days -> At LOCF 0.00025 = 10,000 USD/day
    // CTA-9001 & CTA-9002 (Excluded internal/blocked products): $50,000/day each (must be filtered out with NOT ILIKE AND NOT ILIKE)
    addMonthPhotos(2025, 12, 31, [
      {
        id_cuenta: 'CTA-2001', id_cliente: 'CLI-1001',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'USD',
        montoFn: (d) => (d === 31 ? 23100.00 : 20000.00)
      },
      {
        id_cuenta: 'CTA-2002', id_cliente: 'CLI-1002',
        desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'USD',
        monto: 10000.00
      },
      {
        id_cuenta: 'CTA-2003', id_cliente: 'CLI-1003',
        desc_producto: 'CUENTA DE AHORRO TRADICIONAL', moneda: 'USD',
        activeDays: [22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        monto: 3100.00
      },
      {
        id_cuenta: 'CTA-2004', id_cliente: 'CLI-1004',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'EUR',
        monto: 10000.00
      },
      {
        id_cuenta: 'CTA-2005', id_cliente: 'CLI-1005',
        desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'COP',
        monto: 40000000.00
      },
      {
        id_cuenta: 'CTA-9001', id_cliente: 'CLI-1001',
        desc_producto: 'CUENTA TRANSITORIA INTERNA', moneda: 'USD',
        monto: 50000.00
      },
      {
        id_cuenta: 'CTA-9002', id_cliente: 'CLI-1002',
        desc_producto: 'GARANTIA BLOQUEADA JUDICIAL', moneda: 'USD',
        monto: 50000.00
      }
    ]);

    // --- SEP 2026 (30 days) ---
    // CTA-2001 (USD): $25,000 on days 1..29, $28,000 on day 30 -> Sum = 753,000 -> Avg (div 30) = 25,100.00, Closing = 28,000.00
    // CTA-2002 (USD): $12,000 on all 30 days -> Sum = 360,000 -> Avg (div 30) = 12,000.00, Closing = 12,000.00
    // CTA-2004 (EUR): 15,000 EUR on all 30 days -> At LOCF 1.20 = 18,000 USD/day
    // CTA-2005 (COP): 60,000,000 COP on all 30 days -> At LOCF 0.00025 = 15,000 USD/day
    addMonthPhotos(2026, 9, 30, [
      {
        id_cuenta: 'CTA-2001', id_cliente: 'CLI-1001',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'USD',
        montoFn: (d) => (d === 30 ? 28000.00 : 25000.00)
      },
      {
        id_cuenta: 'CTA-2002', id_cliente: 'CLI-1002',
        desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'USD',
        monto: 12000.00
      },
      {
        id_cuenta: 'CTA-2004', id_cliente: 'CLI-1004',
        desc_producto: 'CUENTA CORRIENTE REMUNERADA', moneda: 'EUR',
        monto: 15000.00
      },
      {
        id_cuenta: 'CTA-2005', id_cliente: 'CLI-1005',
        desc_producto: 'CUENTA CORRIENTE NO REMUNERADA', moneda: 'COP',
        monto: 60000000.00
      }
    ]);

    return rows;
  }

  /**
   * 5. GENERATE pasivos.depositos_vigencia_intervalo
   * Half-open intervals [fecha_apertura, fecha_vencimiento)
   * Last real active day = fecha_vencimiento - 1 day.
   */
  function generateDepositosIntervalo() {
    return [
      // --- DEC 2023 Contracts ---
      // Active all 31 days of Dec 2023 -> Closing (2023-12-31) = 50,000, Avg = 50,000
      {
        id_operacion: 'DEP-301', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2023-11-15', fecha_vencimiento: '2024-01-15',
        monto_nominal: 50000.00
      },
      // Matures on 2023-12-31 -> Active 30 days (Dec 1..30), Closing on 2023-12-31 = 0.00! Avg = 31,000 * 30 / 31 = 30,000.00
      {
        id_operacion: 'DEP-302', id_cliente: 'CLI-1002',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2023-12-01', fecha_vencimiento: '2023-12-31',
        monto_nominal: 31000.00
      },

      // --- DEC 2024 Contracts ---
      // Same-day renewal on Dec 15, 2024:
      // DEP-401: [2024-12-01, 2024-12-15) -> 14 active days * 80,000
      // DEP-402: [2024-12-15, 2025-01-15) -> 17 active days * 80,000 (Active on 2024-12-31!)
      // Combined Dec 2024: Avg = 80,000.00, Closing = 80,000.00
      {
        id_operacion: 'DEP-401', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2024-12-01', fecha_vencimiento: '2024-12-15',
        monto_nominal: 80000.00
      },
      {
        id_operacion: 'DEP-402', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2024-12-15', fecha_vencimiento: '2025-01-15',
        monto_nominal: 80000.00
      },

      // --- DEC 2025 Contracts (Core Period) ---
      // Trap 1: Certificate maturing and renewing on the 15th of the month!
      // DEP-501: [2025-12-01, 2025-12-15) -> 14 days active (Dec 1..14), Closing on Dec 31 = 0.00
      // DEP-502: [2025-12-15, 2026-01-15) -> 17 days active (Dec 15..31), Closing on Dec 31 = 100,000.00
      {
        id_operacion: 'DEP-501', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2025-12-01', fecha_vencimiento: '2025-12-15',
        monto_nominal: 100000.00
      },
      {
        id_operacion: 'DEP-502', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2025-12-15', fecha_vencimiento: '2026-01-15',
        monto_nominal: 100000.00
      },
      // Trap 2: Certificate maturing ON the last day of the month (2025-12-31)
      // DEP-503: [2025-12-01, 2025-12-31) -> Active 30 days (Dec 1..30). Avg = 62,000 * 30 / 31 = 60,000.00. Closing on Dec 31 = 0.00!
      {
        id_operacion: 'DEP-503', id_cliente: 'CLI-1002',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2025-12-01', fecha_vencimiento: '2025-12-31',
        monto_nominal: 62000.00
      },
      // DEP-504: Structured Bond active across the entire month [2025-11-01, 2026-03-01) -> 31 days active, Avg = 40,000.00, Closing = 40,000.00
      {
        id_operacion: 'DEP-504', id_cliente: 'CLI-1003',
        desc_producto: 'BONO BANCARIO ESTRUCTURADO', moneda: 'USD',
        fecha_apertura: '2025-11-01', fecha_vencimiento: '2026-03-01',
        monto_nominal: 40000.00
      },

      // --- SEP 2026 Contracts (30-day month) ---
      // DEP-601: [2026-09-01, 2026-09-15) -> 14 days active (Sep 1..14) at $90,000
      // DEP-602: [2026-09-15, 2026-10-15) -> 16 days active (Sep 15..30) at $90,000 (Closing on Sep 30 = $90,000.00)
      // Combined Sep 2026 for CLI-1001: 30 days * 90,000 / 30 = 90,000.00 Avg, 90,000.00 Closing
      {
        id_operacion: 'DEP-601', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2026-09-01', fecha_vencimiento: '2026-09-15',
        monto_nominal: 90000.00
      },
      {
        id_operacion: 'DEP-602', id_cliente: 'CLI-1001',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2026-09-15', fecha_vencimiento: '2026-10-15',
        monto_nominal: 90000.00
      },
      // DEP-603: Matures on 2026-09-30 -> 29 active days (Sep 1..29) at $30,000 -> Avg = 30,000 * 29 / 30 = 29,000.00, Closing on Sep 30 = 0.00
      {
        id_operacion: 'DEP-603', id_cliente: 'CLI-1002',
        desc_producto: 'CERTIFICADO DEPOSITO A PLAZO', moneda: 'USD',
        fecha_apertura: '2026-09-01', fecha_vencimiento: '2026-09-30',
        monto_nominal: 30000.00
      }
    ];
  }

  /**
   * Build the complete in-memory dataset object and SQL initialization script
   */
  const DATASETS = {
    tasas_cambio_oficial: generateTasasCambio(),
    historico_segmento_cliente: generateHistoricoSegmento(),
    catalogo_cuentas: generateCatalogoCuentas(),
    saldos_diarios_snapshot: generateSaldosDiariosSnapshot(),
    depositos_vigencia_intervalo: generateDepositosIntervalo()
  };

  /**
   * Builds SQL DDL + Batch INSERT statements for DuckDB-Wasm initialization
   */
  function buildDuckDBInitSQL() {
    const sqlParts = [
      `CREATE SCHEMA IF NOT EXISTS tesoreria;`,
      `CREATE SCHEMA IF NOT EXISTS crm;`,
      `CREATE SCHEMA IF NOT EXISTS pasivos;`,

      `DROP TABLE IF EXISTS tesoreria.tasas_cambio_oficial;`,
      `CREATE TABLE tesoreria.tasas_cambio_oficial (
        fecha_tasa DATE,
        tasa_eur_usd DECIMAL(12,6),
        tasa_cop_usd DECIMAL(12,6)
      );`,

      `DROP TABLE IF EXISTS crm.historico_segmento_cliente;`,
      `CREATE TABLE crm.historico_segmento_cliente (
        id_cliente VARCHAR,
        fecha_corte DATE,
        segmento_comercial VARCHAR,
        codigo_oficina INTEGER
      );`,

      `DROP TABLE IF EXISTS pasivos.catalogo_cuentas;`,
      `CREATE TABLE pasivos.catalogo_cuentas (
        id_cuenta VARCHAR,
        id_cliente VARCHAR,
        desc_producto VARCHAR,
        moneda VARCHAR,
        estado_cuenta VARCHAR
      );`,

      `DROP TABLE IF EXISTS pasivos.saldos_diarios_snapshot;`,
      `CREATE TABLE pasivos.saldos_diarios_snapshot (
        id_cuenta VARCHAR,
        id_cliente VARCHAR,
        fecha_foto DATE,
        desc_producto VARCHAR,
        moneda VARCHAR,
        monto_original DECIMAL(18,2)
      );`,

      `DROP TABLE IF EXISTS pasivos.depositos_vigencia_intervalo;`,
      `CREATE TABLE pasivos.depositos_vigencia_intervalo (
        id_operacion VARCHAR,
        id_cliente VARCHAR,
        desc_producto VARCHAR,
        moneda VARCHAR,
        fecha_apertura DATE,
        fecha_vencimiento DATE,
        monto_nominal DECIMAL(18,2)
      );`
    ];

    // Insert tasas_cambio_oficial
    if (DATASETS.tasas_cambio_oficial.length > 0) {
      const vals = DATASETS.tasas_cambio_oficial
        .map(r => `('${r.fecha_tasa}', ${r.tasa_eur_usd.toFixed(6)}, ${r.tasa_cop_usd.toFixed(6)})`)
        .join(',\n');
      sqlParts.push(`INSERT INTO tesoreria.tasas_cambio_oficial VALUES\n${vals};`);
    }

    // Insert historico_segmento_cliente
    if (DATASETS.historico_segmento_cliente.length > 0) {
      const vals = DATASETS.historico_segmento_cliente
        .map(r => `('${r.id_cliente}', '${r.fecha_corte}', '${r.segmento_comercial}', ${r.codigo_oficina})`)
        .join(',\n');
      sqlParts.push(`INSERT INTO crm.historico_segmento_cliente VALUES\n${vals};`);
    }

    // Insert catalogo_cuentas
    if (DATASETS.catalogo_cuentas.length > 0) {
      const vals = DATASETS.catalogo_cuentas
        .map(r => `('${r.id_cuenta}', '${r.id_cliente}', '${r.desc_producto}', '${r.moneda}', '${r.estado_cuenta}')`)
        .join(',\n');
      sqlParts.push(`INSERT INTO pasivos.catalogo_cuentas VALUES\n${vals};`);
    }

    // Insert saldos_diarios_snapshot
    if (DATASETS.saldos_diarios_snapshot.length > 0) {
      const vals = DATASETS.saldos_diarios_snapshot
        .map(r => `('${r.id_cuenta}', '${r.id_cliente}', '${r.fecha_foto}', '${r.desc_producto}', '${r.moneda}', ${r.monto_original.toFixed(2)})`)
        .join(',\n');
      sqlParts.push(`INSERT INTO pasivos.saldos_diarios_snapshot VALUES\n${vals};`);
    }

    // Insert depositos_vigencia_intervalo
    if (DATASETS.depositos_vigencia_intervalo.length > 0) {
      const vals = DATASETS.depositos_vigencia_intervalo
        .map(r => `('${r.id_operacion}', '${r.id_cliente}', '${r.desc_producto}', '${r.moneda}', '${r.fecha_apertura}', '${r.fecha_vencimiento}', ${r.monto_nominal.toFixed(2)})`)
        .join(',\n');
      sqlParts.push(`INSERT INTO pasivos.depositos_vigencia_intervalo VALUES\n${vals};`);
    }

    return sqlParts.join('\n\n');
  }

  /**
   * Schema metadata for UI Schema Explorer
   */
  const SCHEMA_METADATA = [
    {
      table: 'tesoreria.tasas_cambio_oficial',
      badge: 'FX & LOCF Trap',
      rowCount: DATASETS.tasas_cambio_oficial.length,
      trapDescription: 'Sin registros en fines de semana/feriados (requiere LOCF con LEFT JOIN LATERAL) y contiene tasas 0.00 en días trampa (2025-12-10, 2026-09-10) para forzar NULLIF(tasa, 0).',
      columns: [
        { name: 'fecha_tasa', type: 'DATE', desc: 'Fecha hábil de publicación de tasa' },
        { name: 'tasa_eur_usd', type: 'DECIMAL(12,6)', desc: 'Factor conversión EUR -> USD (contiene 0.00 trampa)' },
        { name: 'tasa_cop_usd', type: 'DECIMAL(12,6)', desc: 'Factor conversión COP -> USD (contiene 0.00 trampa)' }
      ]
    },
    {
      table: 'crm.historico_segmento_cliente',
      badge: '1:N Duplication Trap',
      rowCount: DATASETS.historico_segmento_cliente.length,
      trapDescription: 'Clientes (CLI-1001, CLI-1002, CLI-1004) cambiaron de segmento a mitad de mes. Un JOIN directo sin DISTINCT ON (id_cliente) duplica los saldos financieros.',
      columns: [
        { name: 'id_cliente', type: 'VARCHAR', desc: 'Identificador único del cliente bancario' },
        { name: 'fecha_corte', type: 'DATE', desc: 'Fecha de actualización de segmento' },
        { name: 'segmento_comercial', type: 'VARCHAR', desc: 'Segmento asignado (CORPORATIVA, PYME, PRIVADA...)' },
        { name: 'codigo_oficina', type: 'INTEGER', desc: 'Código de sucursal gestora' }
      ]
    },
    {
      table: 'pasivos.saldos_diarios_snapshot',
      badge: 'Snapshots & AVG Trap',
      rowCount: DATASETS.saldos_diarios_snapshot.length,
      trapDescription: 'Fotos diarias en USD, EUR y COP. Incluye cuentas con días dispersos (CTA-2003 activa solo 10 días) donde AVG() falla frente a SUM()/31, y cuentas internas/garantías que deben filtrarse con leyes de De Morgan.',
      columns: [
        { name: 'id_cuenta', type: 'VARCHAR', desc: 'Número de cuenta pasiva' },
        { name: 'id_cliente', type: 'VARCHAR', desc: 'Titular de la cuenta' },
        { name: 'fecha_foto', type: 'DATE', desc: 'Fecha calendario de la fotografía diaria' },
        { name: 'desc_producto', type: 'VARCHAR', desc: 'Descripción comercial del producto' },
        { name: 'moneda', type: 'VARCHAR', desc: 'Divisa original (USD, EUR, COP)' },
        { name: 'monto_original', type: 'DECIMAL(18,2)', desc: 'Saldo al cierre del día en moneda original' }
      ]
    },
    {
      table: 'pasivos.depositos_vigencia_intervalo',
      badge: 'Intervalos [ini, fin)',
      rowCount: DATASETS.depositos_vigencia_intervalo.length,
      trapDescription: 'Intervalos semiabiertos [fecha_apertura, fecha_vencimiento). El día de vencimiento el depósito YA NO devenga (último día activo = fecha_vencimiento - 1). Incluye renovaciones el día 15 y vencimientos el día 30/31.',
      columns: [
        { name: 'id_operacion', type: 'VARCHAR', desc: 'Código del certificado o bono' },
        { name: 'id_cliente', type: 'VARCHAR', desc: 'Titular de la operación' },
        { name: 'desc_producto', type: 'VARCHAR', desc: 'Tipo de depósito a plazo' },
        { name: 'moneda', type: 'VARCHAR', desc: 'Divisa de emisión (USD)' },
        { name: 'fecha_apertura', type: 'DATE', desc: 'Inicio de vigencia (inclusivo)' },
        { name: 'fecha_vencimiento', type: 'DATE', desc: 'Fecha de vencimiento (exclusivo)' },
        { name: 'monto_nominal', type: 'DECIMAL(18,2)', desc: 'Capital nominal vigente en el intervalo' }
      ]
    },
    {
      table: 'pasivos.catalogo_cuentas',
      badge: 'Maestro Pasivos',
      rowCount: DATASETS.catalogo_cuentas.length,
      trapDescription: 'Catálogo maestro de cuentas con estado operativo (ACTIVA, INTERNA, BLOQUEADA).',
      columns: [
        { name: 'id_cuenta', type: 'VARCHAR', desc: 'Identificador de cuenta' },
        { name: 'id_cliente', type: 'VARCHAR', desc: 'Cliente titular' },
        { name: 'desc_producto', type: 'VARCHAR', desc: 'Familia de producto' },
        { name: 'moneda', type: 'VARCHAR', desc: 'Divisa de la cuenta' },
        { name: 'estado_cuenta', type: 'VARCHAR', desc: 'Estado operativo' }
      ]
    }
  ];

  global.DataDojoSeed = {
    DATASETS,
    SCHEMA_METADATA,
    buildDuckDBInitSQL
  };
})(typeof window !== 'undefined' ? window : globalThis);
