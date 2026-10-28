# 🥋 DataDojo: Financial & ETL Optimizer

**Progressive Web App (PWA) 100% Client-Side (Serverless), Offline-First y desplegable en GitHub Pages** diseñada para entrenar pensamiento algorítmico de nivel *Staff / Principal Data Engineer* en **PostgreSQL, Oracle SQL, Python (Pandas/NumPy), Power Query (M) y R (`data.table`)**.

---

## 🏗️ 1. Arquitectura Técnica (Zero-Backend)

1. **Motor Analítico SQL en el Navegador (`DuckDB-Wasm`):**
   - Ejecuta consultas avanzadas compatibles con PostgreSQL (`DISTINCT ON`, `LEFT JOIN LATERAL`, `FILTER (WHERE ...)`, `GREATEST/LEAST`, `VALUES`, Window Functions y planes `EXPLAIN`).
2. **Motor Python & Analizador AST (`Pyodide` + `sqlglot` + `pandas` + `tracemalloc`):**
   - Carga diferida (*lazy-loading*) de WebAssembly que instala `sqlglot` vía `micropip` para parsear el Árbol Sintáctico Abstracto (AST) de **Oracle SQL** (`DECODE`, `NVL`, `TO_DATE`, `TRUNC(fecha, 'MM')`, `FETCH FIRST`) y transpilarlo automáticamente a DuckDB.
   - Ejecuta pipelines de **Python ETL** midiendo tiempo en milisegundos y consumo pico de memoria RAM con `tracemalloc`.
3. **100% Offline-First PWA (`manifest.json` + `sw.js`):**
   - Estrategia *Cache-First* que almacena localmente el App Shell, el catálogo de retos y los binarios WebAssembly (`.wasm`, `.whl`). Funciona incluso en **modo avión** tras la primera apertura.
4. **Puente "Second Brain Sync" para Obsidian (`.md`):**
   - Cada reto y cada tarjeta teórica incluye exportación directa a archivos `.md` con *YAML Frontmatter*, comparativa *Anti-Pattern vs. Código Optimizado* y enlaces internos (`[[Álgebra de Intervalos]]`, `[[Predicados Sargables]]`, `[[Pre-Agregación]]`, `[[LOCF con LEFT JOIN LATERAL]]`, `[[Deduplicación con DISTINCT ON]]`).

---

## 🗄️ 2. Ecosistema Bancario Sintético Camuflado (`assets/js/seed_data.js`)

| Tabla | Propósito y Casos Trampa Incluidos |
| :--- | :--- |
| `tesoreria.tasas_cambio_oficial` | Sin registros en sábados, domingos ni feriados (obliga a usar **LOCF** con `LEFT JOIN LATERAL` o `pd.merge_asof`) y tasas `0.00` en días trampa (`2025-12-10`, `2026-09-10`) para forzar el uso de `NULLIF(tasa, 0)`. |
| `crm.historico_segmento_cliente` | Clientes (`CLI-1001`, `CLI-1002`, `CLI-1004`) con cambios de segmento a mitad de mes para castigar `JOINs` ingenuos que duplican saldos si no se aplica `DISTINCT ON (id_cliente)`. |
| `pasivos.saldos_diarios_snapshot` | Fotografías diarias en `USD`, `EUR` y `COP`, cuentas con días dispersos (`CTA-2003` activa solo 10 días donde `AVG()` falla frente a `SUM()/31`) y cuentas internas/garantías para evaluar las Leyes de De Morgan (`NOT ILIKE ... AND NOT ILIKE ...`). |
| `pasivos.depositos_vigencia_intervalo` | Modelo de intervalos semiabiertos `[fecha_apertura, fecha_vencimiento)` (último día activo real = `fecha_vencimiento - 1`), con certificados que vencen y se renuevan el día 15 y vencimientos el último día del mes. |

---

## 🚀 3. Ejecución Local Rápida

```bash
cd DI-Learn
python -m http.server 8080
```
Abre `http://localhost:8080` en tu navegador.
*(Atajo en el editor: `Ctrl + Enter` o `Cmd + Enter` para ejecutar y auditar tu código).*
