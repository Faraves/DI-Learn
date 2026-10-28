"""
DataDojo: Financial & ETL Optimizer
Deterministic AST Evaluator, Oracle-to-DuckDB Transpiler & Python ETL Sandbox (evaluator.py)

Runs 100% Client-Side inside Pyodide (WebAssembly).
Uses:
- `sqlglot` for PostgreSQL & Oracle SQL AST parsing, anti-pattern auditing, and transpilation to DuckDB.
- `ast` & `tracemalloc` & `pandas` for Python ETL AST inspection, memory profiling, and Golden Dataset verification.
- Enforces DRY Dynamic Parameterization (`WITH params AS (...)` in SQL, single `fecha_base` in Python/M/R).
"""

import ast
import json
import re
import time
import tracemalloc
from typing import Any, Dict, List

try:
    import sqlglot
    from sqlglot import exp
    HAS_SQLGLOT = True
except ImportError:
    HAS_SQLGLOT = False

try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ==============================================================================
# 1. ORACLE SQL -> DUCKDB TRANSPILER (WITH SQLGLOT + REGEX PREPROCESSOR)
# ==============================================================================

def preprocess_oracle_sql(sql_text: str) -> str:
    """
    Normalizes Oracle SQL idioms (TRUNC, LAST_DAY, TO_DATE, NVL, DECODE, FROM DUAL)
    so dynamic parameter CTEs (`WITH params AS (...)`) and queries transpile cleanly to DuckDB.
    """
    cleaned = sql_text.strip().rstrip(";")

    # Convert Oracle TRUNC(expr, 'MM') or TRUNC(expr, 'MONTH') -> CAST(DATE_TRUNC('month', CAST(expr AS DATE)) AS DATE)
    cleaned = re.sub(
        r"\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'(?:MM|MON|MONTH)'\s*\)",
        r"CAST(DATE_TRUNC('month', CAST(\1 AS DATE)) AS DATE)",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Convert Oracle TRUNC(expr, 'YYYY') -> CAST(DATE_TRUNC('year', CAST(expr AS DATE)) AS DATE)
    cleaned = re.sub(
        r"\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'(?:YYYY|YY|YEAR)'\s*\)",
        r"CAST(DATE_TRUNC('year', CAST(\1 AS DATE)) AS DATE)",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Convert Oracle LAST_DAY(expr) -> CAST(LAST_DAY(CAST(expr AS DATE)) AS DATE)
    cleaned = re.sub(
        r"\bLAST_DAY\s*\(\s*([^)]+?)\s*\)",
        r"CAST(LAST_DAY(CAST(\1 AS DATE)) AS DATE)",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Convert Oracle TO_DATE('YYYY-MM-DD', 'YYYY-MM-DD') -> CAST('YYYY-MM-DD' AS DATE)
    cleaned = re.sub(
        r"\bTO_DATE\s*\(\s*('[^']+')\s*,\s*'[^']+'\s*\)",
        r"CAST(\1 AS DATE)",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Convert NVL(a, b) -> COALESCE(a, b)
    cleaned = re.sub(
        r"\bNVL\s*\(",
        "COALESCE(",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Convert FROM DUAL -> omit for DuckDB scalar subquery compatibility
    cleaned = re.sub(
        r"\bFROM\s+DUAL\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned


def transpile_to_duckdb(sql_text: str, dialect: str = "postgres") -> Dict[str, Any]:
    """
    Transpiles PostgreSQL or Oracle SQL into valid DuckDB SQL using sqlglot.
    """
    source_dialect = "oracle" if dialect.lower().startswith("oracle") else "postgres"
    normalized_sql = preprocess_oracle_sql(sql_text) if source_dialect == "oracle" else sql_text.strip().rstrip(";")

    if HAS_SQLGLOT:
        try:
            transpiled_list = sqlglot.transpile(
                normalized_sql,
                read=source_dialect,
                write="duckdb",
                pretty=True,
            )
            if transpiled_list:
                return {
                    "success": True,
                    "duckdb_sql": transpiled_list[0],
                    "ast_valid": True,
                    "dialect_used": source_dialect,
                    "error": None,
                }
        except Exception as exc:
            try:
                transpiled_list = sqlglot.transpile(
                    normalized_sql,
                    read="postgres",
                    write="duckdb",
                    pretty=True,
                )
                return {
                    "success": True,
                    "duckdb_sql": transpiled_list[0],
                    "ast_valid": True,
                    "dialect_used": "postgres-fallback",
                    "error": None,
                }
            except Exception:
                return {
                    "success": False,
                    "duckdb_sql": normalized_sql,
                    "ast_valid": False,
                    "dialect_used": source_dialect,
                    "error": str(exc),
                }

    return {
        "success": True,
        "duckdb_sql": normalized_sql,
        "ast_valid": True,
        "dialect_used": source_dialect,
        "error": None,
    }


# ==============================================================================
# 2. DETERMINISTIC SQL AST AUDITOR (SQLGLOT + DRY PARAMETERIZATION RULES)
# ==============================================================================

def strip_sql_comments(sql_text: str) -> str:
    """Removes -- single line and /* */ block comments before AST regex inspection."""
    no_block = re.sub(r"/\*[\s\S]*?\*/", " ", sql_text)
    no_line = re.sub(r"--[^\n]*", " ", no_block)
    return no_line


def audit_sql_ast(sql_text: str, dialect: str = "postgres", challenge_id: str = "") -> List[Dict[str, str]]:
    """
    Analyzes a SQL query (PostgreSQL or Oracle SQL) and returns pedagogical alerts
    for anti-patterns and violations of DRY Dynamic Parameterization.
    """
    alerts: List[Dict[str, str]] = []
    clean_sql = strip_sql_comments(sql_text)
    upper_sql = clean_sql.upper()

    # 0A. DRY DYNAMIC PARAMETERIZATION CHECK: Magic Divisor (/ 31, / 31.0, / 30, / 30.0, / 28, / 29)
    if re.search(r"/\s*(?:31|30|28|29)(?:\.0+)?\b", clean_sql):
        alerts.append({
            "code": "HARDCODED_MAGIC_DAYS",
            "severity": "critical",
            "title": "Anti-Pattern DRY: Divisor de Días Calendario Quemado (/ 31 o / 30)",
            "message": (
                "Estás dividiendo entre un número mágico fijo (`/ 31.0` o `/ 30.0`). "
                "Por buenas prácticas de arquitectura, define `fecha_base` una sola vez en un CTE `params`, "
                "calcula dinámicamente `dias_mes = (fin_mes - ini_mes + 1)` y divide entre `p.dias_mes`."
            ),
            "wikilink": "[[Parametrización Dinámica DRY]]",
        })

    # 0B. DRY DYNAMIC PARAMETERIZATION CHECK: Repeated Hardcoded Date Literals ('YYYY-MM-DD')
    date_literals = re.findall(r"'202\d-\d{2}-\d{2}'", clean_sql)
    max_allowed_dates = 4 if challenge_id == "CH-06" else 1
    if len(date_literals) > max_allowed_dates:
        alerts.append({
            "code": "HARDCODED_REPEATED_DATES",
            "severity": "critical",
            "title": f"Violación DRY: {len(date_literals)} Fechas Literales Quemadas en la Consulta",
            "message": (
                f"Se detectaron {len(date_literals)} fechas quemadas ({', '.join(date_literals[:4])}) repartidas en el código. "
                f"Debes definir la fecha objetivo **una única vez** ({max_allowed_dates} fecha por periodo en el CTE inicial `params`) "
                "y derivar dinámicamente `ini_mes`, `fin_mes` (`LAST_DAY`) y `dias_mes`, referenciando `p.ini_mes` y `p.fin_mes` en toda la consulta."
            ),
            "wikilink": "[[Parametrización Dinámica DRY]]",
        })

    # 1. NON-SARGABLE PREDICATES IN WHERE (Applying functions to table date columns in WHERE)
    non_sargable_pattern = re.compile(
        r"\bWHERE\b[\s\S]*?\b(?:DATE_TRUNC|EXTRACT|TO_CHAR|TRUNC|YEAR|MONTH)\s*\([^)]*?\b(?:fecha_foto|fecha_tasa|fecha_corte|fecha_apertura|fecha_vencimiento)\b",
        re.IGNORECASE,
    )
    if non_sargable_pattern.search(clean_sql):
        alerts.append({
            "code": "NON_SARGABLE_PREDICATE",
            "severity": "critical",
            "title": "Predicado Non-Sargable detectado en cláusula WHERE",
            "message": (
                "Estás envolviendo una columna de tabla (`fecha_foto`, `fecha_corte`, etc.) dentro de una función "
                "(`DATE_TRUNC`, `EXTRACT`, `TO_CHAR` o `TRUNC`) en el `WHERE`. Esto anula los índices B-Tree "
                "y Zone Maps. Calcula `p.ini_mes` y `p.fin_mes` en el CTE `params` y filtra con `fecha_foto BETWEEN p.ini_mes AND p.fin_mes`."
            ),
            "wikilink": "[[Predicados Sargables]]",
        })

    # 2. TRAMPA DEL AVG() BANCARIO
    if re.search(r"\bAVG\s*\(", clean_sql, re.IGNORECASE):
        alerts.append({
            "code": "BANKING_AVG_TRAP",
            "severity": "critical",
            "title": "Trampa del AVG() Bancario en Saldos Promedios",
            "message": (
                "Detectado uso de `AVG()` directo sobre filas. En banca, una cuenta abierta a mitad de mes "
                "tendrá menos registros físicos y `AVG(monto)` inflará el promedio. "
                "Regla de oro: `SUM(monto) / p.dias_mes`."
            ),
            "wikilink": "[[Trampa del AVG Bancario]]",
        })

    # 3. EXPLOSIÓN DE RAM EN INTERVALOS (generate_series on interval deposits)
    if re.search(r"\bGENERATE_SERIES\s*\(", clean_sql, re.IGNORECASE) and re.search(
        r"\bdepositos_vigencia_intervalo\b", clean_sql, re.IGNORECASE
    ):
        alerts.append({
            "code": "INTERVAL_RAM_EXPLOSION",
            "severity": "critical",
            "title": "Explosión de RAM por Expansión Diaria con generate_series()",
            "message": (
                "Desglosar contratos de `depositos_vigencia_intervalo` día por día mediante `generate_series` multiplica "
                "la cardinalidad por 365x y satura la RAM. Calcula la intersección en O(1) con Álgebra de Intervalos: "
                "`GREATEST(0, (LEAST(d.fecha_vencimiento - INTERVAL '1 day', p.fin_mes)::DATE - GREATEST(d.fecha_apertura, p.ini_mes)::DATE) + 1)`."
            ),
            "wikilink": "[[Álgebra de Intervalos]]",
        })

    # Check half-open interval boundary mistake: fecha_vencimiento >= fin_mes
    if re.search(r"\bdepositos_vigencia_intervalo\b", clean_sql, re.IGNORECASE):
        if re.search(r"fecha_vencimiento\s*>=\s*(?:'202\d-\d{2}-\d{2}'|p\.fin_mes|fin_mes|fecha_cierre)", clean_sql, re.IGNORECASE):
            alerts.append({
                "code": "HALF_OPEN_INTERVAL_BOUNDARY_BUG",
                "severity": "critical",
                "title": "Error de Frontera en Intervalo Semiabierto [apertura, vencimiento)",
                "message": (
                    "En `depositos_vigencia_intervalo`, `fecha_vencimiento` es el día en que vence el certificado "
                    "(último día activo real = `fecha_vencimiento - 1`). Usar `fecha_vencimiento >= p.fin_mes` "
                    "incluye erróneamente contratos que ya vencieron ese día. "
                    "Para saldo puntual al cierre usa `d.fecha_apertura <= p.fin_mes AND d.fecha_vencimiento > p.fin_mes`."
                ),
                "wikilink": "[[Snapshots vs Intervalos Semiabiertos]]",
            })

    # 4. LÓGICA BOOLEANA ROTA (De Morgan violation: NOT LIKE ... OR NOT LIKE ...)
    broken_demorgan = re.compile(
        r"\bNOT\s+I?LIKE\s+'[^']+'\s+OR\s+(?:\w+\.)?desc_producto\s+NOT\s+I?LIKE\b",
        re.IGNORECASE,
    )
    if broken_demorgan.search(clean_sql):
        alerts.append({
            "code": "BROKEN_BOOLEAN_LOGIC",
            "severity": "critical",
            "title": "Violación de las Leyes de De Morgan (NOT ILIKE ... OR NOT ILIKE ...)",
            "message": (
                "Combinar dos exclusiones `NOT ILIKE '%A%' OR NOT ILIKE '%B%'` evalúa a `TRUE` para todas las filas. "
                "Para excluir múltiples patrones usa `NOT ILIKE '%A%' AND NOT ILIKE '%B%'`."
            ),
            "wikilink": "[[Leyes de De Morgan en SQL]]",
        })

    # 5. FX INNER JOIN & MISSING ZERO GUARD
    if (
        "SALDOS_DIARIOS_SNAPSHOT" in upper_sql
        and "TASAS_CAMBIO_OFICIAL" in upper_sql
        and "INNER JOIN" in upper_sql
    ):
        alerts.append({
            "code": "FX_INNER_JOIN_DATA_LOSS",
            "severity": "critical",
            "title": "Pérdida de Saldos de Fin de Semana por INNER JOIN con Tasas",
            "message": (
                "`tesoreria.tasas_cambio_oficial` no tiene registros en sábados, domingos ni feriados. "
                "Un `INNER JOIN` elimina ~30% de los saldos diarios del mes. Usa **LOCF** con `LEFT JOIN LATERAL`."
            ),
            "wikilink": "[[LOCF con LEFT JOIN LATERAL]]",
        })

    if "TASAS_CAMBIO_OFICIAL" in upper_sql:
        has_zero_guard = (
            "NULLIF" in upper_sql
            or "> 0" in upper_sql
            or "!= 0" in upper_sql
            or "<> 0" in upper_sql
        )
        if not has_zero_guard:
            alerts.append({
                "code": "MISSING_ZERO_RATE_GUARD",
                "severity": "critical",
                "title": "Riesgo de Tasa 0.00 sin protección NULLIF / Filtro > 0",
                "message": (
                    "`tesoreria.tasas_cambio_oficial` contiene días trampa con tasa `0.00`. "
                    "Filtra `NULLIF(tasa, 0) IS NOT NULL` dentro de la búsqueda LOCF."
                ),
                "wikilink": "[[LOCF con LEFT JOIN LATERAL]]",
            })

    # 6. CRM JOIN DUPLICATION
    if "HISTORICO_SEGMENTO_CLIENTE" in upper_sql:
        has_dedup = (
            "DISTINCT ON" in upper_sql
            or "ROW_NUMBER" in upper_sql
            or "KEEP" in upper_sql
            or "ARG_MAX" in upper_sql
        )
        if not has_dedup:
            alerts.append({
                "code": "CRM_JOIN_DUPLICATION",
                "severity": "critical",
                "title": "Duplicación de Saldos por JOIN 1:N con Histórico de Segmentos CRM",
                "message": (
                    "`crm.historico_segmento_cliente` tiene múltiples registros en el mismo mes para clientes que "
                    "cambiaron de segmento. Deduplica primero con `DISTINCT ON (id_cliente) ... WHERE fecha_corte <= p.fin_mes ORDER BY id_cliente, fecha_corte DESC`."
                ),
                "wikilink": "[[Deduplicación con DISTINCT ON]]",
            })

    # 7. UNION WITHOUT ALL
    if re.search(r"\bUNION\b(?!\s+ALL\b)", clean_sql, re.IGNORECASE):
        alerts.append({
            "code": "UNION_WITHOUT_ALL",
            "severity": "warning",
            "title": "Uso de UNION en lugar de UNION ALL (Sort innecesario)",
            "message": (
                "`UNION` sin `ALL` fuerza un Sort/Dedup innecesario. Usa siempre `UNION ALL` al consolidar pasivos pre-agregados."
            ),
            "wikilink": "[[Pre-Agregación]]",
        })

    return alerts


# ==============================================================================
# 3. PYTHON ETL AST AUDITOR & SANDBOX EXECUTOR (PANDAS + TRACEMALLOC)
# ==============================================================================

def audit_python_ast(py_code: str) -> List[Dict[str, str]]:
    """
    Uses Python's native `ast` module to inspect user ETL scripts for anti-patterns:
    - `.iterrows()` or `.itertuples()`
    - `.apply(..., axis=1)`
    - Hardcoded magic divisors (`/ 31.0`, `/ 30.0`) or repeated hardcoded date strings
    """
    alerts: List[Dict[str, str]] = []
    try:
        tree = ast.parse(py_code)
    except SyntaxError as exc:
        alerts.append({
            "code": "PYTHON_SYNTAX_ERROR",
            "severity": "critical",
            "title": f"Error de Sintaxis en Python (Línea {exc.lineno})",
            "message": str(exc),
            "wikilink": "[[Vectorización ETL en Python]]",
        })
        return alerts

    # Remove comments for regex inspection of literals
    no_comments = re.sub(r"#[^\n]*", "", py_code)

    # Check DRY Dynamic Parameterization in Python
    if re.search(r"/\s*(?:31|30|28|29)(?:\.0+)?\b", no_comments):
        alerts.append({
            "code": "HARDCODED_MAGIC_DAYS",
            "severity": "critical",
            "title": "Anti-Pattern DRY en Python: Divisor de Días Fijo (/ 31.0)",
            "message": (
                "Estás dividiendo entre `31.0` quemado en código. Deriva dinámicamente los días del mes "
                "a partir de `fecha_base`: `ini_mes = fecha_base.replace(day=1)`, "
                "`fin_mes = ini_mes + pd.offsets.MonthEnd(1)` y `dias_mes = float(fin_mes.day)`."
            ),
            "wikilink": "[[Parametrización Dinámica DRY]]",
        })

    date_literals = re.findall(r"['\"]202\d-\d{2}-\d{2}['\"]", no_comments)
    if len(date_literals) > 1:
        alerts.append({
            "code": "HARDCODED_REPEATED_DATES",
            "severity": "critical",
            "title": f"Violación DRY en Python: {len(date_literals)} Fechas Quemadas",
            "message": (
                f"Se encontraron {len(date_literals)} fechas literales ({', '.join(date_literals)}). "
                "Define `fecha_base = pd.Timestamp('2025-12-01')` una única vez al inicio de la función "
                "y calcula dinámicamente `ini_mes`, `fin_mes` (`pd.offsets.MonthEnd(1)`) y `dias_mes`."
            ),
            "wikilink": "[[Parametrización Dinámica DRY]]",
        })

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr_name = node.func.attr
            if attr_name in ("iterrows", "itertuples"):
                alerts.append({
                    "code": "PYTHON_ITERROWS_ANTIPATTERN",
                    "severity": "critical",
                    "title": f"Anti-Pattern Crítico: Uso de `.{attr_name}()` en Pandas",
                    "message": (
                        f"Iterar fila por fila con `.{attr_name}()` instancia objetos en Python y es 1,000x más lento. "
                        "Usa operaciones vectorizadas (`np.maximum`, `np.minimum`, `np.where`, `np.select`, `pd.merge_asof`)."
                    ),
                    "wikilink": "[[Vectorización ETL en Python]]",
                })
            elif attr_name == "apply":
                has_axis_1 = any(
                    (kw.arg == "axis" and isinstance(kw.value, ast.Constant) and kw.value.value in (1, "columns"))
                    for kw in node.keywords
                )
                if has_axis_1:
                    alerts.append({
                        "code": "PYTHON_APPLY_AXIS1_ANTIPATTERN",
                        "severity": "critical",
                        "title": "Anti-Pattern de CPU/RAM: `.apply(..., axis=1)`",
                        "message": (
                            "`.apply(axis=1)` ejecuta un bucle de Python sobre cada fila del DataFrame. "
                            "Usa aritmética de columnas vectorizada o `np.where` / `np.select`."
                        ),
                        "wikilink": "[[Vectorización ETL en Python]]",
                    })

    return alerts


def execute_python_etl(py_code: str, datasets_json: str) -> Dict[str, Any]:
    """
    Executes a user Python ETL script inside Pyodide with `tracemalloc` profiling.
    """
    alerts = audit_python_ast(py_code)
    if any(a["code"] == "PYTHON_SYNTAX_ERROR" for a in alerts):
        return {
            "success": False,
            "error": alerts[0]["message"],
            "alerts": alerts,
            "rows": [],
            "columns": [],
            "execution_ms": 0.0,
            "peak_kb": 0.0,
        }

    if not HAS_PANDAS:
        return {
            "success": False,
            "error": "Pandas no está cargado aún en Pyodide.",
            "alerts": alerts,
            "rows": [],
            "columns": [],
            "execution_ms": 0.0,
            "peak_kb": 0.0,
        }

    raw_data = json.loads(datasets_json)
    dfs = {k: pd.DataFrame(v) for k, v in raw_data.items()}

    sandbox_globals: Dict[str, Any] = {
        "pd": pd,
        "np": np,
        "datasets": dfs,
    }

    tracemalloc.start()
    t0 = time.perf_counter()
    try:
        exec(py_code, sandbox_globals)
        if "transform_pasivos" in sandbox_globals and callable(sandbox_globals["transform_pasivos"]):
            result_df = sandbox_globals["transform_pasivos"](dfs)
        elif "result_df" in sandbox_globals:
            result_df = sandbox_globals["result_df"]
        else:
            raise ValueError(
                "Tu script debe definir la función `transform_pasivos(datasets)` que retorne un DataFrame de Pandas."
            )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        if not isinstance(result_df, pd.DataFrame):
            raise TypeError("El resultado retornado no es un `pandas.DataFrame`.")

        out_df = result_df.copy()
        for col in out_df.columns:
            if pd.api.types.is_datetime64_any_dtype(out_df[col]):
                out_df[col] = out_df[col].dt.strftime("%Y-%m-%d")
            elif pd.api.types.is_float_dtype(out_df[col]):
                out_df[col] = out_df[col].round(2)

        records = out_df.to_dict(orient="records")
        return {
            "success": True,
            "error": None,
            "alerts": alerts,
            "rows": records,
            "columns": list(out_df.columns),
            "execution_ms": round(elapsed_ms, 2),
            "peak_kb": round(peak_bytes / 1024.0, 2),
        }
    except Exception as exc:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        return {
            "success": False,
            "error": str(exc),
            "alerts": alerts,
            "rows": [],
            "columns": [],
            "execution_ms": 0.0,
            "peak_kb": 0.0,
        }
