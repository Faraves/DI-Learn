"""
DataDojo: Financial & ETL Optimizer
Deterministic AST Evaluator, Oracle-to-DuckDB Transpiler & Python ETL Sandbox (evaluator.py)

Runs 100% Client-Side inside Pyodide (WebAssembly).
Uses:
- `sqlglot` for PostgreSQL & Oracle SQL AST parsing, anti-pattern auditing, and transpilation to DuckDB.
- `ast` & `tracemalloc` & `pandas` for Python ETL AST inspection, memory profiling, and Golden Dataset verification.
"""

import ast
import json
import re
import time
import tracemalloc
from typing import Any, Dict, List, Optional, Tuple

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
# 1. ORACLE SQL -> DUCKDB TRANSPILER (WITH SQLGLOT + REGEX FALLBACK ENHANCEMENTS)
# ==============================================================================

def preprocess_oracle_sql(sql_text: str) -> str:
    """
    Normalizes common Oracle SQL idioms before passing to sqlglot or DuckDB so that
    Oracle constructs like TRUNC(d, 'MM'), NVL, DECODE, TO_DATE, and ROWNUM transpile cleanly.
    """
    cleaned = sql_text.strip().rstrip(";")

    # Convert Oracle TRUNC(expr, 'MM') or TRUNC(expr, 'MONTH') -> DATE_TRUNC('month', CAST(expr AS DATE))
    cleaned = re.sub(
        r"\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'(?:MM|MON|MONTH)'\s*\)",
        r"DATE_TRUNC('month', CAST(\1 AS DATE))",
        cleaned,
        flags=re.IGNORECASE,
    )
    # Convert Oracle TRUNC(expr, 'YYYY') -> DATE_TRUNC('year', CAST(expr AS DATE))
    cleaned = re.sub(
        r"\bTRUNC\s*\(\s*([^,]+?)\s*,\s*'(?:YYYY|YY|YEAR)'\s*\)",
        r"DATE_TRUNC('year', CAST(\1 AS DATE))",
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
    # Convert FROM DUAL -> omit or (SELECT 1)
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
    Returns { "success": bool, "duckdb_sql": str, "ast_valid": bool, "error": str | None }
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
            # Fallback attempt with generic read dialect if Oracle has mixed PG/ANSI constructs
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
# 2. DETERMINISTIC SQL AST AUDITOR (SQLGLOT + STRUCTURAL RULES)
# ==============================================================================

def audit_sql_ast(sql_text: str, dialect: str = "postgres", challenge_id: str = "") -> List[Dict[str, str]]:
    """
    Analyzes a SQL query (PostgreSQL or Oracle SQL) and returns pedagogical alerts
    if any of the 6 core financial/performance anti-patterns are detected.
    """
    alerts: List[Dict[str, str]] = []
    upper_sql = sql_text.upper()

    # 1. NON-SARGABLE PREDICATES IN WHERE
    # Check if date_trunc, EXTRACT, TO_CHAR, or TRUNC is applied to fecha_foto / fecha_tasa / fecha_corte in WHERE
    non_sargable_pattern = re.compile(
        r"\bWHERE\b[\s\S]*?\b(?:DATE_TRUNC|EXTRACT|TO_CHAR|TRUNC|YEAR|MONTH)\s*\([^)]*?\b(?:fecha_foto|fecha_tasa|fecha_corte|fecha_apertura|fecha_vencimiento)\b",
        re.IGNORECASE,
    )
    if non_sargable_pattern.search(sql_text):
        alerts.append({
            "code": "NON_SARGABLE_PREDICATE",
            "severity": "critical",
            "title": "Predicado Non-Sargable detectado en cláusula WHERE",
            "message": (
                "Estás envolviendo una columna de fecha (ej. `fecha_foto` o `fecha_corte`) dentro de una función "
                "(`DATE_TRUNC`, `EXTRACT`, `TO_CHAR` o `TRUNC`) en el `WHERE`. Esto impide el uso de índices B-Tree "
                "y Zone Maps (Partition Pruning), forzando un Full Table Scan fila por fila. "
                "Usa rangos explícitos: `fecha_foto BETWEEN 'YYYY-MM-01' AND 'YYYY-MM-31'`."
            ),
            "wikilink": "[[Predicados Sargables]]",
        })

    # 2. TRAMPA DEL AVG() BANCARIO
    # Using AVG(monto...) or AVG(CASE ... ELSE 0 END) or AVG(NVL(...)) instead of SUM(...) / dias_calendario
    if re.search(r"\bAVG\s*\(", sql_text, re.IGNORECASE):
        alerts.append({
            "code": "BANKING_AVG_TRAP",
            "severity": "critical",
            "title": "Trampa del AVG() Bancario en Saldos Promedios",
            "message": (
                "Detectado uso de `AVG()` directo sobre filas. En banca, una cuenta abierta a mitad de mes o sin "
                "fotos en días con saldo cero tendrá menos de 30/31 registros, por lo que `AVG(monto)` dividirá entre "
                "las filas presentes (ej. 10) e inflará artificialmente el saldo promedio. "
                "Regla de oro: `SUM(monto) / dias_calendario_del_mes` (30 o 31)."
            ),
            "wikilink": "[[Trampa del AVG Bancario]]",
        })

    # 3. EXPLOSIÓN DE RAM EN INTERVALOS (generate_series on interval deposits)
    if re.search(r"\bGENERATE_SERIES\s*\(", sql_text, re.IGNORECASE) and re.search(
        r"\bdepositos_vigencia_intervalo\b", sql_text, re.IGNORECASE
    ):
        alerts.append({
            "code": "INTERVAL_RAM_EXPLOSION",
            "severity": "critical",
            "title": "Explosión de RAM por Expansión Diaria con generate_series()",
            "message": (
                "Desglosar contratos de `depositos_vigencia_intervalo` día por día mediante `generate_series` multiplica "
                "la cardinalidad por 365x y satura la memoria RAM (O(N×D)). Calcula la intersección de días activos "
                "en tiempo constante O(1) con Álgebra de Intervalos: "
                "`GREATEST(0, (LEAST(fecha_vencimiento - 1, fin_mes) - GREATEST(fecha_apertura, ini_mes)) + 1)`."
            ),
            "wikilink": "[[Álgebra de Intervalos]]",
        })

    # Check half-open interval boundary mistake: fecha_vencimiento >= '2025-12-31' for closing balance
    if re.search(r"\bdepositos_vigencia_intervalo\b", sql_text, re.IGNORECASE):
        if re.search(r"fecha_vencimiento\s*>=\s*(?:'202\d-\d{2}-\d{2}'|fin_mes|fecha_cierre)", sql_text, re.IGNORECASE):
            alerts.append({
                "code": "HALF_OPEN_INTERVAL_BOUNDARY_BUG",
                "severity": "warning",
                "title": "Error de Frontera en Intervalo Semiabierto [apertura, vencimiento)",
                "message": (
                    "En `depositos_vigencia_intervalo`, `fecha_vencimiento` es el día en que vence el certificado "
                    "(último día activo real = `fecha_vencimiento - 1`). Usar `fecha_vencimiento >= fecha_cierre` "
                    "incluye erróneamente contratos que ya vencieron ese día o duplica renovaciones del día 15. "
                    "Para saldo puntual al cierre usa `fecha_apertura <= fecha_cierre AND fecha_vencimiento > fecha_cierre`."
                ),
                "wikilink": "[[Snapshots vs Intervalos Semiabiertos]]",
            })

    # 4. LÓGICA BOOLEANA ROTA (De Morgan violation: NOT LIKE ... OR NOT LIKE ...)
    broken_demorgan = re.compile(
        r"\bNOT\s+I?LIKE\s+'[^']+'\s+OR\s+(?:\w+\.)?desc_producto\s+NOT\s+I?LIKE\b",
        re.IGNORECASE,
    )
    if broken_demorgan.search(sql_text):
        alerts.append({
            "code": "BROKEN_BOOLEAN_LOGIC",
            "severity": "critical",
            "title": "Violación de las Leyes de De Morgan (NOT ILIKE ... OR NOT ILIKE ...)",
            "message": (
                "Combinar dos exclusiones `NOT ILIKE '%A%' OR NOT ILIKE '%B%'` evalúa a `TRUE` para absolutamente "
                "todas las filas (porque ningún producto es simultáneamente '%A%' y '%B%'). "
                "Para excluir múltiples patrones debes usar conjunción lógica: `NOT ILIKE '%A%' AND NOT ILIKE '%B%'`."
            ),
            "wikilink": "[[Leyes de De Morgan en SQL]]",
        })

    # 5. FALTA DE PRE-AGREGACIÓN (Late Aggregation on FX or CRM joins)
    # Detect if joining saldos_diarios_snapshot directly with tasas_cambio_oficial without pre-aggregating or filtering
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
                "Un `INNER JOIN` elimina ~30% de los saldos diarios del mes. Debes usar **LOCF (Last Observation Carried Forward)** "
                "con `LEFT JOIN LATERAL (... WHERE t.fecha_tasa <= s.fecha_foto AND NULLIF(t.tasa, 0) IS NOT NULL ORDER BY t.fecha_tasa DESC LIMIT 1)`."
            ),
            "wikilink": "[[LOCF con LEFT JOIN LATERAL]]",
        })

    # Check if tasas_cambio_oficial is queried without NULLIF or > 0 protection
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
                "severity": "warning",
                "title": "Riesgo de Tasa 0.00 sin protección NULLIF / Filtro > 0",
                "message": (
                    "`tesoreria.tasas_cambio_oficial` contiene días trampa con tasa `0.00`. "
                    "Asegúrate de filtrar `tasa > 0` o envolver la columna con `NULLIF(tasa, 0)` antes de dividir o arrastrar con LOCF."
                ),
                "wikilink": "[[LOCF con LEFT JOIN LATERAL]]",
            })

    # Check un-deduplicated join with crm.historico_segmento_cliente
    if "HISTORICO_SEGMENTO_CLIENTE" in upper_sql:
        has_dedup = (
            "DISTINCT ON" in upper_sql
            or "ROW_NUMBER" in upper_sql
            or "KEEP" in upper_sql
            or "MAX(" in upper_sql
            or "ARG_MAX" in upper_sql
        )
        if not has_dedup:
            alerts.append({
                "code": "CRM_JOIN_DUPLICATION",
                "severity": "critical",
                "title": "Duplicación de Saldos por JOIN 1:N con Histórico de Segmentos CRM",
                "message": (
                    "`crm.historico_segmento_cliente` tiene múltiples registros en el mismo mes para clientes que "
                    "cambiaron de segmento. Hacer `JOIN` directo multiplica sus saldos por 2x. "
                    "Deduplica primero en un CTE con `DISTINCT ON (id_cliente) ... ORDER BY id_cliente, fecha_corte DESC`."
                ),
                "wikilink": "[[Deduplicación con DISTINCT ON]]",
            })

    # 6. UNION ASYMMETRY OR EXPENSIVE UNION (SORT DEDUP) INSTEAD OF UNION ALL
    if re.search(r"\bUNION\b(?!\s+ALL\b)", sql_text, re.IGNORECASE):
        alerts.append({
            "code": "UNION_WITHOUT_ALL",
            "severity": "warning",
            "title": "Uso de UNION en lugar de UNION ALL (Sort innecesario)",
            "message": (
                "`UNION` sin `ALL` fuerza al motor SQL a ordenar y deduplicar todas las filas consolidadas, "
                "consumiendo CPU y memoria adicional y pudiendo eliminar registros legítimos con montos idénticos. "
                "Usa siempre `UNION ALL` al consolidar tablas de pasivos."
            ),
            "wikilink": "[[Pre-Agregación]]",
        })

    # Additional AST checks via sqlglot when available
    if HAS_SQLGLOT:
        try:
            read_dialect = "oracle" if dialect.lower().startswith("oracle") else "postgres"
            parsed = sqlglot.parse_one(
                preprocess_oracle_sql(sql_text) if read_dialect == "oracle" else sql_text,
                read=read_dialect,
            )
            # Check for UNION ALL column count symmetry
            for union_node in parsed.find_all(exp.Union):
                left_select = union_node.left
                right_select = union_node.right
                if isinstance(left_select, exp.Select) and isinstance(right_select, exp.Select):
                    if len(left_select.expressions) != len(right_select.expressions):
                        alerts.append({
                            "code": "UNION_COLUMN_MISMATCH",
                            "severity": "critical",
                            "title": "Insimetría de Columnas en UNION ALL",
                            "message": (
                                f"El bloque izquierdo proyecta {len(left_select.expressions)} columnas pero el bloque "
                                f"derecho proyecta {len(right_select.expressions)} columnas. Ambos bloques deben estar "
                                "estrictamente alineados en cantidad y tipo de datos."
                            ),
                            "wikilink": "[[Pre-Agregación]]",
                        })
        except Exception:
            pass

    return alerts


# ==============================================================================
# 3. PYTHON ETL AST AUDITOR & SANDBOX EXECUTOR (PANDAS + TRACEMALLOC)
# ==============================================================================

def audit_python_ast(py_code: str) -> List[Dict[str, str]]:
    """
    Uses Python's native `ast` module to inspect user ETL scripts for anti-patterns:
    - `.iterrows()` or `.itertuples()`
    - `.apply(..., axis=1)`
    - Explicit `for` loops mutating or accumulating rows over DataFrames
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

    for node in ast.walk(tree):
        # Detect .iterrows() or .itertuples()
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            attr_name = node.func.attr
            if attr_name in ("iterrows", "itertuples"):
                alerts.append({
                    "code": "PYTHON_ITERROWS_ANTIPATTERN",
                    "severity": "critical",
                    "title": f"Anti-Pattern Crítico: Uso de `.{attr_name}()` en Pandas",
                    "message": (
                        f"Iterar fila por fila con `.{attr_name}()` instancia objetos Series/tuplas en el intérprete "
                        "de Python y es hasta 1,000x más lento que C/NumPy. Reemplázalo por operaciones vectorizadas "
                        "(`np.maximum`, `np.minimum`, `np.select`, `pd.merge_asof`)."
                    ),
                    "wikilink": "[[Vectorización ETL en Python]]",
                })
            elif attr_name == "apply":
                # Check if axis=1 or axis='columns' is passed
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
                            "`.apply(axis=1)` ejecuta un bucle de Python disfrazado sobre cada fila del DataFrame, "
                            "disparando el consumo de RAM y CPU. Usa aritmética de columnas vectorizada o `np.where` / `np.select`."
                        ),
                        "wikilink": "[[Vectorización ETL en Python]]",
                    })

    # Check if AVG/mean is used directly on sparse daily snapshots in Python
    if ".mean(" in py_code and "saldos" in py_code:
        alerts.append({
            "code": "PYTHON_MEAN_TRAP",
            "severity": "warning",
            "title": "Cuidado con `.mean()` sobre Snapshots Diarios Dispersos",
            "message": (
                "En Pandas, `df.groupby(...)[col].mean()` divide entre el conteo de filas presentes y no entre "
                "los días calendario del mes (30 o 31). Usa `.sum() / dias_mes`."
            ),
            "wikilink": "[[Trampa del AVG Bancario]]",
        })

    return alerts


def execute_python_etl(py_code: str, datasets_json: str) -> Dict[str, Any]:
    """
    Executes a user Python ETL script inside Pyodide with `tracemalloc` profiling.
    The user script must define `def transform_pasivos(datasets): -> pd.DataFrame` (or `result_df`).
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
                "Tu script debe definir la función `transform_pasivos(datasets)` que retorne un DataFrame de Pandas, "
                "o asignar el resultado final a la variable `result_df`."
            )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        if not isinstance(result_df, pd.DataFrame):
            raise TypeError("El resultado retornado no es un `pandas.DataFrame`.")

        # Normalize dates & floats to JSON-friendly records
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
