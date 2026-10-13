from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable
from typing import Any, ClassVar


class SodaDQProcessor:
    """
    Convert raw Soda scan results into a structured pivot.
    The processor extracts scan metadata, standard and dynamic metrics,
    schema diagnostics, freshness results, and table-scoped checks for
    downstream transformation and persistence.
    """

    # Fixed baseline metric columns
    REQUIRED_METRIC_COLUMNS: tuple[str, ...] = (
        "duplicate_count",
        "missing_count",
        "missing_percent",
        "invalid_count",
        "invalid_percent",
        "min",
        "max",
        "avg",
        "freshness",
        "schema",
    )
    # Mapping for meta fields -> SQL columns
    META_MAP: ClassVar[dict[str, int]] = {
        "definitionName": "definition_name",
        "dataTimestamp": "data_timestamp",
        "scanStartTimestamp": "scan_start_ts",
        "scanEndTimestamp": "scan_end_ts",
        "hasErrors": "has_errors",
        "hasWarnings": "has_warnings",
        "hasFailures": "has_failures",
        "tableName": "table_name",
        "row_count": "row_count",
    }
    TABLE_SCOPE_KEY = "__table__"
    ID_COL_RE = re.compile(r"(?:^id$|_id$)", re.IGNORECASE)
    _AS_SPLITTER = re.compile(r"\s+AS\s+", re.IGNORECASE)

    def save_scan_results(self, scan_results: dict | str) -> dict[str, Any]:
        """Convert Soda scan output into aligned column and value collections."""
        pivot = self.build_scan_pivot_dynamic(scan_results)
        extra_metrics = self._extra_metric_names_in_pivot(pivot)
        cols, values = self._insert_pivot_row(
            pivot=pivot, extra_metric_names=extra_metrics
        )
        return cols, values

    # Pivot builder
    @classmethod
    def build_scan_pivot_dynamic(cls, scan: dict | str) -> dict[str, Any]:
        """Build a metric pivot from a Soda scan dictionary or JSON string."""
        # Soda results may be supplied as a dictionary or serialized JSON.
        if isinstance(scan, (str, bytes)):
            scan = json.loads(scan)
        table_name = cls._first_table_from_scan(scan)
        out: dict[str, Any] = {
            "definitionName": scan.get("definitionName"),
            "dataTimestamp": scan.get("dataTimestamp"),
            "scanStartTimestamp": scan.get("scanStartTimestamp"),
            "scanEndTimestamp": scan.get("scanEndTimestamp"),
            "hasErrors": scan.get("hasErrors"),
            "hasWarnings": scan.get("hasWarnings"),
            "hasFailures": scan.get("hasFailures"),
            "tableName": table_name,
        }
        # Index metrics and collect helpers for enrichment
        # Index metrics once so checks can resolve metric IDs efficiently.
        metrics_by_id: dict[str, dict[str, Any]] = {}
        metric_value_by_type_and_col: dict[tuple[str, str | None], Any] = {}
        schema_metric_actual: dict[str, str] = {}
        for m in scan.get("metrics", []):
            mid = m.get("identity")
            if mid:
                metrics_by_id[mid] = m
            metric_name = m.get("metricName")
            val = m.get("value")
            col = (
                cls._metric_column_from_identity(mid, table_name, metric_name)
                if mid
                else None
            )
            metric_value_by_type_and_col[(metric_name, col)] = val
            if metric_name == "row_count":
                out["row_count"] = val
            if metric_name == "schema":
                for entry in val or []:
                    c = entry.get("columnName")
                    t = entry.get("sourceDataType")
                    if c:
                        schema_metric_actual[c] = t
        buckets: dict[str, dict[str, Any]] = defaultdict(dict)
        # Schema expectations & mismatches
        # Track expected and actual schema types for schema diagnostics.
        schema_expected: dict[str, str] = {}
        schema_mismatches: dict[str, dict[str, str]] = {}
        schema_check_outcome = "pass"
        for chk in scan.get("checks", []):
            outcome = chk.get("outcome")
            definition = chk.get("definition") or ""
            diag = chk.get("diagnostics") or {}
            col_from_check = chk.get("column")
            # Freshness checks: put in 'freshness' bucket
            if "freshness(" in definition.lower():
                m = re.search(
                    r"freshness\\(\s*([^)]+)\s*\\)", definition, re.IGNORECASE
                )
                mcol = (
                    (m.group(1).strip() if m else None)
                    or col_from_check
                    or cls.TABLE_SCOPE_KEY
                )
                buckets["freshness"][mcol] = {
                    "status": outcome,
                    "check_value": diag.get("value"),
                    "max_column_timestamp": diag.get("maxColumnTimestamp")
                    or diag.get("maxColumnTimestampUtc"),
                    "freshness": diag.get("freshness"),
                    "threshold": cls._freshness_threshold(definition),
                }
                continue
            # Schema check: capture the expectation list and mismatches
            if (chk.get("name") or "").lower().startswith("schema"):
                schema_check_outcome = outcome or "pass"
                schema_expected.update(cls._parse_schema_expected_types(definition))
                mism = diag.get("column_type_mismatches") or {}
                for c, d in mism.items():
                    schema_mismatches[c] = {
                        "expected_type": d.get("expected_type"),
                        "actual_type": d.get("actual_type"),
                    }
                continue
            # Generic metrics referenced by this check
            for mid in chk.get("metrics", []):
                m = metrics_by_id.get(mid)
                if not m:
                    continue
                check_type = m.get(
                    "metricName"
                )  # may include spaces, e.g., "failed rows"
                mval = m.get("value")
                # Extract or infer target column
                col = col_from_check or cls._metric_column_from_identity(
                    mid, table_name, check_type
                )
                # Special handling for custom "failed rows": infer from SQL / diagnostics
                if (
                    not col
                    and check_type
                    and check_type.strip().lower() == "failed rows"
                ):
                    inferred = cls._infer_column_from_failed_rows_check(chk)
                    if inferred:
                        col = inferred
                if not col:
                    # 'row_count' remains scalar, not a dict
                    if check_type == "row_count":
                        out["row_count"] = mval
                        continue
                    # Table-scoped: store under __table__
                    col = cls.TABLE_SCOPE_KEY
                buckets[check_type][col] = {
                    "status": outcome,
                    "check_value": mval,
                }
        # Enrich percent metrics with their count siblings
        # Attach count metrics to percentage checks for richer downstream results.
        for pct_key, cnt_key in [
            ("missing_percent", "missing_count"),
            ("invalid_percent", "invalid_count"),
        ]:
            if pct_key in buckets:
                for col, payload in list(buckets[pct_key].items()):
                    cnt_val = metric_value_by_type_and_col.get((cnt_key, col))
                    if cnt_val is not None:
                        payload[cnt_key] = cnt_val
        # Build the 'schema' bucket
        # Prefer configured schema columns; otherwise use the observed schema.
        schema_cols: Iterable[str] = (
            schema_expected.keys() if schema_expected else schema_metric_actual.keys()
        )
        schema_cols = list(schema_cols)
        if schema_cols:
            sb = buckets["schema"]
            for col in schema_cols:
                exp = schema_expected.get(
                    col
                )  # can be None when no explicit expected list
                if col in schema_mismatches:
                    mm = schema_mismatches[col]
                    sb[col] = {
                        "check_type": "schema_wrong_column_type",
                        "status": schema_check_outcome,
                        "expected_type": mm.get("expected_type") or exp,
                        "actual_type": mm.get("actual_type")
                        or schema_metric_actual.get(col),
                    }
                else:
                    entry = {
                        "status": "pass",
                        "actual_type": schema_metric_actual.get(col),
                    }
                    if exp:
                        entry["expected_type"] = exp
                    sb[col] = entry
        # Attach all buckets to output
        for check_type, data in buckets.items():
            if data:
                out[check_type] = data
        # Ensure the fixed metric keys exist (even if empty)
        # Keep a stable pivot shape even when a metric is absent from the scan.
        for key in cls.REQUIRED_METRIC_COLUMNS:
            out.setdefault(key, {})
        return out

    def _insert_pivot_row(
        self, pivot: dict[str, Any], extra_metric_names: Iterable[str]
    ) -> None:
        """Build ordered column names and values for one transformed scan record."""
        q = self._q
        col_names: list[str] = []
        placeholders: list[str] = []
        params: list[Any] = []
        # Meta columns
        for pkey, sqlcol in self.META_MAP.items():
            col_names.append(q(sqlcol))
            placeholders.append("%s")
            params.append(pivot.get(pkey))
        # Fixed metrics
        for mcol in self.REQUIRED_METRIC_COLUMNS:
            col_names.append(q(mcol))
            placeholders.append("JSON_PARSE(%s)")
            params.append(json.dumps(pivot.get(mcol, {})))
        # Dynamic metrics (present in pivot)
        fixed = set(self.REQUIRED_METRIC_COLUMNS)
        for name in extra_metric_names:
            if name in fixed:
                continue
            col_names.append(q(name))
            placeholders.append("JSON_PARSE(%s)")
            params.append(json.dumps(pivot.get(name, {})))
        return col_names, params

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def _q(name: str) -> str:
        """Safely quote an SQL identifier."""
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def _first_table_from_scan(scan: dict) -> str | None:
        """Return the first table name found in Soda checks or metrics."""
        # Prefer checks (they carry 'table')
        for chk in scan.get("checks", []):
            if chk.get("table"):
                return chk["table"]
        # Fallback to metrics' tableName
        for m in scan.get("metrics", []):
            tn = m.get("tableName")
            if tn:
                return tn
        return None

    @staticmethod
    def _metric_column_from_identity(
        metric_identity: str | None,
        table_name: str | None,
        metric_name: str | None,
    ) -> str | None:
        """
        Parse Soda metric identity:
          metric-\<def>-\<ds>-\<table>-\<column>-\<metricName>[...]
        Return None for table-scoped metrics (no column segment).
        """
        if not metric_identity or not table_name or not metric_name:
            return None
        id_l = metric_identity.lower()
        tbl_l = table_name.lower()
        needle = f"-{tbl_l}-"
        p = id_l.find(needle)
        if p == -1:
            return None
        tail_orig = metric_identity[p + len(needle) :]  # keep original case
        tail_l = id_l[p + len(needle) :]
        # if tail equals the metric name, it's table-scoped
        if tail_l == metric_name.lower():
            return None
        # find '-\<metricName' in the tail (handles optional suffix)
        idx = tail_l.find(f"-{metric_name.lower()}")
        if idx == -1:
            idx = tail_l.rfind(f"-{metric_name.lower()}")
            if idx == -1:
                return None
        col = tail_orig[:idx]
        return col or None

    @staticmethod
    def _freshness_threshold(definition: str) -> str | None:
        """
        Extract freshness threshold from SodaCL definition text.
        Supported forms:
          - inline:  "freshness(col) < 6h"
          - block:   "warn: when > 6h"  or  "fail: when >= 1d"
        Returns:
          "<6h", ">6h", ">=1d", etc., or None if not found.
        """
        text = definition or ""
        # 1) Inline comparator after freshness(...)
        m = re.search(
            r"freshness\\(\s*[^)]+\s*\\)\s*([<>]=?)\s*([^\s:]+)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return f"{m.group(1)}{m.group(2)}"
        # 2) Block rules: prefer WARN, else FAIL
        m_warn = re.search(
            r"warn\s*:\s*when\s*([<>]=?)\s*([^\s]+)", text, re.IGNORECASE
        )
        if m_warn:
            return f"{m_warn.group(1)}{m_warn.group(2)}"
        m_fail = re.search(
            r"fail\s*:\s*when\s*([<>]=?)\s*([^\s]+)", text, re.IGNORECASE
        )
        if m_fail:
            return f"{m_fail.group(1)}{m_fail.group(2)}"
        return None

    @staticmethod
    def _parse_schema_expected_types(definition: str) -> dict[str, str]:
        """
        Extract explicit list under:
          fail:
            when wrong column type:
              col_a: expected_type
              col_b: expected_type
        """
        if not definition:
            return {}
        m = re.search(
            r"when\s+wrong\s+column\s+type\s*:\s*(.+?)(?:\n\s*\n|$)",
            definition,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return {}
        block = m.group(1)
        pairs = re.findall(
            r"^\s*([A-Za-z0-9_]+)\s*:\s*([A-Za-z0-9 _]+)\s*$",
            block,
            flags=re.MULTILINE,
        )
        return {k: v for k, v in pairs}

    @staticmethod
    def _extract_yaml_block(definition: str, key: str) -> str | None:
        """
        Extract an indented YAML literal block under a key (e.g., 'fail query: |').
        Returns the block string (dedented) or None.
        """
        if not definition:
            return None
        m = re.search(
            rf"{re.escape(key)}\s*:\s*\\\\|\s*(.+?)(?=\n\s*\n|\Z)",
            definition,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return None
        block = m.group(1)
        lines = block.splitlines()
        if not lines:
            return None
        lead = min(
            (len(line) - len(line.lstrip(" ")) for line in lines if line.strip()),
            default=0,
        )
        cleaned = "\n".join(
            line[lead:] if len(line) >= lead else line for line in lines
        ).strip()
        return cleaned or None

    @classmethod
    def _infer_column_from_failed_rows_check(cls, chk: dict) -> str | None:
        """
        Infer a primary column for a 'failed rows' check:
          1\\) Parse WHERE clause from 'fail query' SQL (prefer first non-ID col).
          2\\) Else take first non-ID from SELECT list.
          3\\) Else use diagnostics.failedRowsAnalysis.file.columns first non-ID.
        """
        definition = chk.get("definition") or ""
        diagnostics = chk.get("diagnostics") or {}
        sql = cls._extract_yaml_block(definition, "fail query")
        if sql:
            # remove ${TABLE}
            sql_clean = re.sub(r"\\\\$\\\\{TABLE\\\\}", "", sql, flags=re.IGNORECASE)
            # 1) WHERE clause: e.g., loan_amount < 0
            where_match = re.search(
                r"\bWHERE\b(.+)", sql_clean, flags=re.IGNORECASE | re.DOTALL
            )
            if where_match:
                where_sql = where_match.group(1)
                where_cols = re.findall(
                    r"\b([A-Za-z_][A-Za-z0-9_]*)\b\s*(?:=|<>|!=|<|>|<=|>=|\bIN\b|\bLIKE\b)",
                    where_sql,
                    flags=re.IGNORECASE,
                )
                for c in where_cols:
                    if not cls.ID_COL_RE.search(c):
                        return c
            # 2) SELECT list
            sel_match = re.search(
                r"\bSELECT\b(.+?)\bFROM\b", sql_clean, flags=re.IGNORECASE | re.DOTALL
            )
            if sel_match:
                sel_list = sel_match.group(1)
                parts = [p.strip() for p in sel_list.split(",")]
                sel_cols = []
                for part in parts:
                    # split on AS (case-insensitive)
                    first = cls._AS_SPLITTER.split(part)[0].strip()
                    # remove table alias if present
                    if "." in first:
                        first = first.split(".")[-1].strip()
                    # keep bare identifiers
                    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", first):
                        sel_cols.append(first)
                for c in sel_cols:
                    if not cls.ID_COL_RE.search(c):
                        return c
        # 3) Diagnostics sample columns
        blocks = diagnostics.get("blocks") or []
        for b in blocks:
            if b.get("type") == "failedRowsAnalysis":
                file_info = b.get("file") or {}
                cols = file_info.get("columns") or []
                for c in cols:
                    name = c.get("name")
                    if name and not cls.ID_COL_RE.search(name):
                        return name
        return None

    @classmethod
    def _extra_metric_names_in_pivot(cls, pivot: dict[str, Any]) -> list[str]:
        """Return metric bucket names that are not part of the fixed metric set."""
        fixed = set(cls.REQUIRED_METRIC_COLUMNS)
        names = []
        for k, v in pivot.items():
            if k in fixed:
                continue
            if k in cls.META_MAP or k == "row_count":
                continue
            if isinstance(v, dict):
                names.append(k)
        return names
