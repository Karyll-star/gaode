#!/usr/bin/env python
"""
Lightweight SQLite web viewer for candidates.sqlite.

Usage:
  python sqlite_web_viewer.py --db candidates.sqlite --host 127.0.0.1 --port 8000
Then open:
  http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, urlparse


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SQLite Viewer</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0f172a;
      --card: rgba(255,255,255,0.08);
      --card-border: rgba(255,255,255,0.12);
      --text: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --accent-2: #a78bfa;
      color-scheme: dark;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      background: radial-gradient(circle at 20% 20%, rgba(56, 189, 248, 0.18), transparent 30%),
                  radial-gradient(circle at 80% 0%, rgba(167, 139, 250, 0.20), transparent 28%),
                  var(--bg);
      color: var(--text);
    }
    .shell { max-width: 1200px; margin: 0 auto; padding: 28px 20px 44px; }
    header { display: flex; align-items: baseline; gap: 12px; margin-bottom: 16px; }
    h1 { margin: 0; font-size: 28px; letter-spacing: 0.2px; }
    .subtitle { color: var(--muted); font-size: 14px; }
    .panel {
      background: var(--card);
      border: 1px solid var(--card-border);
      border-radius: 14px;
      padding: 16px;
      backdrop-filter: blur(6px);
      box-shadow: 0 18px 40px rgba(0,0,0,0.25);
    }
    .controls {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 12px;
      align-items: center;
      margin-bottom: 14px;
    }
    label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.6px; color: var(--muted); }
    select, button, input {
      width: 100%;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid var(--card-border);
      background: rgba(255,255,255,0.05);
      color: var(--text);
      font-size: 14px;
    }
    button {
      cursor: pointer;
      font-weight: 600;
      transition: transform 0.12s ease, box-shadow 0.12s ease, background 0.12s ease;
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      border: none;
      box-shadow: 0 10px 25px rgba(56, 189, 248, 0.25);
    }
    button:hover { transform: translateY(-1px); box-shadow: 0 14px 30px rgba(56, 189, 248, 0.30); }
    .meta {
      display: flex; gap: 10px; align-items: center; flex-wrap: wrap;
      color: var(--muted); font-size: 13px; margin-top: 4px;
    }
    .rank-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .rank-card {
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid var(--card-border);
      background: rgba(255,255,255,0.06);
      box-shadow: 0 12px 28px rgba(0,0,0,0.2);
      cursor: pointer;
      transition: transform 0.12s ease, border-color 0.12s ease, background 0.12s ease;
    }
    .rank-card:hover { transform: translateY(-2px); border-color: var(--accent); }
    .rank-card.active { border-color: var(--accent); background: rgba(56,189,248,0.12); }
    .rank-title { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 6px; }
    .rank-name { font-weight: 600; }
    .rank-score { font-size: 24px; font-weight: 700; color: var(--accent); }
    .rank-meta { color: var(--muted); font-size: 12px; display: flex; gap: 8px; flex-wrap: wrap; }
    .pill {
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid var(--card-border);
      background: rgba(255,255,255,0.06);
      color: var(--text);
      font-size: 13px;
    }
    .table-wrap {
      margin-top: 12px;
      border: 1px solid var(--card-border);
      border-radius: 14px;
      overflow: auto;
      max-height: 70vh;
      background: rgba(15,23,42,0.6);
    }
    table { border-collapse: collapse; width: 100%; min-width: 760px; }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      text-align: left;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    th {
      position: sticky; top: 0; z-index: 2;
      background: rgba(15,23,42,0.92);
      font-size: 13px; letter-spacing: 0.2px; color: var(--muted);
    }
    tbody tr:nth-child(even) { background: rgba(255,255,255,0.03); }
    tbody tr:hover { background: rgba(56, 189, 248, 0.08); }
    .side {
      display: grid;
      gap: 8px;
    }
    .table-list {
      display: grid;
      gap: 6px;
      max-height: 260px;
      overflow: auto;
    }
    .table-item {
      display: flex; justify-content: space-between; align-items: center;
      padding: 10px 12px; border-radius: 10px;
      border: 1px solid var(--card-border);
      background: rgba(255,255,255,0.04);
      cursor: pointer;
      transition: transform 0.1s ease, border-color 0.1s ease, background 0.1s ease;
    }
    .table-item.active {
      border-color: var(--accent);
      background: rgba(56, 189, 248, 0.12);
    }
    .table-item:hover { transform: translateY(-1px); }
    .count { color: var(--muted); font-size: 12px; }
    .actions { display: grid; grid-template-columns: repeat(auto-fit,minmax(120px,1fr)); gap: 10px; margin-top: 6px; }
    .search {
      display: grid; gap: 6px;
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <h1>SQLite Viewer</h1>
      <span class="subtitle">清晰标注 · 现代呈现 · 数据不丢失</span>
    </header>

    <div class="panel">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <div style="font-size:14px;color:var(--muted);text-transform:uppercase;letter-spacing:0.6px;">目标概览 / 排名</div>
        <div style="font-size:12px;color:var(--muted);" id="summaryMeta"></div>
      </div>
      <div class="rank-grid" id="rankGrid"></div>
    </div>

    <div class="controls panel">
      <div class="side">
        <label>数据表</label>
        <div class="table-list" id="tableList"></div>
      </div>
      <div class="search">
        <div>
          <label for="tableSelect">表 (快速切换备用)</label>
          <select id="tableSelect"></select>
        </div>
        <div>
          <label for="pageSize">每页行数</label>
          <input id="pageSize" type="number" value="50" min="1" max="200" />
        </div>
        <div>
          <label for="filterInput">当前页快速筛选</label>
          <input id="filterInput" type="search" placeholder="输入关键字过滤当前页..." />
        </div>
        <div class="actions">
          <button id="refreshBtn">刷新</button>
          <button id="prevBtn">上一页</button>
          <button id="nextBtn">下一页</button>
          <button id="csvBtn">导出当前页 CSV</button>
        </div>
        <div class="meta">
          <span class="pill" id="meta"></span>
          <span class="pill" id="tableCount"></span>
        </div>
      </div>
    </div>

    <div class="panel table-wrap">
      <table id="dataTable">
        <thead></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <script>
    let currentPage = 1;
    let totalPages = 1;
    let lastColumns = [];
    let lastRows = [];
    let selectedTargetId = null;

    const friendly = { targets: "候选点", poi_hits: "POI命中", scores: "评分" };
    const tableSelect = document.getElementById("tableSelect");
    const tableList = document.getElementById("tableList");
    const pageSizeInput = document.getElementById("pageSize");
    const meta = document.getElementById("meta");
    const tableCount = document.getElementById("tableCount");
    const filterInput = document.getElementById("filterInput");
    const thead = document.querySelector("#dataTable thead");
    const tbody = document.querySelector("#dataTable tbody");
    const rankGrid = document.getElementById("rankGrid");
    const summaryMeta = document.getElementById("summaryMeta");

    async function fetchJson(url) {
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error("HTTP " + res.status);
      }
      return await res.json();
    }

    function renderTable(columns, rows) {
      lastColumns = columns;
      lastRows = rows;
      thead.innerHTML = "";
      tbody.innerHTML = "";

      const trHead = document.createElement("tr");
      for (const col of columns) {
        const th = document.createElement("th");
        th.textContent = col;
        trHead.appendChild(th);
      }
      thead.appendChild(trHead);

      for (const row of rows) {
        const tr = document.createElement("tr");
        for (const col of columns) {
          const td = document.createElement("td");
          const val = row[col];
          td.textContent = val === null ? "" : String(val);
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
      applyFilter();
    }

    function applyFilter() {
      const keyword = filterInput.value.trim().toLowerCase();
      const trs = tbody.querySelectorAll("tr");
      if (!keyword) {
        trs.forEach(tr => tr.style.display = "");
        meta.textContent = meta.dataset.baseText || meta.textContent;
        return;
      }
      let kept = 0;
      trs.forEach(tr => {
        const text = tr.textContent.toLowerCase();
        const match = text.includes(keyword);
        tr.style.display = match ? "" : "none";
        if (match) kept += 1;
      });
      meta.textContent = `${meta.dataset.baseText} | 当前页匹配 ${kept} 行`;
    }

    function label(name, count) {
      const zh = friendly[name] || name;
      return `${zh} (${count})`;
    }

    function renderRank(items) {
      rankGrid.innerHTML = "";
      if (!items || !items.length) {
        summaryMeta.textContent = "暂无数据";
        return;
      }
      summaryMeta.textContent = `共 ${items.length} 个目标`;
      items.forEach((item, idx) => {
        const div = document.createElement("div");
        div.className = "rank-card";
        div.dataset.tid = item.id;
        if (Number(selectedTargetId) === Number(item.id)) div.classList.add("active");
        div.innerHTML = `
          <div class="rank-title">
            <span class="rank-name">${idx + 1}. ${item.name || "(未命名)"}</span>
            <span class="rank-score">${Number(item.total || 0).toFixed(2)}</span>
          </div>
          <div class="rank-meta">
            <span>${item.city || ""} ${item.district || ""}</span>
            <span>POI: ${item.poi_count || 0}</span>
          </div>
        `;
        div.addEventListener("click", async () => {
          selectedTargetId = item.id;
          // 若当前表支持 target_id（scores / poi_hits），则保持当前表；否则切到 scores
          const current = tableSelect.value;
          const supportsTarget = current === "scores" || current === "poi_hits";
          const targetTable = supportsTarget ? current : "scores";
          tableSelect.value = targetTable;
          setActive(targetTable);
          currentPage = 1;
          await loadTable();
          renderRank(items); // refresh active state
        });
        rankGrid.appendChild(div);
      });
    }

    function renderTableList(tables) {
      tableList.innerHTML = "";
      tables.forEach((item, idx) => {
        const div = document.createElement("div");
        div.className = "table-item";
        div.dataset.name = item.name;
        div.innerHTML = `<span>${friendly[item.name] || item.name}</span><span class="count">${item.count} 行</span>`;
        if (idx === 0) div.classList.add("active");
        div.addEventListener("click", async () => {
          tableSelect.value = item.name;
          setActive(item.name);
          currentPage = 1;
          await loadTable();
        });
        tableList.appendChild(div);
      });
      tableCount.textContent = `表数量：${tables.length}`;
    }

    function setActive(name) {
      document.querySelectorAll(".table-item").forEach(el => {
        el.classList.toggle("active", el.dataset.name === name);
      });
    }

    async function loadTables() {
      const [data, summary] = await Promise.all([
        fetchJson("/api/tables"),
        fetchJson("/api/summary").catch(() => ({ items: [] })),
      ]);
      tableSelect.innerHTML = "";
      data.tables.forEach((item, idx) => {
        const opt = document.createElement("option");
        opt.value = item.name;
        opt.textContent = label(item.name, item.count);
        tableSelect.appendChild(opt);
        if (idx === 0) {
          tableSelect.value = item.name;
        }
      });
      renderTableList(data.tables);
      renderRank(summary.items || []);
    }

    async function loadTable() {
      const table = tableSelect.value;
      if (!table) return;
      setActive(table);
      const pageSize = Math.max(1, Math.min(200, Number(pageSizeInput.value || "50")));
      const targetParam = selectedTargetId ? `&target_id=${selectedTargetId}` : "";
      const data = await fetchJson(`/api/table?name=${encodeURIComponent(table)}&page=${currentPage}&page_size=${pageSize}${targetParam}`);
      totalPages = Math.max(1, Math.ceil(data.total / data.page_size));
      currentPage = data.page;
      renderTable(data.columns, data.rows);
      const tgt = selectedTargetId ? ` | 目标ID: ${selectedTargetId}` : "";
      const base = `表: ${table} | 行: ${data.total} | 页: ${currentPage}/${totalPages}${tgt}`;
      meta.dataset.baseText = base;
      meta.textContent = base;
    }

    async function refreshAll() {
      await loadTables();
      currentPage = 1;
      await loadTable();
    }

    function exportCsv() {
      if (!lastColumns.length) return;
      const rows = [lastColumns.join(",")].concat(
        lastRows.map(r => lastColumns.map(c => {
          const val = r[c];
          const s = val === null || val === undefined ? "" : String(val);
          // 简单转义逗号与引号
          const escaped = s.replace(/\"/g,'\"\"');
          return /[\",\\n]/.test(escaped) ? `"${escaped}"` : escaped;
        }).join(","))
      ).join("\\n");
      const blob = new Blob([rows], {type: "text/csv;charset=utf-8;"});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${tableSelect.value || "table"}_page${currentPage}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    }

    document.getElementById("refreshBtn").addEventListener("click", async () => {
      currentPage = 1;
      await refreshAll();
    });

    document.getElementById("prevBtn").addEventListener("click", async () => {
      if (currentPage > 1) {
        currentPage -= 1;
        await loadTable();
      }
    });

    document.getElementById("nextBtn").addEventListener("click", async () => {
      if (currentPage < totalPages) {
        currentPage += 1;
        await loadTable();
      }
    });

    document.getElementById("csvBtn").addEventListener("click", exportCsv);

    tableSelect.addEventListener("change", async () => {
      currentPage = 1;
      await loadTable();
    });

    filterInput.addEventListener("input", applyFilter);

    refreshAll().catch((err) => {
      meta.textContent = "Error: " + err.message;
    });
  </script>
</body>
</html>
"""


class SQLiteViewerHandler(BaseHTTPRequestHandler):
    db_path: Path

    def _write_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _write_html(self, html: str, status: int = 200) -> None:
        data = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _list_tables(self, conn: sqlite3.Connection) -> List[Tuple[str, int]]:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        out: List[Tuple[str, int]] = []
        for (name,) in rows:
            count = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            out.append((name, int(count)))
        return out

    def _summary(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            t.id,
            t.name,
            t.city,
            t.district,
            t.address,
            t.lng,
            t.lat,
            COALESCE(
                (SELECT s.score FROM scores s WHERE s.target_id = t.id AND s.dimension = 'TOTAL' LIMIT 1),
                0
            ) AS total,
            COUNT(DISTINCT p.poi_id) AS poi_count
        FROM targets t
        LEFT JOIN poi_hits p ON p.target_id = t.id
        GROUP BY t.id
        ORDER BY total DESC, t.id ASC
        """
        rows = conn.execute(sql).fetchall()
        return [
            {
                "id": r[0],
                "name": r[1],
                "city": r[2],
                "district": r[3],
                "address": r[4],
                "lng": r[5],
                "lat": r[6],
                "total": r[7],
                "poi_count": r[8],
            }
            for r in rows
        ]

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._write_html(HTML_PAGE)
            return

        if parsed.path == "/api/tables":
            try:
                with sqlite3.connect(self.db_path) as conn:
                    data = self._list_tables(conn)
                self._write_json({"tables": [{"name": n, "count": c} for n, c in data]})
            except Exception as exc:
                self._write_json({"error": str(exc)}, status=500)
            return

        if parsed.path == "/api/summary":
            try:
                with sqlite3.connect(self.db_path) as conn:
                    data = self._summary(conn)
                self._write_json({"items": data})
            except Exception as exc:
                self._write_json({"error": str(exc)}, status=500)
            return

        if parsed.path == "/api/table":
            qs = parse_qs(parsed.query)
            name = (qs.get("name") or [""])[0]
            page = max(1, int((qs.get("page") or ["1"])[0]))
            page_size = max(1, min(200, int((qs.get("page_size") or ["50"])[0])))
            target_id_val = qs.get("target_id", [None])[0]
            try:
                with sqlite3.connect(self.db_path) as conn:
                    tables = {t[0] for t in self._list_tables(conn)}
                    if name not in tables:
                        self._write_json({"error": "unknown table"}, status=400)
                        return

                    columns = [r[1] for r in conn.execute(f'PRAGMA table_info("{name}")').fetchall()]
                    where_clause = ""
                    params: Tuple[Any, ...] = ()
                    if target_id_val is not None and "target_id" in columns:
                        where_clause = " WHERE target_id = ? "
                        params = (int(target_id_val),)
                    total = int(conn.execute(f'SELECT COUNT(*) FROM "{name}"{where_clause}', params).fetchone()[0])
                    offset = (page - 1) * page_size
                    rows = conn.execute(
                        f'SELECT * FROM "{name}"{where_clause} LIMIT ? OFFSET ?',
                        params + (page_size, offset),
                    ).fetchall()
                    dict_rows = [dict(zip(columns, row)) for row in rows]
                self._write_json(
                    {
                        "name": name,
                        "columns": columns,
                        "total": total,
                        "page": page,
                        "page_size": page_size,
                        "rows": dict_rows,
                    }
                )
            except Exception as exc:
                self._write_json({"error": str(exc)}, status=500)
            return

        self._write_json({"error": "not found"}, status=404)


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple web viewer for SQLite")
    parser.add_argument("--db", default="candidates.sqlite", help="Path to SQLite DB")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    if not db_path.exists():
        raise SystemExit(f"DB file not found: {db_path}")

    SQLiteViewerHandler.db_path = db_path
    server = ThreadingHTTPServer((args.host, args.port), SQLiteViewerHandler)
    print(f"SQLite viewer running at http://{args.host}:{args.port}")
    print(f"Using DB: {db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
