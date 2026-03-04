#!/usr/bin/env python
# coding: utf-8
"""
社区筛选流水线：
1. 读取 candidates.csv
2. 地理编码（可选，缺经纬度时）
3. 周边检索 A/B/C 分类 POI
4. 聚合并按《量化与淘汰标准.md》打分/硬淘汰
5. 生成 report.csv 与 candidates.sqlite

默认直接调用高德 Web API；若本机有 mcprouter 的高德 MCP Server，可通过环境变量 AMAP_MODE=mcp 启用适配（见 AMapClient._call_mcp）。
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, cast

import requests

# 半径默认值（米）
DEFAULT_RADII = (200, 500, 600)

# 六维权重（与《量化与淘汰标准.md》一致）
DIM_WEIGHTS: Dict[str, float] = {
    "节点完整度": 0.15,
    "边界清晰": 0.15,
    "角色混合度": 0.15,
    "试点可行": 0.25,
    "可评估性": 0.20,
    "进入性": 0.10,
}

# A/B/C 分类与子类别映射；高德 typecode 可按需微调
CATEGORY_MAP: Dict[str, Dict[str, List[str]]] = {
    "A": {
        "express": ["060102", "0601"],  # 快递/自提/邮政
        "food": ["0500"],  # 餐饮密度
        "sanitation": ["190301"],  # 环卫/垃圾
        "parking_charging": ["1509", "991700"],  # 停车/充电
        "community_service": ["141200"],  # 社区/居委/党群
    },
    "B": {
        "school": ["1412", "1413"],  # 学校/幼儿园
        "park_office": ["1701"],  # 园区/办公
        "commerce": ["0604", "0602"],  # 商业综合体/商铺
        "residence_property": ["120201", "190207"],  # 住宅小区/物业公司
    },
    "C": {
        "gate": ["120201", "150900"],  # 小区/停车出入口（粗略用）
        "express_point": ["060102", "0601"],
        "trash_point": ["190301"],
        "parking": ["1509"],
    },
}


Candidate = Dict[str, Any]


def read_candidates(path: Path) -> List[Candidate]:
    rows: List[Candidate] = []
    # utf-8-sig 可自动吃掉首列 BOM，避免出现 '\ufeff名称' 这类键名
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cleaned: Candidate = {}
            for k, v in row.items():
                if k is None:
                    continue
                key = k.lstrip("\ufeff").strip()
                cleaned[key] = (v or "").strip()
            rows.append(cleaned)
    return rows


class AMapClient:
    """高德请求封装，支持 direct（默认）与 mcp 两种模式。"""

    def __init__(
        self,
        key: Optional[str],
        mode: str = "direct",
        mcp_base: Optional[str] = None,
        qps: int = 10,
    ):
        self.key = key or os.getenv("AMAP_KEY")
        self.mode = mode
        self.mcp_base = mcp_base or os.getenv("MCP_AMAP_BASE", "http://127.0.0.1:3001")
        self.min_interval = 1.0 / max(qps, 1)
        self._last = 0.0

    def _throttle(self):
        delta = time.time() - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.time()

    def geocode(self, name: str, city: str, address: str = "") -> Optional[Tuple[float, float]]:
        self._throttle()
        if self.mode == "mcp":
            data = self._call_mcp("geocode", {"address": address or name, "city": city})
            if not data:
                return None
            loc_val = data.get("location") if isinstance(data, dict) else data if isinstance(data, str) else None
            if not loc_val:
                return None
            try:
                lng, lat = loc_val.split(",")
                return float(lng), float(lat)
            except Exception:
                return None
        params = {"address": address or name, "city": city, "output": "json", "key": self.key}
        try:
            resp = requests.get("https://restapi.amap.com/v3/geocode/geo", params=params, timeout=8)
            data = resp.json()
        except Exception:
            return None
        if data.get("status") != "1" or not data.get("geocodes"):
            return None
        loc = data["geocodes"][0].get("location")
        if not loc:
            return None
        try:
            lng, lat = loc.split(",")
            return float(lng), float(lat)
        except Exception:
            return None

    def place_around(
        self, lng: float, lat: float, radius: int, typecodes: Sequence[str], page_size: int = 25
    ) -> List[Dict[str, Any]]:
        self._throttle()
        if self.mode == "mcp":
            result = self._call_mcp(
                "place/around",
                {"location": f"{lng},{lat}", "radius": radius, "types": "|".join(typecodes), "page_size": page_size},
                expect_list=True,
            )
            return cast(List[Dict[str, Any]], result or [])
        results: List[Dict[str, Any]] = []
        page = 1
        while True:
            params = {
                "location": f"{lng},{lat}",
                "radius": radius,
                "types": "|".join(typecodes),
                "page_size": page_size,
                "page_num": page,
                "output": "json",
                "key": self.key,
            }
            try:
                resp = requests.get("https://restapi.amap.com/v5/place/around", params=params, timeout=8)
                data = resp.json()
            except Exception:
                break
            if data.get("status") != "1":
                break
            pois = data.get("pois") or []
            results.extend(pois)
            if len(pois) < page_size:
                break
            page += 1
            if page > 3:  # 控制请求量
                break
        return results

    def _call_mcp(self, path: str, params: Dict[str, Any], expect_list: bool = False) -> Any:
        """MCP Router 适配占位：按需调整接口路径/字段。"""
        last_err: Optional[str] = None
        for _ in range(3):
            try:
                resp = requests.post(f"{self.mcp_base}/{path}", json=params, timeout=12)
                if resp.status_code != 200:
                    last_err = f"HTTP {resp.status_code}"
                    continue
                data: Dict[str, Any] = resp.json() if resp.content else {}
                break
            except requests.RequestException as e:
                last_err = str(e)
                time.sleep(0.5)
        else:
            print(f"[warn] MCP request failed {path}: {last_err}")
            return [] if expect_list else None
        if expect_list:
            return data.get("data") or []
        return data.get("data")


def collect_poi(
    client: AMapClient, lng: float, lat: float, radii: Tuple[int, int, int]
) -> Tuple[List[Dict[str, Any]], Dict[str, set]]:
    hits_by_key: Dict[Tuple[str, str, Any], Dict[str, Any]] = {}
    present: Dict[str, set] = {"A": set(), "B": set(), "C": set()}

    def should_keep_hit(group: str, poi: Dict[str, Any]) -> bool:
        # 高德社区服务类型中会混入学校类，需做温和过滤，避免误删治理主体入口
        if group != "community_service":
            return True
        name = str(poi.get("name") or "")
        governance_keywords = ("居委", "居委会", "党群", "服务中心", "服务站", "社区中心", "村委", "街道办")
        if any(k in name for k in governance_keywords):
            return True
        school_keywords = ("学校", "幼儿园", "学院", "中学", "小学", "大学", "教育", "培训")
        return not any(k in name for k in school_keywords)

    def should_replace(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
        old_radius = old.get("radius")
        new_radius = new.get("radius")
        if isinstance(old_radius, int) and isinstance(new_radius, int) and new_radius < old_radius:
            return True
        if old_radius == new_radius:
            old_dist = old.get("distance")
            new_dist = new.get("distance")
            if new_dist is not None and (old_dist is None or float(new_dist) < float(old_dist)):
                return True
        return False

    for cat, groups in CATEGORY_MAP.items():
        for group, typecodes in groups.items():
            for radius in radii:
                pois = client.place_around(lng, lat, radius, typecodes)
                for poi in pois:
                    if not should_keep_hit(group, poi):
                        continue
                    location = poi.get("location") or poi.get("entr_location") or ""
                    try:
                        lng_p, lat_p = [float(x) for x in location.split(",")]
                    except Exception:
                        lng_p, lat_p = (None, None)
                    dist_val = poi.get("distance")
                    dist = float(dist_val) if dist_val not in (None, "") else None
                    hit = {
                        "category": cat,
                        "group": group,
                        "radius": radius,
                        "id": poi.get("id"),
                        "name": poi.get("name"),
                        "typecode": poi.get("typecode"),
                        "lng": lng_p,
                        "lat": lat_p,
                        "distance": dist,
                    }
                    dedupe_id = poi.get("id") or (poi.get("name"), poi.get("typecode"), lng_p, lat_p)
                    dedupe_key = (cat, group, dedupe_id)
                    prev = hits_by_key.get(dedupe_key)
                    if prev is None or should_replace(prev, hit):
                        hits_by_key[dedupe_key] = hit
                    present[cat].add(group)
    hits = sorted(
        hits_by_key.values(),
        key=lambda x: (
            str(x.get("category", "")),
            str(x.get("group", "")),
            int(x.get("radius", 10**9)),
            float(x.get("distance")) if x.get("distance") is not None else 1e9,
        ),
    )
    return hits, present


def score_candidate(
    present: Dict[str, set],
    hits: List[Dict],
    radii: Tuple[int, int, int],
) -> Tuple[float, Dict[str, float], Dict[str, str], List[str]]:
    reasons: List[str] = []
    group_labels = {
        "express": "快递/自提",
        "food": "餐饮",
        "sanitation": "环卫/垃圾",
        "parking_charging": "停车/充电",
        "community_service": "社区/党群",
        "school": "学校/幼儿园",
        "park_office": "园区/办公",
        "commerce": "商业",
        "residence_property": "物业/小区",
        "gate": "门岗/出入口",
        "express_point": "快递点",
        "trash_point": "垃圾点",
        "parking": "停车口",
    }
    a_order = ["express", "food", "sanitation", "parking_charging", "community_service"]
    b_order = ["school", "park_office", "commerce", "residence_property"]
    c_order = ["gate", "express_point", "trash_point", "parking"]
    split_road_keywords = ("路", "大道", "高架", "快速路", "立交", "环路", "国道", "省道")
    split_market_keywords = ("超市", "商场", "商城", "购物中心", "大卖场", "百货", "MALL", "mall")

    def pick_examples(groups: Iterable[str]) -> List[str]:
        examples: List[str] = []
        seen: set = set()
        for g in groups:
            if g in seen:
                continue
            seen.add(g)
            candidates = [h for h in hits if h.get("group") == g and h.get("name")]
            if not candidates:
                continue
            candidates.sort(key=lambda x: x.get("distance") if x.get("distance") is not None else 1e9)
            sample = candidates[0]
            dist_val = sample.get("distance")
            dist_str = f"@{int(dist_val)}m" if dist_val is not None else ""
            examples.append(f"{group_labels.get(g, g)}: {sample.get('name', '')}{dist_str}")
        return examples

    def dedupe_hits(items: Iterable[Dict[str, Any]], by_group: bool = True) -> List[Dict[str, Any]]:
        uniq: List[Dict[str, Any]] = []
        seen: set = set()
        for h in items:
            if by_group:
                # 去重时保留 group 维度，避免跨组同 id 误吞命中（如 school 被 community_service 覆盖）
                key = (h.get("group"), h.get("id")) if h.get("id") else (
                    h.get("group"),
                    h.get("name"),
                    h.get("typecode"),
                    h.get("lng"),
                    h.get("lat"),
                )
            else:
                # 按 POI 实体去重，避免同一地点跨子类重复计数
                key = h.get("id") or (
                    h.get("name"),
                    h.get("typecode"),
                    h.get("lng"),
                    h.get("lat"),
                )
            if key in seen:
                continue
            seen.add(key)
            uniq.append(h)
        return uniq

    def pick_hit_examples(items: Iterable[Dict[str, Any]], label: str, limit: int = 2) -> List[str]:
        rows = sorted(
            [h for h in items if h.get("name")],
            key=lambda x: x.get("distance") if x.get("distance") is not None else 1e9,
        )[:limit]
        out: List[str] = []
        for h in rows:
            d = h.get("distance")
            d_str = f"@{int(d)}m" if d is not None else ""
            out.append(f"{label}: {h.get('name', '')}{d_str}")
        return out

    score_nodes = 1 + min(len(present["A"]), 4)
    score_mix = 1 + min(len(present["B"]), 4)
    core_hits = [h for h in hits if h["radius"] <= radii[0]]
    c_core = [h for h in core_hits if h["category"] == "C"]
    c_core_unique = dedupe_hits(c_core, by_group=False)
    score_pilot = 1 if not c_core_unique else min(5, 2 + len(c_core_unique))
    # 可评估性按需求不再计算，统一按满分 5 分计
    score_eval = 5
    entry_clue = [h for h in hits if h["group"] in ("community_service", "residence_property")]
    score_entry = 1 if not entry_clue else min(5, 3 + min(len(entry_clue), 2))
    road_split_hits = dedupe_hits(
        [
            h
            for h in core_hits
            if str(h.get("typecode", "")).startswith("1903")
            or any(k in str(h.get("name", "")) for k in split_road_keywords)
        ],
        by_group=False,
    )
    market_split_hits = dedupe_hits(
        [h for h in core_hits if h.get("group") == "commerce" and any(k in str(h.get("name", "")) for k in split_market_keywords)],
        by_group=False,
    )
    school_split_hits = dedupe_hits([h for h in core_hits if h.get("group") == "school"], by_group=False)
    split_unique_hits = dedupe_hits([*road_split_hits, *market_split_hits, *school_split_hits], by_group=False)
    split_count = len(split_unique_hits)
    # 边界清晰采用满分 5 分减分制：核心半径内每个“切割线索”扣 1 分，最低 1 分
    score_boundary = max(1, 5 - min(split_count, 4))

    dims: Dict[str, float] = {
        "节点完整度": score_nodes,
        "边界清晰": score_boundary,
        "角色混合度": score_mix,
        "试点可行": score_pilot,
        "可评估性": score_eval,
        "进入性": score_entry,
    }
    weighted_total = sum(dims[k] * DIM_WEIGHTS[k] for k in DIM_WEIGHTS)

    if len(present["A"]) == 0:
        reasons.append("A 类高频节点缺失（快递/餐饮/环卫/停车/社区服务均未命中）")
    if score_boundary <= 1:
        reasons.append("边界极不清晰")
    if score_entry == 1:
        reasons.append("进入性线索缺失（物业/居委未命中）")

    final_total = 0 if reasons else weighted_total

    a_examples = pick_examples([g for g in a_order if g in present["A"]])
    b_examples = pick_examples([g for g in b_order if g in present["B"]])
    c_examples = pick_examples([g for g in c_order if any(h["group"] == g for h in c_core_unique)])
    boundary_examples = (
        pick_hit_examples(road_split_hits, "主干道", limit=2)
        + pick_hit_examples(market_split_hits, "商超", limit=1)
        + pick_hit_examples(school_split_hits, "学校", limit=1)
    )
    entry_examples = pick_examples([g for g in ("community_service", "residence_property") if any(h["group"] == g for h in entry_clue)])

    details = {
        "节点完整度": f"A类命中 {len(present['A'])} 种，得分 {score_nodes}/5；命中示例：{'; '.join(a_examples) if a_examples else '暂无'}",
        "边界清晰": (
            f"核心半径切割线索 {split_count} 个（主干道 {len(road_split_hits)} / 商超 {len(market_split_hits)} / 学校 {len(school_split_hits)}），"
            f"得分 {score_boundary}/5（5分减分制）；命中示例：{'; '.join(boundary_examples) if boundary_examples else '暂无'}"
        ),
        "角色混合度": f"B类命中 {len(present['B'])} 种，得分 {score_mix}/5；命中示例：{'; '.join(b_examples) if b_examples else '暂无'}",
        "试点可行": f"C类核心半径内唯一命中 {len(c_core_unique)} 个，得分 {score_pilot}/5；命中示例：{'; '.join(c_examples) if c_examples else '暂无'}",
        "可评估性": "按规则固定满分 5/5（不再参与命中统计）",
        "进入性": f"物业/居委线索 {len(entry_clue)} 条，得分 {score_entry}/5；命中示例：{'; '.join(entry_examples) if entry_examples else '暂无'}",
    }

    return final_total, dims, details, reasons


def ensure_schema(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS targets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            city TEXT,
            district TEXT,
            address TEXT,
            lng REAL,
            lat REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS poi_hits(
            target_id INTEGER,
            category TEXT,
            subgroup TEXT,
            radius INTEGER,
            poi_id TEXT,
            name TEXT,
            typecode TEXT,
            lng REAL,
            lat REAL,
            distance REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scores(
            target_id INTEGER,
            dimension TEXT,
            score REAL,
            detail TEXT
        )
        """
    )


def persist(
    db_path: Path,
    rows: List[Candidate],
    all_hits: Dict[str, List[Dict[str, Any]]],
    all_scores: Dict[str, Tuple[float, Dict[str, float], Dict[str, str], List[str]]],
):
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)
    conn.execute("DELETE FROM targets")
    conn.execute("DELETE FROM poi_hits")
    conn.execute("DELETE FROM scores")
    name_to_id: Dict[str, int] = {}
    for row in rows:
        cur = conn.execute(
            "INSERT INTO targets(name, city, district, address, lng, lat) VALUES(?,?,?,?,?,?)",
            (
                str(row.get("名称", "")),
                str(row.get("城市", "")),
                str(row.get("区县", "")),
                str(row.get("地址", "")),
                row.get("经度"),
                row.get("纬度"),
            ),
        )
        name_key = str(row.get("_row_key", ""))
        if cur.lastrowid is not None:
            name_to_id[name_key] = int(cur.lastrowid)
    for row_key, hits in all_hits.items():
        tid = name_to_id.get(row_key)
        if not tid:
            continue
        conn.executemany(
            "INSERT INTO poi_hits VALUES(?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    tid,
                    h["category"],
                    h["group"],
                    h["radius"],
                    h.get("id"),
                    h.get("name"),
                    h.get("typecode"),
                    h.get("lng"),
                    h.get("lat"),
                    h.get("distance"),
                )
                for h in hits
            ],
        )
    for row_key, score_pack in all_scores.items():
        total, dims, details, reasons = score_pack
        tid = name_to_id.get(row_key)
        if not tid:
            continue
        rows_to_insert = [(tid, k, v, details.get(k, "")) for k, v in dims.items()]
        # 若因地理编码等失败导致无维度分，也写入一条错误记录便于前端查看
        if not rows_to_insert and reasons:
            rows_to_insert = [(tid, "错误", 0, ";".join(reasons))]
        if rows_to_insert:
            conn.executemany("INSERT INTO scores VALUES(?,?,?,?)", rows_to_insert)
        # 记录总分，便于前端直接取用且避免重复累加
        conn.execute(
            "INSERT INTO scores VALUES(?,?,?,?)",
            (tid, "TOTAL", round(total, 3), ";".join(reasons) if reasons else ""),
        )
    conn.commit()
    conn.close()


def write_report(path: Path, rows: List[Candidate], all_scores: Dict[str, Tuple[float, Dict[str, float], Dict[str, str], List[str]]]):
    header = [
        "名称",
        "城市",
        "区县",
        "经度",
        "纬度",
        "总分",
        "淘汰标记",
        "淘汰原因",
        "节点完整度",
        "边界清晰",
        "角色混合度",
        "试点可行",
        "可评估性",
        "进入性",
        "示例POI",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in rows:
            name = str(row.get("名称", "")).strip()
            row_key = str(row.get("_row_key", ""))
            total, dims, details, reasons = all_scores.get(row_key, (0, {}, {}, []))
            eliminated = "是" if reasons else "否"
            sample_hits = row.get("_sample_hits", [])
            sample_str = "；".join(sample_hits) if sample_hits else ""
            writer.writerow(
                [
                    name,
                    row.get("城市"),
                    row.get("区县"),
                    row.get("经度"),
                    row.get("纬度"),
                    round(total, 3),
                    eliminated,
                    "；".join(reasons),
                    dims.get("节点完整度", 0),
                    dims.get("边界清晰", 0),
                    dims.get("角色混合度", 0),
                    dims.get("试点可行", 0),
                    dims.get("可评估性", 0),
                    dims.get("进入性", 0),
                    sample_str,
                ]
            )


def main():
    parser = argparse.ArgumentParser(description="社区筛选自动化流水线")
    parser.add_argument("--input", default="candidates.csv", help="候选点 CSV 路径")
    parser.add_argument("--db", default="candidates.sqlite", help="输出 SQLite 路径")
    parser.add_argument("--report", default="report.csv", help="输出报告 CSV")
    parser.add_argument("--mode", default=os.getenv("AMAP_MODE", "direct"), choices=["direct", "mcp"])
    parser.add_argument("--mcp-base", default=os.getenv("MCP_AMAP_BASE", "http://127.0.0.1:3001"))
    parser.add_argument("--qps", type=int, default=10)
    parser.add_argument("--core-radius", type=int, default=DEFAULT_RADII[0])
    parser.add_argument("--ext-radius", type=int, default=DEFAULT_RADII[1])
    parser.add_argument("--obs-radius", type=int, default=DEFAULT_RADII[2])
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"未找到候选文件: {input_path}")

    rows = read_candidates(input_path)
    if not rows:
        raise SystemExit("候选列表为空")

    client = AMapClient(key=os.getenv("AMAP_KEY"), mode=args.mode, mcp_base=args.mcp_base, qps=args.qps)
    radii = (args.core_radius, args.ext_radius, args.obs_radius)

    # 为每行生成唯一键，避免同名地点数据混写
    for idx, row in enumerate(rows):
        row["_row_key"] = f"{row.get('名称','')}_{idx}"

    def pick_radii(row: Candidate, defaults: Tuple[int, int, int]) -> Tuple[int, int, int]:
        r_core = row.get("半径_核心米")
        r_ext = row.get("半径_扩展米")
        r_obs = row.get("半径_观察米")
        try:
            return (
                int(r_core) if r_core else defaults[0],
                int(r_ext) if r_ext else defaults[1],
                int(r_obs) if r_obs else defaults[2],
            )
        except Exception:
            return defaults

    all_hits: Dict[str, List[Dict[str, Any]]] = {}
    all_scores: Dict[str, Tuple[float, Dict[str, float], Dict[str, str], List[str]]] = {}

    for row in rows:
        row_key = str(row.get("_row_key", ""))
        name = str(row.get("名称", ""))
        city = str(row.get("城市", ""))
        address = str(row.get("地址", ""))
        lng = row.get("经度")
        lat = row.get("纬度")
        if lng and lat:
            lng_f, lat_f = float(lng), float(lat)
        else:
            geo = client.geocode(name, city, address)
            if not geo:
                all_scores[row_key] = (0, {}, {}, ["地理编码失败"])
                continue
            lng_f, lat_f = geo
            row["经度"], row["纬度"] = lng_f, lat_f

        row_radii = pick_radii(row, radii)
        hits, present = collect_poi(client, lng_f, lat_f, row_radii)
        # 报表示例：取距离最近的 3 个“唯一 POI”，避免跨 group 同一地点重复展示
        sample_hits: List[str] = []
        seen_sample_keys: set = set()
        for h in sorted(hits, key=lambda x: x.get("distance") or 1e9):
            if not h.get("name"):
                continue
            sample_key = h.get("id") or (h.get("name"), h.get("typecode"), h.get("lng"), h.get("lat"))
            if sample_key in seen_sample_keys:
                continue
            seen_sample_keys.add(sample_key)
            sample_hits.append(f"{h.get('name','')}@{h.get('distance','')}m")
            if len(sample_hits) >= 3:
                break
        row["_sample_hits"] = sample_hits
        all_hits[row_key] = hits
        total, dims, details, reasons = score_candidate(present, hits, row_radii)
        all_scores[row_key] = (total, dims, details, reasons)

    persist(Path(args.db), rows, all_hits, all_scores)
    write_report(Path(args.report), rows, all_scores)
    print(f"完成。报告: {args.report}，数据库: {args.db}")


if __name__ == "__main__":
    main()
