# 社区筛选工具

批量评估候选社区周边的可用设施与运营可行性，自动生成评分报表（CSV）和可浏览的 SQLite 数据库，支持高德地图直连或通过 mcprouter 的 MCP Server 调用。

## 目录
- `pipeline.py`：主流程脚本，完成地理编码、周边 POI 拉取、六维评分、淘汰原因、报表/数据库落盘。
- `sqlite_web_viewer.py`：本地 SQLite Web 查看器，便于排序/筛选/导出。
- `candidates.csv`：示例候选清单。
- `report.csv`：示例输出报表（含分数与淘汰标记）。
- 文档：`初始化方案.md`、`筛选目标.md`、`量化与淘汰标准.md`。

## 环境与依赖
- Python 3.9+
- 安装依赖：`pip install -r requirements.txt`
- 环境变量：
  - `AMAP_KEY`：高德 Web API Key（直连模式必填）。
  - `AMAP_MODE`（可选）：`direct`（默认）或 `mcp`。
  - `MCP_AMAP_BASE`（可选）：MCP Server 地址，默认 `http://127.0.0.1:3001`。

## 输入格式：`candidates.csv`
UTF-8 或 UTF-8-BOM 均可，示例见仓库。字段：
- `名称`（必填）
- `城市`、`区县`（必填，用于地理编码）
- `地址`（可选，优先用于地理编码）
- `经度`、`纬度`（可选，若给定则跳过地理编码）
- `半径_核心米`、`半径_扩展米`、`备注`（可选，半径可覆盖默认值）

## 快速开始
1) 准备 Key：`export AMAP_KEY=你的高德Key`（Windows 用 `set`）。
2) 运行评估：
```bash
python pipeline.py --input candidates.csv --db candidates.sqlite --report report.csv --mode direct
```
常用参数：
- `--mode direct|mcp`：直连或经 MCP Server。
- `--mcp-base`：MCP Server 地址。
- `--qps`：请求节流，默认 10。
- `--core-radius` / `--ext-radius` / `--obs-radius`：默认 200 / 300 / 450（米），可被 CSV 中对应列覆盖。

## 输出
- `report.csv`：列包含名称、城市/区县、经纬度、总分、淘汰标记、淘汰原因、六个维度分数、示例 POI。
- `candidates.sqlite`：表结构
  - `targets`：候选点及坐标
  - `poi_hits`：命中 POI 明细（分类、半径、距离等）
  - `scores`：维度分数与总分（维度名 `TOTAL`）

## 查看数据（Web）
```bash
python sqlite_web_viewer.py --db candidates.sqlite --host 127.0.0.1 --port 8000
```
浏览器打开 `http://127.0.0.1:8000` 可排序、分页、按目标筛选并导出当前页 CSV。

## 评分与淘汰概览（依据《量化与淘汰标准.md》）
- 六维各 1–5 分，权重：节点完整度 15%、边界清晰 15%、角色混合 15%、试点可行 25%、可评估性 20%、进入性 10%；总分为加权和。
- 硬淘汰：A 类高频事务节点全缺；边界极不清晰；进入性=1 且无法补充联系方式/入口线索。
- 典型信号：
  - A 类：快递/自提、餐饮密度、环卫/垃圾、停车/充电、社区服务。
  - B 类：学校/园区/商业、物业或住宅信息。
  - C 类：门岗/出入口、快递点、垃圾点、停车口（≤核心半径更优）。

## MCP 模式提示
若已在 mcprouter 部署高德 MCP Server，将 `AMAP_MODE=mcp`，并用 `--mcp-base` 指向服务地址；其余逻辑与直连一致，可降低本地暴露 Key 的风险。

## 常见问题
- 地理编码失败：确认城市/区县填写准确；也可直接提供经纬度列。
- 请求过快被限：调低 `--qps`，或在 MCP 端做并发控制。
- 输出乱码：确保 CSV 使用 UTF-8（带或不带 BOM 均可）。

## 复现与迭代
- 本地复跑：修改 `candidates.csv` 后再次执行 `pipeline.py`，旧表会被清空并重建。
- 调参：可在 CSV 中为单行指定半径，或调整 `CATEGORY_MAP` / `DEFAULT_RADII` 等常量后重跑。

欢迎根据实际业务微调 typecode、半径与评分阈值，再结合人工抽检迭代。
