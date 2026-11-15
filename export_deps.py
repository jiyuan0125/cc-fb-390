#!/usr/bin/env python3
"""export_deps.py — 订单链路调度：依赖图、受阻清单、任务点表导出

输入
    taskruns.csv   一行一次运行：运行号,任务,分区日,开始时间,结束时间,状态,上游运行号列表

输出
    deps.csv       每行一条“上游任务 → 下游任务”的边，带分区日
    blocked.csv    受阻清单（整任务失败 / 单分区失败 两档）
    tasks.csv      逐任务点表（运行次数、成功率、依赖度数、受阻档位）

口径（与调度约定一致）
    * 一次运行 = 输入一行。重跑是失败之后的再一次运行，“重跑”表示重跑成功，
      与“成功”同属产出可用，因此重跑也算一次成功运行。
    * (任务, 分区日) 的最终结果只看该组合最后成功的那次运行（按开始时间，同时间按运行号）：
      存在 成功/重跑 → 最终成功；全部为 失败 → 最终失败（失败且没有重跑成过）。
      同一分区日上“成功”之后再出现的失败运行不会翻转结果，只有又一次成功的重跑才有效。
    * 依赖边来自下游运行行声明的上游运行号列表：上游运行产出哪个分区日，边就带哪个分区日。
      一条边按 (上游任务, 下游任务, 分区日) 去重，重跑不重复出边；被后续重跑取代的
      失败运行所声明的上游关系仍然算这条依赖，因为该次运行确实消费了这些上游运行。
    * 下游某天没有运行记录，该天就不存在这层依赖，也不会被牵连。
    * 受阻清单 = 最终失败的任务 ∪ 消费了这些失败分区的下游任务。
      全部分区日都最终失败 → 整任务失败；否则 → 单分区失败。
    * 成功率 = (成功次数 + 重跑次数) / 运行次数。

用法
    python3 export_deps.py                          # 读 ./taskruns.csv，产物写到 ./
    python3 export_deps.py --input 路径 --outdir 路径
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path

REQUIRED_COLUMNS = (
    "运行号",
    "任务",
    "分区日",
    "开始时间",
    "结束时间",
    "状态",
    "上游运行号列表",
)
VALID_STATUS = ("成功", "失败", "重跑")
OK_STATUS = frozenset(("成功", "重跑"))  # 重跑 = 失败之后的成功重跑
TIER_ALL = "整任务失败"
TIER_PART = "单分区失败"

DEPS_HEADER = ["上游任务", "下游任务", "分区日"]
BLOCKED_HEADER = ["任务", "受阻档位", "失败分区数", "分区日数", "失败分区日", "牵连上游分区"]
TASKS_HEADER = [
    "任务",
    "运行次数",
    "成功次数",
    "失败次数",
    "重跑次数",
    "成功率",
    "分区日数",
    "最终成功分区数",
    "最终失败分区数",
    "出边数",
    "入边数",
    "受阻档位",
]


@dataclass(frozen=True)
class Run:
    run_id: str
    task: str
    part: str
    start: datetime
    end: datetime
    status: str
    upstreams: tuple[str, ...]

    @property
    def order_key(self) -> tuple[datetime, str]:
        return (self.start, self.run_id)


def _text(row: dict, col: str) -> str:
    return (row.get(col) or "").strip()


def load_runs(path: Path) -> list[Run]:
    runs: list[Run] = []
    seen_ids: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"{path}: 空文件，缺少表头")
        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"{path}: 缺少字段 {missing}；实际字段 {reader.fieldnames}")

        for line_no, row in enumerate(reader, start=2):
            if not any((v or "").strip() for v in row.values()):
                continue  # 跳过空行
            run_id = _text(row, "运行号")
            task = _text(row, "任务")
            part = _text(row, "分区日")
            status = _text(row, "状态")
            if not run_id or not task or not part:
                raise ValueError(f"{path}:{line_no}: 运行号/任务/分区日 不能为空")
            if status not in VALID_STATUS:
                raise ValueError(f"{path}:{line_no}: 未知状态 {status!r}，应为 {list(VALID_STATUS)}")
            try:
                date.fromisoformat(part)
            except ValueError:
                raise ValueError(f"{path}:{line_no}: 分区日不是 YYYY-MM-DD: {part!r}") from None
            try:
                start = datetime.fromisoformat(_text(row, "开始时间"))
                end = datetime.fromisoformat(_text(row, "结束时间"))
            except ValueError as exc:
                raise ValueError(f"{path}:{line_no}: 起止时间无法解析: {exc}") from None
            if end < start:
                raise ValueError(f"{path}:{line_no}: 结束时间早于开始时间")
            if run_id in seen_ids:
                raise ValueError(f"{path}:{line_no}: 运行号重复: {run_id}")
            seen_ids.add(run_id)

            raw_up = _text(row, "上游运行号列表")
            upstreams = tuple(u.strip() for u in raw_up.split("|") if u.strip())
            runs.append(Run(run_id, task, part, start, end, status, upstreams))

    if not runs:
        raise ValueError(f"{path}: 没有可用的运行记录")
    return runs


def build(runs: list[Run]) -> dict:
    by_id = {r.run_id: r for r in runs}

    # (任务, 分区日) -> 该组合的全部运行，按开始时间/运行号排序
    partitions: dict[tuple[str, str], list[Run]] = defaultdict(list)
    for r in runs:
        partitions[(r.task, r.part)].append(r)
    for rs in partitions.values():
        rs.sort(key=lambda r: r.order_key)

    # 有效运行：最后一次成功（含重跑）的那次；都没有成功就取最后一次运行
    effective: dict[tuple[str, str], Run] = {}
    for p, rs in partitions.items():
        ok_runs = [r for r in rs if r.status in OK_STATUS]
        effective[p] = ok_runs[-1] if ok_runs else rs[-1]

    final_ok = {p: effective[p].status in OK_STATUS for p in partitions}
    failed_parts = {p for p, ok in final_ok.items() if not ok}

    # 依赖边：下游行声明消费的上游运行 → 上游产出分区日
    edges: set[tuple[str, str, str]] = set()
    consumed_failed: dict[str, set[tuple[str, str]]] = defaultdict(set)
    anomalies: list[str] = []
    for r in runs:
        for uid in dict.fromkeys(r.upstreams):  # 同一行重复声明只算一次
            up = by_id.get(uid)
            if up is None:
                raise ValueError(f"运行 {r.run_id} 引用了不存在的上游运行号 {uid}")
            edges.add((up.task, r.task, up.part))
            if up.part != r.part:
                anomalies.append(
                    f"运行 {r.run_id}（{r.task} 分区 {r.part}）消费了 {uid}"
                    f"（{up.task} 分区 {up.part}），分区日不一致"
                )
            if (up.task, up.part) in failed_parts:
                consumed_failed[r.task].add((up.task, up.part))

    failed_tasks = {t for (t, _d) in failed_parts}
    blocked_tasks = failed_tasks | set(consumed_failed)

    # 任务点表
    task_names = sorted({r.task for r in runs})
    per_task_status: dict[str, Counter] = defaultdict(Counter)
    for r in runs:
        per_task_status[r.task][r.status] += 1

    task_days: dict[str, list[str]] = defaultdict(list)
    for (t, d) in partitions:
        task_days[t].append(d)
    for t in task_days:
        task_days[t].sort()

    out_deg = Counter(up for up, _down, _d in edges)
    in_deg = Counter(down for _up, down, _d in edges)

    def tier_of(task: str) -> str:
        if task not in blocked_tasks:
            return ""
        days = task_days[task]
        failed = [d for d in days if not final_ok[(task, d)]]
        # 只在档位判定里区分：全部分区日都最终失败 → 整任务失败
        return TIER_ALL if len(failed) == len(days) else TIER_PART

    tasks_rows: list[list] = []
    for t in task_names:
        st = per_task_status[t]
        n = st["成功"] + st["失败"] + st["重跑"]
        succ, fail, rerun = st["成功"], st["失败"], st["重跑"]
        rate = (succ + rerun) / n if n else 0.0
        days = task_days[t]
        ok_days = sum(1 for d in days if final_ok[(t, d)])
        tasks_rows.append(
            [
                t,
                n,
                succ,
                fail,
                rerun,
                f"{rate:.4f}",
                len(days),
                ok_days,
                len(days) - ok_days,
                out_deg.get(t, 0),
                in_deg.get(t, 0),
                tier_of(t),
            ]
        )

    # 受阻清单：整任务失败排前面，其余按任务名
    blocked_rows: list[list] = []
    for t in sorted(blocked_tasks, key=lambda x: (tier_of(x) != TIER_ALL, x)):
        days = task_days[t]
        bad_days = [d for d in days if not final_ok[(t, d)]]
        causes = sorted(consumed_failed.get(t, set()))
        blocked_rows.append(
            [
                t,
                tier_of(t),
                len(bad_days),
                len(days),
                ";".join(bad_days),
                ";".join(f"{u}@{d}" for u, d in causes),
            ]
        )

    deps_rows = [list(e) for e in sorted(edges, key=lambda e: (e[2], e[0], e[1]))]

    return {
        "runs": runs,
        "by_id": by_id,
        "partitions": partitions,
        "effective": effective,
        "final_ok": final_ok,
        "failed_parts": failed_parts,
        "failed_tasks": failed_tasks,
        "blocked_tasks": blocked_tasks,
        "consumed_failed": consumed_failed,
        "edges": edges,
        "deps_rows": deps_rows,
        "blocked_rows": blocked_rows,
        "tasks_rows": tasks_rows,
        "anomalies": anomalies,
        "task_days": task_days,
    }


def check_consistency(m: dict) -> None:
    """三份产物计数互洽的自检；对不上直接报错，不导出半成品。"""
    runs = m["runs"]
    edges = m["edges"]
    tasks_rows = m["tasks_rows"]
    blocked_rows = m["blocked_rows"]
    failed_parts = m["failed_parts"]
    final_ok = m["final_ok"]

    if sum(row[1] for row in tasks_rows) != len(runs):
        raise ValueError("tasks 运行次数合计 != 运行行数")
    for row in tasks_rows:
        if row[1] != row[2] + row[3] + row[4]:
            raise ValueError(f"{row[0]} 状态计数合计 != 运行次数")
        if row[1] == 0:
            raise ValueError(f"{row[0]} 运行次数为 0")
        if not (0.0 <= float(row[5]) <= 1.0):
            raise ValueError(f"{row[0]} 成功率超出 [0,1]")
        if row[6] != row[7] + row[8]:
            raise ValueError(f"{row[0]} 分区日数 != 成功分区数 + 失败分区数")

    if sum(row[9] for row in tasks_rows) != len(edges):
        raise ValueError("tasks 出边数合计 != deps 边数")
    if sum(row[10] for row in tasks_rows) != len(edges):
        raise ValueError("tasks 入边数合计 != deps 边数")
    if sum(row[8] for row in tasks_rows) != len(failed_parts):
        raise ValueError("tasks 最终失败分区数合计 != 最终失败分区数")
    if sum(row[2] for row in blocked_rows) != len(failed_parts):
        raise ValueError("blocked 失败分区数合计 != 最终失败分区数")

    blocked_tasks = m["blocked_tasks"]
    if {row[0] for row in blocked_rows} != blocked_tasks:
        raise ValueError("blocked 任务集合与受阻判定不一致")
    for row in tasks_rows:
        t, tier = row[0], row[11]
        if t not in blocked_tasks:
            if tier != "":
                raise ValueError(f"{t} 未受阻却带档位")
            continue
        if tier not in (TIER_ALL, TIER_PART):
            raise ValueError(f"{t} 档位非法: {tier!r}")
        if tier == TIER_ALL and row[8] != row[6]:
            raise ValueError(f"{t} 整任务失败但仍有成功分区")
        if tier == TIER_PART and not (0 <= row[8] < row[6]):
            raise ValueError(f"{t} 单分区失败档位与分区计数不符")
        # 分区的最终状态只有一种
        for d in m["task_days"][t]:
            ok = final_ok[(t, d)]
            if not isinstance(ok, bool):
                raise ValueError(f"{t}@{d} 最终状态不是布尔")

    for up, down, day in edges:
        if up not in m["task_days"] or down not in m["task_days"]:
            raise ValueError(f"deps 边 {up}->{down}@{day} 的任务不在点表")
        if not any(
            r.task == down and r.part == day and up in {m["by_id"][u].task for u in r.upstreams}
            for r in runs
        ):
            raise ValueError(f"deps 边 {up}->{down}@{day} 找不到对应的下游运行声明")


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    os.replace(tmp, path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="导出订单链路调度依赖图与受阻清单")
    ap.add_argument("--input", default="taskruns.csv", help="运行记录 CSV（默认 ./taskruns.csv）")
    ap.add_argument("--outdir", default=".", help="产物输出目录（默认当前目录）")
    args = ap.parse_args(argv)

    in_path = Path(args.input)
    if not in_path.is_file():
        ap.error(f"找不到输入文件: {in_path}")
    runs = load_runs(in_path)
    model = build(runs)
    check_consistency(model)  # 先自检，数据不互洽就不落盘

    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    deps_path = out_dir / "deps.csv"
    blocked_path = out_dir / "blocked.csv"
    tasks_path = out_dir / "tasks.csv"
    write_csv(deps_path, DEPS_HEADER, model["deps_rows"])
    write_csv(blocked_path, BLOCKED_HEADER, model["blocked_rows"])
    write_csv(tasks_path, TASKS_HEADER, model["tasks_rows"])

    for msg in model["anomalies"]:
        print(f"注意: {msg}", file=sys.stderr)

    status = Counter(r.status for r in runs)
    tier_counts = Counter(row[1] for row in model["blocked_rows"])
    print(f"输入 {in_path}：{len(runs)} 次运行，{len(model['partitions'])} 个任务分区，"
          f"{len(model['task_days'])} 个任务")
    print(f"{deps_path}：{len(model['edges'])} 条边")
    print(f"{blocked_path}：{len(model['blocked_rows'])} 个受阻任务"
          f"（{TIER_ALL} {tier_counts[TIER_ALL]}，{TIER_PART} {tier_counts[TIER_PART]}）")
    print(f"{tasks_path}：{len(model['tasks_rows'])} 个任务；"
          f"运行 成功 {status['成功']} / 重跑 {status['重跑']} / 失败 {status['失败']}")
    print(f"最终失败分区 {len(model['failed_parts'])} 个，最终失败任务 {len(model['failed_tasks'])} 个")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
