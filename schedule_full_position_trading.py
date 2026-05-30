"""
每天定时运行当日满仓决策脚本（long_cycle_full_position_runner.py）。

默认每天 21:00、21:30 执行：
    python schedule_full_position_trading.py

指定执行时间（单个）：
    python schedule_full_position_trading.py --run-time 20:30

指定多个执行时间：
    python schedule_full_position_trading.py --run-times 20:00,20:30

仅执行一次（便于测试）：
    python schedule_full_position_trading.py --once
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_FULL_POSITION_DECISION_ROOT = Path("data/artifacts/decision/daily_full_position_ver1/portfolio")
logger = logging.getLogger(__name__)


def configure_logging(log_file: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


def parse_time_str(time_str: str) -> tuple[int, int]:
    try:
        hour_str, minute_str = time_str.split(":")
        hour = int(hour_str)
        minute = int(minute_str)
    except ValueError as exc:
        raise ValueError(f"时间格式错误: {time_str}，应为 HH:MM") from exc

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"时间范围错误: {time_str}")
    return hour, minute


def parse_time_list(run_times: str) -> list[tuple[int, int]]:
    slots = []
    for item in run_times.split(","):
        item = item.strip()
        if not item:
            continue
        slots.append(parse_time_str(item))

    unique_slots = sorted(set(slots))
    if not unique_slots:
        raise ValueError("至少需要一个有效执行时间，例如 20:00 或 20:00,20:30")
    return unique_slots


def run_full_position_for_today(
    project_root: Path,
    script_path: Path,
    python_executable: str,
    full_position_decision_root: Path,
    interval: int,
) -> int:
    today = datetime.now().strftime("%Y%m%d")
    bootstrap_code = """
import importlib.util
import sys
from pathlib import Path

script_path = Path(sys.argv[1]).resolve()
trade_date = sys.argv[2]
decision_root = Path(sys.argv[3])
interval = sys.argv[4]

spec = importlib.util.spec_from_file_location("full_position_override", script_path)
if spec is None or spec.loader is None:
    raise RuntimeError(f"无法加载脚本: {script_path}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

module.FULL_POSITION_DECISION_ROOT = decision_root
sys.argv = [
    str(script_path),
    "--start-date",
    trade_date,
    "--end-date",
    trade_date,
    "--interval",
    interval,
    "--single-date",
    trade_date,
]
module.main()
""".strip()

    command = [
        python_executable,
        "-c",
        bootstrap_code,
        str(script_path),
        today,
        str(full_position_decision_root),
        str(interval),
    ]
    logger.info("开始执行: %s", " ".join(command))
    completed = subprocess.run(command, cwd=project_root, check=False)
    logger.info("执行结束，退出码: %s", completed.returncode)
    return completed.returncode


def get_next_run_time(slots: list[tuple[int, int]]) -> datetime:
    now = datetime.now()
    candidates: list[datetime] = []
    for hour, minute in slots:
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        candidates.append(target)
    return min(candidates)


def run_scheduler(
    run_times: str,
    once: bool,
    project_root: Path,
    script_path: Path,
    python_executable: str,
    full_position_decision_root: Path,
    interval: int,
) -> None:
    slots = parse_time_list(run_times)
    slot_text = ", ".join(f"{h:02d}:{m:02d}" for h, m in slots)
    logger.info(
        "定时器启动，计划每天 [%s] 执行 %s (cwd=%s)",
        slot_text,
        script_path.as_posix(),
        project_root.as_posix(),
    )
    logger.info("Full Position 存储路径覆盖为: %s", full_position_decision_root.as_posix())

    while True:
        next_run = get_next_run_time(slots)
        wait_seconds = max(0, int((next_run - datetime.now()).total_seconds()))
        logger.info("下一次执行时间: %s（约 %s 秒后）", next_run.strftime("%Y-%m-%d %H:%M:%S"), wait_seconds)
        time.sleep(wait_seconds)

        exit_code = run_full_position_for_today(
            project_root=project_root,
            script_path=script_path,
            python_executable=python_executable,
            full_position_decision_root=full_position_decision_root,
            interval=interval,
        )
        if exit_code != 0:
            logger.warning("当日任务执行失败，exit_code=%s", exit_code)
        else:
            logger.info("当日任务执行成功")

        if once:
            logger.info("once 模式已完成，退出定时器")
            return

        time.sleep(61)


def main() -> None:
    parser = argparse.ArgumentParser(description="每天定时运行当日满仓决策脚本")
    parser.add_argument("--run-time", default="", help="每日执行时间，格式 HH:MM；设置后优先于 --run-times")
    parser.add_argument("--run-times", default="21:00,21:30", help="每日执行时间列表，格式 HH:MM,HH:MM")
    parser.add_argument("--once", action="store_true", help="仅触发一次后退出（用于测试）")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT), help="运行命令的工作目录")
    parser.add_argument("--script-path", default="long_cycle_full_position_runner.py", help="要执行的脚本路径")
    parser.add_argument("--python-executable", default=sys.executable, help="Python 可执行文件路径")
    parser.add_argument("--interval", type=int, default=20, help="传给满仓脚本的交易日间隔参数")
    parser.add_argument(
        "--log-file",
        default="data/logs/full_position_scheduler.log",
        help="定时器日志文件路径；传空字符串可关闭文件日志",
    )
    parser.add_argument(
        "--full-position-decision-root",
        default=str(DEFAULT_FULL_POSITION_DECISION_ROOT),
        help="覆盖 long_cycle_full_position_runner 的 FULL_POSITION_DECISION_ROOT",
    )
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    script_path = Path(args.script_path)
    if not script_path.is_absolute():
        script_path = (project_root / script_path).resolve()

    log_file_arg = args.log_file.strip()
    log_file = None
    if log_file_arg:
        log_file = Path(log_file_arg)
        if not log_file.is_absolute():
            log_file = (project_root / log_file).resolve()

    full_position_decision_root = Path(args.full_position_decision_root)
    if not full_position_decision_root.is_absolute():
        full_position_decision_root = (project_root / full_position_decision_root).resolve()

    configure_logging(log_file)
    if not script_path.exists():
        raise FileNotFoundError(f"脚本不存在: {script_path.as_posix()}")

    run_scheduler(
        run_times=args.run_time if args.run_time else args.run_times,
        once=args.once,
        project_root=project_root,
        script_path=script_path,
        python_executable=args.python_executable,
        full_position_decision_root=full_position_decision_root,
        interval=args.interval,
    )


if __name__ == "__main__":
    main()

