import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def run_step(script_name: str, year: int | None) -> int:
    script_path = Path(script_name)
    if not script_path.exists():
        logging.error("Script not found: %s", script_name)
        return 1

    cmd = [sys.executable, script_name]
    if year is not None:
        cmd.extend(["--year", str(year)])

    logging.info("Running: %s", " ".join(cmd))
    completed = subprocess.run(cmd, check=False)
    if completed.returncode != 0:
        logging.error("Step failed (%s) with exit code %s", script_name, completed.returncode)
    else:
        logging.info("Step completed: %s", script_name)
    return completed.returncode


def run_once(year: int | None, continue_on_error: bool) -> int:
    steps = ["events.py", "sessions.py", "results.py"]
    for step in steps:
        code = run_step(step, year)
        if code != 0 and not continue_on_error:
            return code
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run MotoGP import pipeline: events -> sessions -> results")
    parser.add_argument("--year", type=int, default=None, help="Target year passed to each step")
    parser.add_argument("--loop", action="store_true", help="Run continuously in a loop")
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=900,
        help="Delay between cycles when --loop is enabled (default: 900)",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Stop after N cycles (only with --loop)",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue remaining steps/cycles even if one step fails",
    )
    args = parser.parse_args()

    setup_logging()

    if not args.loop:
        return run_once(args.year, args.continue_on_error)

    cycle = 0
    while True:
        cycle += 1
        logging.info("Starting cycle %s", cycle)
        code = run_once(args.year, args.continue_on_error)
        if code != 0 and not args.continue_on_error:
            return code

        if args.max_cycles is not None and cycle >= args.max_cycles:
            logging.info("Reached max cycles: %s", args.max_cycles)
            return 0

        logging.info("Sleeping %s seconds before next cycle", args.sleep_seconds)
        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
