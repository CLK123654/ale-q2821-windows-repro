from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: build_delivery.py INPUT_ROOT OUTPUT_ROOT")
    input_root = Path(sys.argv[1]).resolve()
    output_root = Path(sys.argv[2]).resolve()
    if output_root.exists():
        raise SystemExit("output目录必须不存在")
    airflow_home = output_root.parent / f".{output_root.name}.airflow-home"
    if airflow_home.exists():
        shutil.rmtree(airflow_home)
    shutil.copytree(ROOT / "implementation" / "template_output", output_root)
    env = os.environ.copy()
    env.update(
        {
            "AIRFLOW_HOME": str(airflow_home),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{(airflow_home / 'airflow.db').as_posix()}",
            "ALE_INPUT_ROOT": str(input_root),
            "ALE_RESULTS_ROOT": str(output_root / "results"),
        }
    )
    migrate = subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        env=env,
        text=True,
        capture_output=True,
        timeout=300,
    )
    if migrate.returncode:
        shutil.rmtree(output_root, ignore_errors=True)
        raise SystemExit(migrate.stdout + migrate.stderr)
    replay = subprocess.run(
        [sys.executable, str(output_root / "tools" / "replay_batches.py")],
        env=env,
        text=True,
        capture_output=True,
        timeout=900,
    )
    if replay.returncode:
        shutil.rmtree(output_root, ignore_errors=True)
        raise SystemExit(replay.stdout + replay.stderr)


if __name__ == "__main__":
    main()
