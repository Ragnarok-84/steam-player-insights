"""Offline checks for the integrity observer and distributed submit contract."""
import io
import json
import subprocess
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from infra.k3s import fault_watch
from infra.k3s.render_benchmark import render


def observe(drop_acknowledged=False):
    clock = [0.0]
    records = []
    producer = MagicMock()
    producer.send.side_effect = lambda topic, value: (records.append(value) or MagicMock())
    consumer = MagicMock()

    def poll(**kwargs):
        clock[0] += 1
        return {0: [SimpleNamespace(value=value) for value in records
                    if not (drop_acknowledged and value["seq"] == 0)]}

    consumer.poll.side_effect = poll
    hdfs = MagicMock()
    hdfs.read.side_effect = lambda path: io.BytesIO(b"steam-ft-probe\n")
    output = io.StringIO()
    with patch.object(fault_watch, "KafkaAdminClient"), \
         patch.object(fault_watch, "KafkaProducer", return_value=producer), \
         patch.object(fault_watch, "KafkaConsumer", return_value=consumer), \
         patch.object(fault_watch, "InsecureClient", return_value=hdfs), \
         patch.object(fault_watch.time, "monotonic", side_effect=lambda: clock[0]), \
         patch.object(fault_watch.time, "sleep", side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds)), \
         patch("sys.argv", ["fault-watch", "--seconds", "60"]), redirect_stdout(output):
        if drop_acknowledged:
            with pytest.raises(RuntimeError, match="observation failed"):
                fault_watch.main()
        else:
            fault_watch.main()
    return output.getvalue()


def test_observer_rejects_missing_acknowledged_record():
    output = observe(drop_acknowledged=True)
    result = next(json.loads(line) for line in output.splitlines() if '"event": "RESULT"' in line)
    assert result["missing_acked"] == [0]
    assert "INTEGRITY_OK" not in output


def test_observer_accepts_complete_acknowledged_set():
    output = observe()
    result = next(json.loads(line) for line in output.splitlines() if '"event": "RESULT"' in line)
    assert result["acknowledged"] == 60
    assert result["received_unique"] == 60
    assert result["missing_acked"] == []
    assert "INTEGRITY_OK" in output


def test_distributed_submit_expands_executor_count_and_pod_address(tmp_path):
    capture = tmp_path / "spark-submit"
    capture.write_text('#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
    capture.chmod(0o755)
    import os
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "POD_NAME": "driver-test",
           "POD_IP": "10.42.1.10", "EXECUTORS": "4", "ROWS": "1000000"}
    command = render(4, 1000000)["spec"]["template"]["spec"]["containers"][0]["command"]
    argv = json.loads(subprocess.check_output(command, env=env, text=True))
    assert argv[argv.index("--master") + 1] == "k8s://https://kubernetes.default.svc:443"
    assert "spark.executor.instances=4" in argv
    assert "spark.driver.host=10.42.1.10" in argv
    assert "spark.kubernetes.driver.pod.name=driver-test" in argv
    assert argv[-1] == "hdfs://namenode:8020/steam/bench/driver-test"


def test_executor_entrypoint_keeps_non_spark_commands_working():
    script = Path(__file__).resolve().parents[2] / "infra/k3s/spark-entrypoint.sh"
    output = subprocess.check_output(["bash", str(script), "printf", "%s", "demo-pass-through"], text=True)
    assert output == "demo-pass-through"
