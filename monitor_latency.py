import subprocess
import time
from datetime import datetime, time as dtime
import logging
from typing import Callable

HOST = "bromotenggersemeru.id"
CHECK_INTERVAL = 5  # seconds
START_TIME = dtime(15, 55)
END_TIME = dtime(16, 15)
LOG_FILE = "latency.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

def ping_latency(host: str) -> float | None:
    """Ping the host once and return latency in milliseconds.

    Returns None if the ping fails or latency cannot be determined.
    """
    try:
        output = subprocess.check_output(
            ["ping", "-c", "1", "-W", "2", host],
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
    except subprocess.CalledProcessError:
        return None

    for line in output.splitlines():
        if "time=" in line:
            try:
                return float(line.split("time=")[1].split()[0])
            except (IndexError, ValueError):
                return None
    return None

def within_monitoring_window(now: datetime) -> bool:
    return START_TIME <= now.time() <= END_TIME

def monitor_latency_loop(on_result: Callable[[str], None] | None = None) -> None:
    """Run the latency monitor until the end of the monitoring window.

    If ``on_result`` is provided, it will be called with each log line.
    """
    while datetime.now().time() <= END_TIME:
        now = datetime.now()
        if within_monitoring_window(now):
            latency = ping_latency(HOST)
            if latency is not None:
                msg = f"Latency: {latency:.2f} ms"
            else:
                msg = "Ping failed"
            logging.info(msg)
            if on_result:
                on_result(msg)
        time.sleep(CHECK_INTERVAL)


def main() -> None:
    monitor_latency_loop()


if __name__ == "__main__":
    main()
