# tests/conftest.py
import socket
import subprocess
import time
import pytest


def _free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture(scope="session")
def mosquitto_broker():
    """Start a real mosquitto broker on a free port. Yields (host, port)."""
    port = _free_port()
    proc = subprocess.Popen(
        ["/usr/sbin/mosquitto", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.3)  # let it start
    assert proc.poll() is None, "mosquitto failed to start"
    yield ("localhost", port)
    proc.terminate()
    proc.wait()


class RestartableBroker:
    def __init__(self) -> None:
        self.host = "localhost"
        self.port = _free_port()
        self.proc = None
        self.start()

    def start(self) -> None:
        self.proc = subprocess.Popen(
            ["/usr/sbin/mosquitto", "-p", str(self.port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(0.3)
        assert self.proc.poll() is None, "mosquitto failed to start"

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait()
        self.proc = None

    def restart(self) -> None:
        self.stop()
        self.start()


@pytest.fixture
def restartable_broker():
    broker = RestartableBroker()
    try:
        yield broker
    finally:
        broker.stop()
