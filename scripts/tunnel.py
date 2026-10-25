#!/usr/bin/env python3
"""Keep one SSH connection alive and expose its local forwarding status."""

import fcntl
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "runs"
STATE = RUNS / "tunnel.state"
LOG = RUNS / "tunnel.log"
CONTROL = RUNS / "tunnel.ssh-control"
STOP = RUNS / "tunnel.stop"
PORTS = (23306, 26379, 25672, 35672, 37017)
FATAL = re.compile(r"Permission denied|Host key verification failed|REMOTE HOST IDENTIFICATION HAS CHANGED|Address already in use|cannot listen to port|Could not request local forwarding|Bad configuration option|Bad port", re.I)


def read_state():
    try:
        return json.loads(STATE.read_text())
    except (OSError, ValueError):
        return {}


def write_state(state):
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state) + "\n")
    temporary.replace(STATE)


def supervisor_alive(_state=None):
    with (RUNS / "tunnel.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except BlockingIOError:
            return True


def control(action):
    try:
        return subprocess.run(["ssh", "-S", str(CONTROL), "-O", action,
                           os.environ["INVHUB_SSH_USER"] + "@" + os.environ["INVHUB_SSH_HOST"]],
                          stdin=subprocess.DEVNULL, capture_output=True, timeout=5).returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


def end_child(child):
    # Prefer a clean exit through the control socket; fall back to signals when
    # the master never got its socket (for example after a crash left a stale one).
    if CONTROL.exists() and control("exit"):
        try:
            child.wait(timeout=10)
            return
        except subprocess.TimeoutExpired:
            pass
    child.terminate()
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()


def ssh_alive(_state=None):
    return CONTROL.exists() and control("check")


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def status():
    state = read_state()
    alive = supervisor_alive(state)
    print(f"Supervisor: {'running' if alive else 'stopped'}; SSH: {'running' if ssh_alive(state) else 'stopped'}; disconnects: {state.get('disconnects', 0)}; state: {state.get('status', 'stopped')}")
    checks = [port_open(p) for p in PORTS]
    for port, opened in zip(PORTS, checks):
        print(f"{'✓' if opened else '✗'} 127.0.0.1:{port}")
    print("TCP listeners only; use make dev-check for service authentication and transactions.")
    return 0 if alive and ssh_alive(state) and all(checks) else 1


def supervise(lock_fd):
    # The inherited descriptor owns the lock for the supervisor's lifetime.
    os.fstat(lock_fd)
    state = {"pid": os.getpid(), "ssh_pid": None, "disconnects": 0, "status": "connecting"}
    write_state(state)
    child = None
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    args = ["ssh", "-N", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
            "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
            "-o", "ControlMaster=yes", "-o", "ControlPath=" + str(CONTROL)]
    for port in PORTS:
        args += ["-L", f"127.0.0.1:{port}:127.0.0.1:{port}"]
    args += ["-p", os.environ["INVHUB_SSH_PORT"], os.environ["INVHUB_SSH_USER"] + "@" + os.environ["INVHUB_SSH_HOST"]]
    delay = 1
    try:
        while not stopping:
            if STOP.exists():
                break
            if LOG.exists() and LOG.stat().st_size > 1024 * 1024:
                LOG.replace(LOG.with_suffix(".log.1"))
            started = time.monotonic()
            with LOG.open("a") as log, (RUNS / "tunnel.ssh-error").open("w+") as errors:
                log.write(time.strftime("%F %T") + " connecting\n")
                log.flush()
                # A master killed with SIGKILL leaves its socket behind.
                CONTROL.unlink(missing_ok=True)
                child = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=errors)
                state.update(ssh_pid=child.pid, status="connecting")
                write_state(state)
                while child.poll() is None:
                    if STOP.exists():
                        stopping = True
                    if stopping:
                        end_child(child)
                        break
                    if state["status"] != "connected" and CONTROL.exists() and all(port_open(p) for p in PORTS):
                        state["status"] = "connected"
                    write_state(state)
                    try:
                        child.wait(timeout=1)
                    except subprocess.TimeoutExpired:
                        continue
                errors.seek(0)
                error = errors.read()
                log.write(error)
                log.write(time.strftime("%F %T") + f" SSH exited ({child.returncode})\n")
            state["ssh_pid"] = None
            if stopping:
                break
            state["disconnects"] += 1
            if FATAL.search(error):
                state["status"] = "gave_up"
                write_state(state)
                return 1
            state["status"] = "reconnecting"
            write_state(state)
            if time.monotonic() - started > 60:
                delay = 1
            deadline = time.monotonic() + delay
            while not stopping and time.monotonic() < deadline:
                if STOP.exists():
                    stopping = True
                time.sleep(0.2)
            delay = min(delay * 2, 10)
    finally:
        if child is not None and child.poll() is None:
            end_child(child)
        if state["status"] != "gave_up":
            state.update(status="stopped", ssh_pid=None)
            write_state(state)
    return 0


def start():
    lock = (RUNS / "tunnel.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("Tunnel supervisor already active.")
        status()
        return 0
    if CONTROL.exists() and control("check"):
        print("An orphaned SSH connection remains; run scripts/tunnel.sh stop.", file=sys.stderr)
        return 1
    busy = [p for p in PORTS if port_open(p)]
    if busy:
        print("Local ports occupied: " + ", ".join(map(str, busy)) + "; inspect them or run scripts/tunnel.sh stop.", file=sys.stderr)
        return 1
    STOP.unlink(missing_ok=True)
    CONTROL.unlink(missing_ok=True)
    with (RUNS / "tunnel-supervisor.log").open("a") as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "_run", str(lock.fileno())],
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                   start_new_session=True, pass_fds=(lock.fileno(),))
    lock.close()
    for _ in range(30):
        if process.poll() is not None:
            print("Tunnel stopped; inspect runs/tunnel.log.", file=sys.stderr)
            return 1
        state = read_state()
        if state.get("pid") == process.pid and state.get("status") == "connected":
            return status()
        time.sleep(0.5)
    print("Tunnel is reconnecting in the background; inspect runs/tunnel.log.")
    status()
    return 0


def stop():
    state = read_state()
    STOP.touch()
    if supervisor_alive(state):
        for _ in range(75):
            if not supervisor_alive(state):
                break
            time.sleep(0.2)
        if supervisor_alive(state):
            print("Supervisor did not stop; inspect runs/tunnel-supervisor.log.", file=sys.stderr)
            return 1
    if ssh_alive(state):
        control("exit")
    print("Tunnel stopped.")
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    RUNS.mkdir(exist_ok=True)
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    if action == "_run" and len(sys.argv) == 3:
        sys.exit(supervise(int(sys.argv[2])))
    if action not in ("start", "stop", "status"):
        sys.exit("Usage: scripts/tunnel.sh start|stop|status")
    sys.exit({"start": start, "stop": stop, "status": status}[action]())
