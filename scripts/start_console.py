"""Launch the local console, preserving configuration and owning only its gateway child.

Storage services and volumes stay running when the launcher exits. No dependencies
are installed, no token is printed, and browser opening requires --open.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class LaunchError(Exception):
    """A safe operator-facing failure without configuration values."""


def port_number(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Port must be an integer between 1 and 65535") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("Port must be between 1 and 65535")
    return port


def project_name(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", value):
        raise argparse.ArgumentTypeError(
            "Project must use 1–63 lowercase letters, digits, hyphens or underscores"
        )
    return value


def positive_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Startup timeout must be a positive number") from exc
    if not math.isfinite(timeout) or not 1 <= timeout <= 3600:
        raise argparse.ArgumentTypeError("Startup timeout must be between 1 and 3600 seconds")
    return timeout


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--mode",
        choices=("demo", "configured"),
        default="demo",
        help="demo uses synthetic replies; configured uses the normal gateway and model settings",
    )
    result.add_argument(
        "--project",
        type=project_name,
        default="contextual-gateway-console",
        help="Explicit Docker Compose project name (default: contextual-gateway-console)",
    )
    result.add_argument(
        "--port", type=port_number, default=8001, help="Loopback gateway port (default: 8001)"
    )
    result.add_argument(
        "--env-file",
        type=Path,
        default=ROOT / ".env",
        help="Configuration file; custom files must already exist (default: repository .env)",
    )
    result.add_argument(
        "--no-services",
        action="store_true",
        help="Use already-running storage services; do not invoke Docker",
    )
    result.add_argument(
        "--startup-timeout",
        type=positive_timeout,
        default=120.0,
        help="Seconds allowed for each storage/gateway startup stage (default: 120)",
    )
    result.add_argument(
        "--open", action="store_true", help="Open the console in the default browser after readiness"
    )
    result.add_argument("--_serve", action="store_true", help=argparse.SUPPRESS)
    return result


def require_dependencies() -> None:
    if sys.version_info < (3, 12):
        raise LaunchError(
            "Use Python 3.12 or newer and install requirements-dev.txt in a virtual environment."
        )
    missing = [
        name
        for name in (
            "uvicorn",
            "fastapi",
            "pydantic_settings",
            "dotenv",
            "httpx",
            "asyncpg",
            "neo4j",
            "redis",
            "anthropic",
        )
        if importlib.util.find_spec(name) is None
    ]
    if missing:
        raise LaunchError(
            "Gateway dependencies are missing. Activate your virtual environment and run "
            "python -m pip install -r requirements-dev.txt before launching."
        )


def ensure_env_file(env_file: Path, mode: str, *, root: Path = ROOT) -> None:
    if env_file.is_file():
        return
    if env_file.exists():
        raise LaunchError("The selected environment path is not a regular file.")
    if mode != "demo":
        raise LaunchError("Configured mode requires an existing .env or --env-file. See .env.example.")
    if env_file != root / ".env":
        raise LaunchError(
            "A custom --env-file must already exist. Demo setup only creates the repository .env."
        )
    result = subprocess.run(
        [sys.executable, str(root / "scripts" / "setup_demo.py")],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode or not env_file.is_file():
        raise LaunchError(
            "Demo configuration could not be created. Existing configuration was not overwritten."
        )
    print("Created a local demo .env with random credentials; no existing file was overwritten.", flush=True)


def load_settings(env_file: Path):
    """Select exactly one dotenv file, while preserving normal exported-env precedence.

    app.config constructs a default Settings during import. Isolating that initial
    import prevents a different or malformed cwd/.env from being read first. The
    application modules are imported only after the selected settings are assigned.
    """
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        previous = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="lowq-config-import-") as isolated:
            try:
                os.chdir(isolated)
                from app.config import Settings
            finally:
                os.chdir(previous)
        return Settings(_env_file=env_file)
    except (ValueError, OSError) as exc:
        raise LaunchError(
            "Invalid gateway configuration. Check the selected env file and exported settings."
        ) from exc


def validate_mode(settings, mode: str) -> None:
    try:
        if mode == "demo":
            if (
                settings.environment != "development"
                or not settings.auth_enabled
                or not settings.gateway_api_keys
            ):
                raise LaunchError(
                    "Demo mode requires development mode, authentication, and GATEWAY_API_KEYS."
                )
            # Validate identities/limits without requiring unused paid-provider keys.
            settings.model_copy(
                update={"anthropic_api_key": "unused-demo", "voyage_api_key": "unused-demo"}
            ).validate_runtime()
        else:
            settings.validate_runtime()
            if settings.embedding_backend == "local" and any(
                importlib.util.find_spec(name) is None for name in ("sentence_transformers", "torch")
            ):
                raise LaunchError(
                    "Local embeddings require requirements-acceleration.txt. Install it explicitly."
                )
    except ValueError as exc:
        raise LaunchError(
            "Gateway settings are incomplete for this mode. Check authentication, provider keys, and limits."
        ) from exc


def check_port(port: int) -> None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            if os.name == "posix":
                # Match Uvicorn's restart behavior without treating TCP TIME_WAIT as a listener.
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    except OSError as exc:
        raise LaunchError(
            f"Port {port} is unavailable. Choose another --port; "
            "the launcher will not stop an existing process."
        ) from exc


def compose_command(args, *, root: Path = ROOT) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-name",
        args.project,
        "--env-file",
        str(args.env_file),
        "--file",
        str(root / "docker-compose.yml"),
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        str(math.ceil(args.startup_timeout)),
        "postgres",
        "neo4j",
        "redis",
    ]


def start_services(args, *, root: Path = ROOT) -> None:
    if shutil.which("docker") is None:
        raise LaunchError(
            "Docker Compose is required. Start Docker, or use --no-services with existing stores."
        )
    print(
        f"Starting storage services in Compose project {args.project}; existing volumes are preserved.",
        flush=True,
    )
    try:
        result = subprocess.run(
            compose_command(args, root=root),
            cwd=root,
            capture_output=True,
            text=True,
            timeout=args.startup_timeout + 15,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise LaunchError(
            "Storage startup timed out. Inspect Docker health or increase --startup-timeout."
        ) from exc
    if result.returncode:
        output = (result.stderr + result.stdout).lower()
        if "port" in output and any(word in output for word in ("allocated", "in use", "bind")):
            raise LaunchError(
                "A storage port is already in use. Reuse the existing stores with --no-services and matching "
                "connection settings, or choose the existing Compose --project."
            )
        if "daemon" in output or "cannot connect" in output:
            raise LaunchError("Docker is unavailable. Start Docker and retry.")
        # Compose can include interpolated secrets in errors; do not echo its output.
        raise LaunchError(
            "Storage startup failed. Inspect Docker service health and credentials. No volumes were removed."
        )


def child_command(args) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--_serve",
        "--mode",
        args.mode,
        "--env-file",
        str(args.env_file),
        "--port",
        str(args.port),
    ]


def stop_child(child, *, timeout: float = 20) -> None:
    if child.poll() is not None:
        return
    try:
        child.send_signal(signal.SIGINT)
        child.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
    except ProcessLookupError:
        pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def wait_ready(child, port: int, timeout: float) -> None:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise LaunchError("The gateway exited during startup. Review its startup logs and configuration.")
        try:
            with opener.open(f"http://127.0.0.1:{port}/health/ready", timeout=1) as response:
                data = json.loads(response.read(65536))
                if (
                    response.status == 200
                    and isinstance(data, dict)
                    and data.get("status") in {"ready", "degraded"}
                ):
                    return
        except (OSError, ValueError, urllib.error.URLError):
            pass
        time.sleep(0.2)
    raise LaunchError(
        "Gateway readiness timed out. Check storage/model configuration or increase --startup-timeout."
    )


def serve(args) -> int:
    selected = load_settings(args.env_file)
    validate_mode(selected, args.mode)
    import app.config

    app.config.settings = selected
    import uvicorn

    if args.mode == "demo":
        selected.simple_model = selected.complex_model = "synthetic-demo"
        from scripts.demo_server import build_demo_app

        application = build_demo_app()
    else:
        from app.main import app

        application = app
    uvicorn.run(
        application,
        host="127.0.0.1",
        port=args.port,
        proxy_headers=False,
        access_log=False,
        timeout_graceful_shutdown=15,
    )
    return 0


def launch(args) -> int:
    require_dependencies()
    check_port(args.port)
    ensure_env_file(args.env_file, args.mode)
    validate_mode(load_settings(args.env_file), args.mode)
    if not args.no_services:
        start_services(args)
    label = (
        "SYNTHETIC DEMO: replies echo fixture context; no paid provider"
        if args.mode == "demo"
        else "CONFIGURED GATEWAY: replies use your configured generation backend"
    )
    print(label, flush=True)
    child = subprocess.Popen(child_command(args), cwd=ROOT, start_new_session=os.name == "posix")
    try:
        wait_ready(child, args.port, args.startup_timeout)
        url = f"http://127.0.0.1:{args.port}/inspector"
        print(f"Console ready: {url}", flush=True)
        print(
            f"Connect with a key from GATEWAY_API_KEYS in {args.env_file}, or its exported override.",
            flush=True,
        )
        print(
            "Press Ctrl+C to stop this gateway. Storage services and volumes will remain available.",
            flush=True,
        )
        if args.open:
            if not webbrowser.open(url, new=2):
                print("The browser did not open automatically. Open the console URL above.", flush=True)
        return child.wait()
    finally:
        stop_child(child)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    args.env_file = args.env_file.expanduser().resolve()
    previous_handlers = {}

    def interrupt(_signum, _frame):
        raise KeyboardInterrupt

    if not args._serve:
        for signum in (signal.SIGTERM, getattr(signal, "SIGHUP", signal.SIGTERM)):
            if signum not in previous_handlers:
                previous_handlers[signum] = signal.signal(signum, interrupt)
    try:
        return serve(args) if args._serve else launch(args)
    except KeyboardInterrupt:
        print("\nConsole stopped. Storage services and volumes were left running.", flush=True)
        return 130
    except (LaunchError, OSError, subprocess.SubprocessError) as exc:
        # Unexpected subprocess exceptions can include command/env details.
        message = (
            str(exc) if isinstance(exc, LaunchError) else "Launch failed; check local dependencies and paths."
        )
        print(f"Console could not start: {message}", file=sys.stderr)
        return 2
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
