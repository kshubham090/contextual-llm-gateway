import json
import os
import signal
import socket
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from fastapi import FastAPI

from app.inspector import router
from scripts import start_console


def test_launcher_defaults_are_explicit_and_browser_opening_is_opt_in():
    args = start_console.parser().parse_args([])
    assert args.mode == "demo"
    assert args.project == "contextual-gateway-console"
    assert args.port == 8001
    assert args.open is False
    assert args.no_services is False


@pytest.mark.parametrize(
    "arguments",
    [
        ["--mode", "production"],
        ["--port", "0"],
        ["--port", "65536"],
        ["--project", "../other"],
        ["--project", "-bad"],
        ["--startup-timeout", "nan"],
        ["--startup-timeout", "0"],
    ],
)
def test_invalid_arguments_fail_before_starting_any_process(arguments):
    with pytest.raises(SystemExit) as error:
        start_console.parser().parse_args(arguments)
    assert error.value.code == 2


def test_existing_env_is_never_replaced(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    content = b"GATEWAY_API_KEYS='existing-private-content'\n"
    path.write_bytes(content)
    run = Mock(side_effect=AssertionError("No setup process should run"))
    monkeypatch.setattr(start_console.subprocess, "run", run)
    for mode in ("demo", "configured"):
        start_console.ensure_env_file(path, mode, root=tmp_path)
        assert path.read_bytes() == content
    run.assert_not_called()


@pytest.mark.parametrize("mode,filename", [("configured", ".env"), ("demo", "custom.env")])
def test_missing_configured_or_custom_env_does_not_generate_credentials(
    tmp_path, monkeypatch, mode, filename
):
    run = Mock(side_effect=AssertionError("Setup is only allowed for the default demo file"))
    monkeypatch.setattr(start_console.subprocess, "run", run)
    with pytest.raises(start_console.LaunchError):
        start_console.ensure_env_file(tmp_path / filename, mode, root=tmp_path)
    run.assert_not_called()
    assert not (tmp_path / filename).exists()


def test_absent_default_demo_env_runs_existing_setup_without_printing_values(tmp_path, monkeypatch, capsys):
    path = tmp_path / ".env"

    def setup(command, **kwargs):
        assert command == [start_console.sys.executable, str(tmp_path / "scripts" / "setup_demo.py")]
        assert kwargs["cwd"] == tmp_path
        assert kwargs["capture_output"] is True
        path.write_text("private-demo-value")
        return SimpleNamespace(returncode=0, stdout="private-demo-value", stderr="")

    monkeypatch.setattr(start_console.subprocess, "run", setup)
    start_console.ensure_env_file(path, "demo", root=tmp_path)
    assert "private-demo-value" not in capsys.readouterr().out


def test_missing_dependencies_explain_installation_without_installing(monkeypatch):
    monkeypatch.setattr(start_console.importlib.util, "find_spec", lambda _: None)
    with pytest.raises(start_console.LaunchError, match="requirements-dev.txt"):
        start_console.require_dependencies()


def test_port_preflight_never_takes_over_existing_listener():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        with pytest.raises(start_console.LaunchError, match="will not stop"):
            start_console.check_port(listener.getsockname()[1])


def test_compose_command_selects_project_env_and_only_backing_stores(tmp_path):
    args = start_console.parser().parse_args(["--project", "my-console", "--startup-timeout", "12.5"])
    args.env_file = tmp_path / "private.env"
    command = start_console.compose_command(args, root=tmp_path)
    assert command[:5] == ["docker", "compose", "--project-name", "my-console", "--env-file"]
    assert command[5] == str(args.env_file)
    assert "--wait" in command
    assert command[command.index("--wait-timeout") + 1] == "13"
    assert command[-3:] == ["postgres", "neo4j", "redis"]
    assert "down" not in command and "--remove-orphans" not in command


def test_compose_failure_never_echoes_interpolated_secrets(monkeypatch, capsys):
    args = start_console.parser().parse_args([])
    monkeypatch.setattr(start_console.shutil, "which", lambda _: "/test/docker")
    monkeypatch.setattr(
        start_console.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(
            returncode=1,
            stdout="password=PRIVATE_SECRET",
            stderr="service failed",
        ),
    )
    with pytest.raises(start_console.LaunchError) as error:
        start_console.start_services(args)
    assert "PRIVATE_SECRET" not in str(error.value)
    assert "PRIVATE_SECRET" not in capsys.readouterr().out


def test_custom_env_uses_exported_overrides_without_mutating_parent_settings(tmp_path, monkeypatch):
    from app.config import settings

    original = settings.gateway_api_keys.copy()
    path = tmp_path / "custom.env"
    key = "k" * 40
    path.write_text("GATEWAY_API_KEYS='" + json.dumps({key: "from-file"}) + "'\nSIMPLE_MODEL=file-model\n")
    monkeypatch.delenv("GATEWAY_API_KEYS", raising=False)
    monkeypatch.setenv("SIMPLE_MODEL", "exported-model")
    selected = start_console.load_settings(path)
    assert selected.gateway_api_keys == {key: "from-file"}
    assert selected.simple_model == "exported-model"
    assert settings.gateway_api_keys == original


def test_cold_settings_import_ignores_a_different_malformed_default_env(tmp_path):
    (tmp_path / ".env").write_text("EMBEDDING_DIM=not-a-number\nSIMPLE_MODEL=wrong-file\n")
    chosen = tmp_path / "chosen.env"
    chosen.write_text("EMBEDDING_DIM=384\nSIMPLE_MODEL=chosen-model\n")
    code = (
        "import sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); "
        "from scripts.start_console import load_settings; "
        "selected=load_settings(Path(sys.argv[2])); "
        "assert selected.embedding_dim == 384; assert selected.simple_model == 'chosen-model'; "
        "print('CUSTOM_ENV_OK')"
    )
    environment = {
        key: value for key, value in os.environ.items() if key not in {"EMBEDDING_DIM", "SIMPLE_MODEL"}
    }
    result = subprocess.run(
        [start_console.sys.executable, "-c", code, str(start_console.ROOT), str(chosen)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == "CUSTOM_ENV_OK"


def test_demo_validation_does_not_require_paid_keys_but_configured_mode_does(tmp_path, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("VOYAGE_API_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_API_KEYS", raising=False)
    path = tmp_path / "custom.env"
    path.write_text("GATEWAY_API_KEYS='" + json.dumps({"k" * 40: "demo"}) + "'\n")
    selected = start_console.load_settings(path)
    start_console.validate_mode(selected, "demo")
    with pytest.raises(start_console.LaunchError, match="incomplete"):
        start_console.validate_mode(selected, "configured")
    assert selected.anthropic_api_key == ""
    assert selected.voyage_api_key == ""


def test_stop_only_signals_owned_child_and_escalates_after_deadline():
    child = Mock()
    child.poll.return_value = None
    child.wait.side_effect = [subprocess.TimeoutExpired("owned-child", 20), None]
    start_console.stop_child(child)
    child.send_signal.assert_called_once_with(signal.SIGINT)
    child.terminate.assert_called_once()
    child.kill.assert_not_called()


def test_no_services_launch_waits_for_readiness_and_closes_owned_child(tmp_path, monkeypatch, capsys):
    args = start_console.parser().parse_args(["--no-services", "--mode", "configured", "--port", "8101"])
    args.env_file = tmp_path / "custom.env"
    calls = []
    for name in ("require_dependencies", "check_port", "ensure_env_file", "validate_mode"):
        monkeypatch.setattr(start_console, name, lambda *a, **k: None)
    monkeypatch.setattr(start_console, "load_settings", lambda _: object())
    monkeypatch.setattr(start_console, "start_services", lambda _: pytest.fail("Docker should not run"))
    child = Mock()
    child.wait.return_value = 0
    monkeypatch.setattr(start_console.subprocess, "Popen", lambda command, **kwargs: child)
    monkeypatch.setattr(start_console, "wait_ready", lambda *args: calls.append("ready"))
    monkeypatch.setattr(start_console, "stop_child", lambda process: calls.append(process))
    monkeypatch.setattr(
        start_console.webbrowser, "open", lambda *a, **k: pytest.fail("Browser opening is opt-in")
    )
    assert start_console.launch(args) == 0
    assert calls == ["ready", child]
    output = capsys.readouterr().out
    assert "http://127.0.0.1:8101/inspector" in output
    assert "CONFIGURED GATEWAY" in output


@pytest.mark.parametrize("failure", [start_console.LaunchError("Not ready"), KeyboardInterrupt()])
def test_failed_or_interrupted_readiness_still_closes_the_launcher_owned_child(monkeypatch, failure):
    args = start_console.parser().parse_args(["--no-services"])
    for name in ("require_dependencies", "check_port", "ensure_env_file", "validate_mode"):
        monkeypatch.setattr(start_console, name, lambda *a, **k: None)
    monkeypatch.setattr(start_console, "load_settings", lambda _: object())
    child = Mock()
    monkeypatch.setattr(start_console.subprocess, "Popen", lambda *a, **k: child)
    monkeypatch.setattr(start_console, "wait_ready", Mock(side_effect=failure))
    stop = Mock()
    monkeypatch.setattr(start_console, "stop_child", stop)
    with pytest.raises(type(failure)):
        start_console.launch(args)
    stop.assert_called_once_with(child)


def test_parent_sigterm_runs_interrupt_path_and_restores_the_previous_handler(monkeypatch, capsys):
    previous = signal.getsignal(signal.SIGTERM)

    def stop_with_sigterm(_args):
        signal.raise_signal(signal.SIGTERM)

    monkeypatch.setattr(start_console, "launch", stop_with_sigterm)
    assert start_console.main(["--no-services"]) == 130
    assert signal.getsignal(signal.SIGTERM) == previous
    assert "Storage services and volumes were left running" in capsys.readouterr().out


def test_readiness_ignores_malformed_responses_and_has_a_deadline(monkeypatch):
    child = Mock()
    child.poll.return_value = None
    response = Mock()
    response.status = 200
    response.read.return_value = b"[]"
    opener = Mock()
    opener.open.return_value.__enter__ = Mock(return_value=response)
    opener.open.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(start_console.urllib.request, "build_opener", lambda *a: opener)
    monkeypatch.setattr(start_console.time, "monotonic", Mock(side_effect=[0.0, 0.0, 2.0]))
    monkeypatch.setattr(start_console.time, "sleep", lambda _: None)
    with pytest.raises(start_console.LaunchError, match="readiness timed out"):
        start_console.wait_ready(child, 8011, 1)


async def test_gateway_root_redirects_to_console_without_embedding_credentials():
    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/")
    assert response.status_code == 307
    assert response.headers["location"] == "/inspector"
    assert response.headers["cache-control"] == "no-store"
