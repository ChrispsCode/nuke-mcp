"""Tests for NukeConnection.send_command and error handling."""

import pytest

from nukemcp.connection import NukeConnectionError


def test_send_command_success(connection):
    result = connection.send_command("get_script_info")
    assert "name" in result
    assert "fps" in result


def test_send_command_error_raises(connection):
    with pytest.raises(RuntimeError, match="not found"):
        connection.send_command("get_node_info", {"node_name": "nonexistent"})


def test_send_command_unknown_command(connection):
    with pytest.raises(RuntimeError, match="Unknown command"):
        connection.send_command("totally_bogus_command")


def test_send_command_with_params(connection):
    result = connection.send_command("create_node", {"node_class": "Grade", "name": "MyGrade"})
    assert result["name"] == "MyGrade"
    assert result["class"] == "Grade"


def test_send_not_connected():
    from nukemcp.connection import NukeConnection
    conn = NukeConnection()
    with pytest.raises(NukeConnectionError, match="Not connected"):
        conn.send({"type": "ping"})


def _wait_until(predicate, timeout=5.0):
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_no_reconnect_by_default(mock_server, mock_port):
    """Without auto_reconnect, a dropped connection stays dropped."""
    from nukemcp.connection import NukeConnection
    conn = NukeConnection("127.0.0.1", mock_port)
    conn.connect()
    mock_server.stop()
    assert _wait_until(lambda: not conn.is_connected)
    mock_server.start()
    with pytest.raises(NukeConnectionError, match="Not connected"):
        conn.send_command("get_script_info")
    conn.disconnect()


def test_reconnects_after_addon_restart(mock_server, mock_port):
    """Stopping the addon drops the client; the next command reconnects."""
    from nukemcp.connection import NukeConnection
    conn = NukeConnection("127.0.0.1", mock_port, auto_reconnect=True)
    conn.connect()
    assert conn.send_command("get_script_info")["fps"]

    mock_server.stop()
    assert _wait_until(lambda: not conn.is_connected), "stop() should drop the client"

    mock_server.start()
    assert conn.send_command("get_script_info")["fps"]
    assert conn.is_connected
    conn.disconnect()


def test_reconnect_reports_unreachable_addon(mock_port):
    """With nothing listening, reconnect fails fast with a clear message."""
    from nukemcp.connection import NukeConnection
    conn = NukeConnection("127.0.0.1", mock_port, auto_reconnect=True)
    with pytest.raises(NukeConnectionError, match="not reachable"):
        conn.send_command("get_script_info")


def test_connects_when_addon_starts_late(mock_port):
    """A connection created before the addon is up works once it starts."""
    from nukemcp.connection import NukeConnection
    from nukemcp.mock import MockNukeServer
    conn = NukeConnection("127.0.0.1", mock_port, auto_reconnect=True)
    with pytest.raises(NukeConnectionError):
        conn.send_command("get_script_info")

    server = MockNukeServer(port=mock_port)
    server.start()
    try:
        assert conn.send_command("get_script_info")["fps"]
    finally:
        conn.disconnect()
        server.stop()
