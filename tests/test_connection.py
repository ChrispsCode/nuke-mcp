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


# ---------------------------------------------------------------------------
# Several clients, stale responses, dead readers, bounded startup
# ---------------------------------------------------------------------------

class _ScriptedAddon:
    """A bare addon stand-in whose replies a test controls line by line.

    `reply(command)` returns the bytes to send back (or None for no reply).
    """

    def __init__(self, port, reply, handshake=True):
        import socket
        import threading
        self.reply = reply
        self.handshake = handshake
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", port))
        self.sock.listen(5)
        self.clients = []
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        import json
        import threading
        while True:
            try:
                client, _ = self.sock.accept()
            except OSError:
                return
            self.clients.append(client)
            threading.Thread(target=self._client, args=(client, json), daemon=True).start()

    def _client(self, client, json):
        if self.handshake:
            client.sendall(b'{"type": "handshake", "nuke_version": "17.1v1", "variant": "NukeX"}\n')
        buf = b""
        while True:
            try:
                chunk = client.recv(65536)
            except OSError:
                return
            if not chunk:
                return
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                out = self.reply(json.loads(line))
                if out:
                    try:
                        client.sendall(out)
                    except OSError:
                        return

    def close(self):
        import socket
        for s in [self.sock, *self.clients]:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            s.close()


def test_two_clients_connected_at_once(mock_server, mock_port):
    """An idle connection must not lock other clients out (one per Claude session)."""
    from nukemcp.connection import NukeConnection
    first = NukeConnection("127.0.0.1", mock_port)
    second = NukeConnection("127.0.0.1", mock_port)
    first.connect()
    try:
        second.connect(retries=1, timeout=2.0)
        assert second.send_command("get_script_info")["fps"]
        assert first.send_command("get_script_info")["fps"]
    finally:
        first.disconnect()
        second.disconnect()


def test_events_reach_every_client(mock_server, mock_port):
    from nukemcp.connection import NukeConnection
    seen = {"a": [], "b": []}
    conns = {}
    for key, events in seen.items():
        conn = NukeConnection("127.0.0.1", mock_port)
        conn.set_event_handler(events.append)
        conn.connect()
        conns[key] = conn
    try:
        mock_server.push_event("node_created", {"name": "Grade1"})
        assert _wait_until(lambda: seen["a"] and seen["b"])
    finally:
        for conn in conns.values():
            conn.disconnect()


def test_late_response_is_not_taken_by_next_request(mock_port):
    """After a timeout, the late reply must be dropped, not returned to the next call."""
    import json
    import time

    from nukemcp.connection import NukeConnection, NukeConnectionError

    def reply(cmd):
        if cmd["params"].get("slow"):
            time.sleep(0.5)
        return (json.dumps({"status": "ok", "result": cmd["params"]["n"], "id": cmd["id"]}) + "\n").encode()

    addon = _ScriptedAddon(mock_port, reply)
    conn = NukeConnection("127.0.0.1", mock_port)
    conn.connect()
    try:
        with pytest.raises(NukeConnectionError, match="Timeout"):
            conn.send_command("x", {"n": 1, "slow": True}, timeout=0.2)
        assert conn.send_command("x", {"n": 2}) == 2
        assert conn.send_command("x", {"n": 3}) == 3
    finally:
        conn.disconnect()
        addon.close()


def test_works_with_addon_that_does_not_echo_ids(mock_port):
    """An addon from before request ids (not yet restarted) still works."""
    from nukemcp.connection import NukeConnection
    addon = _ScriptedAddon(mock_port, lambda cmd: b'{"status": "ok", "result": "pong"}\n')
    conn = NukeConnection("127.0.0.1", mock_port)
    conn.connect()
    try:
        assert conn.send_command("ping") == "pong"
        assert conn.send_command("ping") == "pong"
    finally:
        conn.disconnect()
        addon.close()


def test_malformed_message_drops_connection_and_next_call_reconnects(mock_port):
    """A reader that dies must close the socket, not leave a dead connection behind."""
    import json

    from nukemcp.connection import NukeConnection, NukeConnectionError
    calls = []

    def reply(cmd):
        calls.append(cmd["type"])
        if len(calls) == 1:
            return b"this is not json\n"
        return (json.dumps({"status": "ok", "result": "pong", "id": cmd["id"]}) + "\n").encode()

    addon = _ScriptedAddon(mock_port, reply)
    conn = NukeConnection("127.0.0.1", mock_port, auto_reconnect=True)
    conn.connect()
    try:
        with pytest.raises(NukeConnectionError, match="lost"):
            # Fails as soon as the reader dies, not after the full timeout
            conn.send_command("ping", timeout=30.0)
        assert _wait_until(lambda: not conn.is_connected)
        assert conn.send_command("ping") == "pong"
    finally:
        conn.disconnect()
        addon.close()


def test_startup_connect_is_bounded(mock_port, monkeypatch):
    """A listener that never sends a handshake must not stall server startup."""
    import time

    import nukemcp.server as server_mod
    monkeypatch.setattr(server_mod, "STARTUP_CONNECT_TIMEOUT", 0.5)
    addon = _ScriptedAddon(mock_port, lambda cmd: None, handshake=False)
    try:
        start = time.monotonic()
        server_mod.build_server(port=mock_port)
        assert time.monotonic() - start < 3.0
    finally:
        addon.close()
