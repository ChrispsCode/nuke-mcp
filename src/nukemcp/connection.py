"""Socket connection to the Nuke addon."""

import json
import logging
import queue
import socket
import threading
import time

log = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 54321
RECV_BUFFER = 1024 * 1024  # 1MB


class NukeConnectionError(Exception):
    """Failed to connect or communicate with the Nuke addon."""


class NukeConnection:
    """Manages a TCP socket connection to the Nuke addon running inside Nuke.

    On connect, reads a handshake message containing Nuke version and variant info.
    Sends JSON commands and receives JSON responses. Supports receiving asynchronous
    event messages pushed by the addon.
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 auto_reconnect: bool = False):
        self.host = host
        self.port = port
        # When True, send() reconnects on demand if the connection is down
        # (Nuke started late, or the addon was stopped/restarted).
        self.auto_reconnect = auto_reconnect
        self.sock: socket.socket | None = None
        self.handshake: dict | None = None
        self._buffer = b""
        self._response_queue: queue.Queue[dict] = queue.Queue()
        self._event_handler: callable | None = None
        self._reader_thread: threading.Thread | None = None
        self._reader_running = False

    def set_event_handler(self, handler):
        """Set a callback for event messages pushed by the addon.

        The handler receives a dict with keys: type, event_type, data.
        """
        self._event_handler = handler

    def connect(self, retries: int = 3, base_delay: float = 1.0) -> dict:
        """Connect to the Nuke addon and return the handshake data.

        Retries with exponential backoff on failure.
        """
        # Close any old socket first (wakes its reader), then wait for the
        # reader to exit so it can't read from or close the new socket.
        self._close_socket()
        self._stop_reader()
        last_error = None
        for attempt in range(retries):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(10.0)
                self.sock.connect((self.host, self.port))
                self._buffer = b""
                self.handshake = self._read_message()
                log.info(
                    "Connected to Nuke %s (%s) at %s:%d",
                    self.handshake.get("nuke_version", "unknown"),
                    self.handshake.get("variant", "unknown"),
                    self.host,
                    self.port,
                )
                # Drop responses left over from a previous connection
                self._clear_responses()
                # Switch to blocking mode and start the reader thread
                self.sock.settimeout(None)
                self._start_reader()
                return self.handshake
            except (OSError, json.JSONDecodeError, TimeoutError) as e:
                last_error = e
                self._close_socket()
                if attempt < retries - 1:
                    delay = base_delay * (2**attempt)
                    log.warning(
                        "Connection attempt %d failed: %s. Retrying in %.1fs...",
                        attempt + 1,
                        e,
                        delay,
                    )
                    time.sleep(delay)

        raise NukeConnectionError(
            f"Failed to connect to Nuke addon at {self.host}:{self.port} "
            f"after {retries} attempts: {last_error}"
        )

    def send(self, command: dict, timeout: float = 10.0) -> dict:
        """Send a JSON command and return the JSON response."""
        if not self.sock and self.auto_reconnect:
            self._reconnect()
        if not self.sock:
            raise NukeConnectionError("Not connected to Nuke addon")

        msg = json.dumps(command) + "\n"
        try:
            self.sock.sendall(msg.encode("utf-8"))
        except OSError as e:
            self._close_socket()
            raise NukeConnectionError(f"Communication error: {e}") from e

        try:
            return self._response_queue.get(timeout=timeout)
        except queue.Empty:
            raise NukeConnectionError(
                f"Timeout waiting for response after {timeout}s"
            ) from None

    def send_command(self, cmd_type: str, params: dict | None = None, timeout: float = 10.0) -> dict:
        """Send a command and return the result, raising on error."""
        response = self.send({"type": cmd_type, "params": params or {}}, timeout=timeout)
        if response["status"] == "error":
            raise RuntimeError(response["error"])
        return response["result"]

    def disconnect(self):
        """Cleanly disconnect from the Nuke addon."""
        self._close_socket()
        self._stop_reader()
        self.handshake = None
        log.info("Disconnected from Nuke addon")

    @property
    def is_connected(self) -> bool:
        return self.sock is not None

    def _reconnect(self):
        """Try once to (re)connect, raising a clear error if the addon isn't up."""
        log.info("Not connected; trying to reconnect to Nuke addon at %s:%d", self.host, self.port)
        try:
            self.connect(retries=1)
        except NukeConnectionError as e:
            raise NukeConnectionError(
                f"Nuke addon not reachable at {self.host}:{self.port}. "
                "Make sure Nuke is running and the NukeMCP server is started "
                f"in its panel, then try again. ({e})"
            ) from None

    def _clear_responses(self):
        while True:
            try:
                self._response_queue.get_nowait()
            except queue.Empty:
                return

    def _start_reader(self):
        """Start the background thread that reads all incoming messages."""
        self._reader_running = True
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

    def _stop_reader(self):
        """Stop the reader thread."""
        self._reader_running = False
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        self._reader_thread = None

    def _reader_loop(self):
        """Background loop: read messages and route events vs responses."""
        while self._reader_running:
            try:
                msg = self._read_message()
            except (NukeConnectionError, OSError):
                break
            if msg.get("type") == "event":
                if self._event_handler:
                    try:
                        self._event_handler(msg)
                    except Exception:
                        log.exception("Event handler error")
            else:
                self._response_queue.put(msg)

    def _read_message(self) -> dict:
        """Read a newline-delimited JSON message from the socket."""
        while b"\n" not in self._buffer:
            sock = self.sock
            if sock is None:
                raise NukeConnectionError("Not connected to Nuke addon")
            chunk = sock.recv(RECV_BUFFER)
            if not chunk:
                self._close_socket(only=sock)
                raise NukeConnectionError("Connection closed by Nuke addon")
            self._buffer += chunk

        line, self._buffer = self._buffer.split(b"\n", 1)
        return json.loads(line.decode("utf-8"))

    def _close_socket(self, only: socket.socket | None = None):
        """Close the current socket. With `only`, close it only if it is still current."""
        sock = self.sock
        if sock is None or (only is not None and sock is not only):
            return
        self.sock = None
        try:
            # Wake a reader thread blocked in recv() on this socket
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass
