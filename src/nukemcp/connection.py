"""Socket connection to the Nuke addon."""

import itertools
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
CONNECT_TIMEOUT = 10.0  # seconds to connect and receive the handshake

# Queued by the reader thread when its connection ends, so a send() waiting
# for a response fails straight away instead of sitting out its timeout.
_DISCONNECTED = "_disconnected"


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
        # Each request carries an id that the addon echoes back, so a late
        # response to a timed-out request is never taken as the answer to the
        # next one.
        self._request_ids = itertools.count(1)
        # Bumped on every connect, so a disconnect notice from an old
        # connection's reader can't fail a request on the new one.
        self._generation = 0
        # One request in flight at a time: responses share one queue.
        self._send_lock = threading.Lock()

    def set_event_handler(self, handler):
        """Set a callback for event messages pushed by the addon.

        The handler receives a dict with keys: type, event_type, data.
        """
        self._event_handler = handler

    def connect(self, retries: int = 3, base_delay: float = 1.0,
                timeout: float = CONNECT_TIMEOUT) -> dict:
        """Connect to the Nuke addon and return the handshake data.

        Retries with exponential backoff on failure. `timeout` bounds each
        attempt, including the wait for the handshake (which needs Nuke's main
        thread, so a busy Nuke can hold it up).
        """
        # Close any old socket first (wakes its reader), then wait for the
        # reader to exit so it can't read from or close the new socket.
        self._close_socket()
        self._stop_reader()
        last_error = None
        for attempt in range(retries):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(timeout)
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
        with self._send_lock:
            if not self.sock and self.auto_reconnect:
                self._reconnect()
            sock = self.sock
            if not sock:
                raise NukeConnectionError("Not connected to Nuke addon")

            generation = self._generation
            request_id = next(self._request_ids)
            # Anything still queued answers an earlier request that timed out.
            self._clear_responses()
            msg = json.dumps({**command, "id": request_id}) + "\n"
            try:
                sock.sendall(msg.encode("utf-8"))
            except OSError as e:
                self._close_socket(only=sock)
                raise NukeConnectionError(f"Communication error: {e}") from e

            return self._wait_for_response(request_id, generation, command, timeout)

    def _wait_for_response(self, request_id: int, generation: int,
                           command: dict, timeout: float) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            try:
                if remaining <= 0:
                    raise queue.Empty
                response = self._response_queue.get(timeout=remaining)
            except queue.Empty:
                raise NukeConnectionError(
                    f"Timeout waiting for response after {timeout}s. Nuke may "
                    "still be busy running the command."
                ) from None

            if response.get("type") == _DISCONNECTED:
                if response.get("generation") != generation:
                    continue  # Notice from an older connection
                raise NukeConnectionError(
                    f"Connection to Nuke addon lost while waiting for "
                    f"'{command.get('type')}': {response.get('error')}"
                )
            response_id = response.get("id")
            # An addon from before request ids doesn't echo them; take its
            # responses as they come.
            if response_id is None or response_id == request_id:
                return response
            log.warning(
                "Discarding stale response to request %s (waiting for %s)",
                response_id, request_id,
            )

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
        self._generation += 1
        self._reader_running = True
        self._reader_thread = threading.Thread(
            target=self._reader_loop, args=(self.sock, self._generation), daemon=True,
        )
        self._reader_thread.start()

    def _stop_reader(self):
        """Stop the reader thread."""
        self._reader_running = False
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        self._reader_thread = None

    def _reader_loop(self, sock: socket.socket, generation: int):
        """Background loop: read messages and route events vs responses.

        However the loop ends, it closes its socket, so the next send()
        reconnects instead of writing to a connection nobody is reading.
        """
        reason = "connection closed"
        try:
            while self._reader_running:
                msg = self._read_message(sock)
                if msg.get("type") == "event":
                    if self._event_handler:
                        try:
                            self._event_handler(msg)
                        except Exception:
                            log.exception("Event handler error")
                else:
                    self._response_queue.put(msg)
        except (NukeConnectionError, OSError) as e:
            reason = str(e) or reason
        except Exception as e:
            # A malformed message means the stream can't be trusted any more
            log.exception("Reader thread failed; dropping the connection")
            reason = f"{type(e).__name__}: {e}"
        finally:
            self._close_socket(only=sock)
            self._response_queue.put(
                {"type": _DISCONNECTED, "generation": generation, "error": reason}
            )

    def _read_message(self, sock: socket.socket | None = None) -> dict:
        """Read a newline-delimited JSON message from `sock` (default: the current socket)."""
        while b"\n" not in self._buffer:
            if sock is None:
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
