"""A small thread-safe progress bar for the CLI, drawn on stderr only when it is a terminal.

Harvest and column costs report work through this interface: `add(n)` grows the
total as list calls reveal it, `advance(n)` marks work done, and `log(line)` prints a
line above the bar. `Quiet` keeps the log lines and draws nothing, for pipes, the UI
and MCP.
"""

from __future__ import annotations

import shutil
import sys
import threading
import time

REDRAW_SECONDS = 0.1  # at most ten redraws a second


class Quiet:
    def __init__(self, stream=None):
        self.stream = stream or sys.stderr
        self.lock = threading.Lock()

    def add(self, n: int = 1) -> None:
        pass

    def advance(self, n: int = 1) -> None:
        pass

    def log(self, line: str) -> None:
        with self.lock:
            print(line, file=self.stream, flush=True)

    def close(self) -> None:
        pass


class Bar(Quiet):
    def __init__(self, label: str, stream=None):
        super().__init__(stream)
        self.label, self.done, self.total = label, 0, 0
        self.started, self.drawn_at = time.monotonic(), 0.0

    def add(self, n: int = 1) -> None:
        with self.lock:
            self.total += n
            self._draw()

    def advance(self, n: int = 1) -> None:
        with self.lock:
            self.done += n
            self._draw(force=self.done >= self.total)

    def log(self, line: str) -> None:
        with self.lock:
            self._clear()
            print(line, file=self.stream)
            self._draw(force=True)

    def close(self) -> None:
        with self.lock:
            self._clear()

    def _clear(self) -> None:
        self.stream.write("\r\033[K")
        self.stream.flush()

    def _draw(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.drawn_at < REDRAW_SECONDS:
            return
        self.drawn_at = now
        total = max(self.total, self.done, 1)
        width = max(10, min(40, shutil.get_terminal_size((80, 20)).columns - 50))
        filled = round(width * self.done / total)
        elapsed = now - self.started
        self.stream.write(f"\r\033[K{self.label} [{'█' * filled}{'·' * (width - filled)}] "
                          f"{self.done}/{self.total} · {elapsed:.0f}s")
        self.stream.flush()


def for_terminal(label: str, stream=None) -> Quiet:
    """A bar when the stream is an interactive terminal, otherwise plain log lines."""
    stream = stream or sys.stderr
    return Bar(label, stream) if stream.isatty() else Quiet(stream)
