from __future__ import annotations

import logging
import threading
import time

log = logging.getLogger("services.command_worker")


class TelegramCommandWorker:
    def __init__(self, command_service_factory, poll_interval_sec: int = 2):
        self.command_service_factory = command_service_factory
        self.poll_interval_sec = max(1, int(poll_interval_sec))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="telegram-command-worker", daemon=True)
        self._thread.start()
        log.info("Telegram command worker started")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        commands_service = self.command_service_factory()
        while not self._stop_event.is_set():
            try:
                commands_service.poll_once()
            except Exception:
                log.exception("Telegram command polling failed")
            self._stop_event.wait(self.poll_interval_sec)
