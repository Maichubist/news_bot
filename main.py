import logging
import time

from dotenv import load_dotenv

from app.bootstrap import build_runtime
from app.logging_setup import setup_logging
from app.services.command_worker import TelegramCommandWorker

log = logging.getLogger("main")


def main() -> None:
    load_dotenv()

    runtime = build_runtime("config.yaml")
    setup_logging(runtime.cfg.app.log_level)

    command_worker = TelegramCommandWorker(
        command_service_factory=runtime.command_service_factory,
        poll_interval_sec=runtime.cfg.monitor.command_poll_seconds,
    )
    command_worker.start()

    every = runtime.cfg.monitor.every_seconds
    next_pipeline_run = 0.0

    try:
        while True:
            now = time.monotonic()
            if now >= next_pipeline_run:
                t0 = time.time()
                try:
                    runtime.pipeline.run_once()
                except Exception as ex:
                    log.exception("Run failed: %s", ex)

                try:
                    runtime.analytics_service.maybe_send_daily_report()
                except Exception as ex:
                    log.exception("Analytics report failed: %s", ex)

                dt = time.time() - t0
                next_pipeline_run = time.monotonic() + max(1, every - dt)

            time.sleep(1)
    finally:
        command_worker.stop()


if __name__ == "__main__":
    main()
