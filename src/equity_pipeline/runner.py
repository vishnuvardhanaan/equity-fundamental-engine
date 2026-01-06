import logging
import time

logger = logging.getLogger(__name__)


def run_step(
    name,
    fn,
    *,
    retries=3,
    backoff=2.0,
    on_event=None,
    cancel_event=None,
):
    attempt = 0

    while True:
        if cancel_event and cancel_event.is_set():
            logger.warning("Step %s cancelled before attempt %d", name, attempt + 1)
            raise RuntimeError(f"Step {name} cancelled by user")

        attempt += 1
        start = time.perf_counter()

        logger.info("▶ Starting %s (attempt %d)", name, attempt)
        if on_event:
            on_event("start", name, attempt)

        try:
            fn()

        except Exception as exc:
            elapsed = time.perf_counter() - start

            if cancel_event and cancel_event.is_set():
                logger.warning("Step %s cancelled during attempt %d", name, attempt)
                raise RuntimeError(f"Step {name} cancelled by user")

            if on_event:
                on_event("failure", name, attempt, elapsed)

            logger.warning(
                "⚠ Step %s failed (attempt %d, %.2fs), will retry if attempts remain",
                name,
                attempt,
                elapsed,
            )

            if attempt >= retries:
                logger.exception("✖ Step %s exhausted retries", name)
                raise RuntimeError(
                    f"Step {name} failed after {attempt} attempts"
                ) from exc

            sleep_time = backoff ** (attempt - 1)
            logger.info("↻ Retrying %s in %.1f seconds", name, sleep_time)

            # Check cancel before sleeping
            for _ in range(int(sleep_time * 10)):
                if cancel_event and cancel_event.is_set():
                    logger.warning("Step %s cancelled during backoff", name)
                    raise RuntimeError(f"Step {name} cancelled by user")
                time.sleep(0.1)

        else:
            elapsed = time.perf_counter() - start
            logger.info("✔ Step %s completed (%.2fs)", name, elapsed)
            if on_event:
                on_event("success", name, elapsed)
            return {"attempts": attempt, "duration": elapsed}
