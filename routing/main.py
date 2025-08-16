import logging
import os
import time
import threading
import random
import signal
from typing import Optional

from db.db_connection import fetch_active_clients
from selector import evaluate_and_store_best_route
import reroute  # uses reroute.loop_once()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("main")

# Tunables (env‑overridable)
INITIAL_WAIT = int(os.getenv("ROUTING_INITIAL_WAIT_SECONDS", "24"))
PLANNER_SLEEP = int(os.getenv("ROUTING_PLANNER_SLEEP_SECONDS", "300"))   # 5 min
REROUTE_TICK = int(os.getenv("ROUTING_REROUTE_TICK_SECONDS", "5"))       # 5 s
JOIN_TIMEOUT = int(os.getenv("ROUTING_THREAD_JOIN_TIMEOUT", "15"))       # seconds

_stop = threading.Event()


def process_client(client_id: str):
    try:
        evaluate_and_store_best_route(client_id)
    except Exception as e:
        log.error(f"❌ Routing failed for {client_id}: {e}", exc_info=True)


def planner_loop():
    """Periodic full planning pass over active clients."""
    # small randomized jitter so multiple replicas don't sync‑hammer the DB
    time.sleep(random.uniform(0, 1.5))
    backoff: Optional[int] = None

    while not _stop.is_set():
        try:
            clients = fetch_active_clients()
            if not clients:
                log.info("🔕 No active clients found. Sleeping planner...")
                _stop.wait(PLANNER_SLEEP)
                continue

            log.info(f"👥 Found {len(clients)} active clients. Launching threads.")
            threads = []
            for client_id in clients:
                t = threading.Thread(target=process_client, args=(client_id,), daemon=True)
                t.start()
                threads.append(t)

            for t in threads:
                t.join(timeout=JOIN_TIMEOUT)

            log.info(f"✅ Planner cycle complete. Sleeping {PLANNER_SLEEP}s.")
            _stop.wait(PLANNER_SLEEP)
            backoff = None  # reset backoff on success

        except Exception as e:
            log.error(f"Planner loop error: {e}", exc_info=True)
            # exponential backoff (cap at 60s) to self‑heal
            backoff = 2 if backoff is None else min(backoff * 2, 60)
            _stop.wait(backoff)


def reroute_loop():
    """Fast loop: detect deviation/GTFS shifts and reroute a.s.a.p."""
    # tiny jitter so planner/reroute don’t always align
    time.sleep(random.uniform(0, 0.8))
    backoff: Optional[int] = None

    log.info("🧭 reroute loop started.")
    while not _stop.is_set():
        try:
            reroute.loop_once()  # uses reroute’s thresholds + logic
            _stop.wait(REROUTE_TICK)
            backoff = None
        except Exception as e:
            log.error(f"Reroute loop error: {e}", exc_info=True)
            backoff = 2 if backoff is None else min(backoff * 2, 30)
            _stop.wait(backoff)


def _handle_sigterm(*_):
    log.info("🫡 Received shutdown signal. Stopping loops...")
    _stop.set()


def run_routing_engine():
    log.info("🚦 Routing module initialized.")
    log.info(f"⏳ Waiting {INITIAL_WAIT}s for DB to stabilize...")
    _stop.wait(INITIAL_WAIT)

    # Wire signals for graceful shutdown
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)
    except Exception:
        # Some environments (e.g. Windows) may not permit setting signals
        pass

    planner = threading.Thread(target=planner_loop, name="planner", daemon=True)
    reloop = threading.Thread(target=reroute_loop, name="reroute", daemon=True)

    planner.start()
    reloop.start()

    # Keep the main thread alive while workers run
    while not _stop.is_set():
        _stop.wait(1)

    log.info("🛑 Waiting for worker threads to exit...")
    planner.join(timeout=5)
    reloop.join(timeout=5)
    log.info("✅ Routing module stopped cleanly.")


if __name__ == "__main__":
    run_routing_engine()
