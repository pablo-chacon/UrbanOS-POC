import logging
import time
from planner import run_weekly_planner

logging.basicConfig(level=logging.INFO)


def main():
    counter = 0
    while True:
        try:
            logging.info(f"🧠 Running daily planner... (iteration {counter + 1}/7)")
            run_weekly_planner(prediction_type="daily")

            counter += 1

            if counter >= 7:
                logging.info("🔁 7 daily iterations reached. Triggering weekly planner.")
                run_weekly_planner(prediction_type="weekly")
                counter = 0  # reset

        except Exception as e:
            logging.error(f"❌ Planner error: {e}", exc_info=True)

        time.sleep(24 * 3600)  # Wait 24h
