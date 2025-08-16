import os
import time
from gtfs_parser import (
    load_static_gtfs,
    parse_stops, parse_routes, parse_trips,
    parse_stop_times, parse_calendar, parse_calendar_dates
)
from db.db_connection import (
    save_gtfs_stops, save_gtfs_routes, save_gtfs_trips,
    save_gtfs_stop_times, save_gtfs_calendar, save_gtfs_calendar_dates
)

SLEEP_INTERVAL = 26 * 3600  # 26h default interval


def process_all_gtfs_files():
    parsed = load_static_gtfs()

    if parsed is None:
        print("⏩ GTFS feed not modified. Skipping processing.\n")
        return

    success_count = 0
    fail_count = 0

    # 🔄 Each GTFS table process with safety
    for name, parser, saver in [
        ("gtfs_routes", parse_routes, save_gtfs_routes),
        ("gtfs_calendar", parse_calendar, save_gtfs_calendar),
        ("gtfs_calendar_dates", parse_calendar_dates, save_gtfs_calendar_dates),
        ("gtfs_stops", parse_stops, save_gtfs_stops),
        ("gtfs_trips", parse_trips, save_gtfs_trips),
        ("gtfs_stop_times", parse_stop_times, save_gtfs_stop_times),
    ]:
        try:
            print(f"📄 Parsing + saving → {name}")
            saver(parser(parsed[name]))
            success_count += 1
        except Exception as e:
            print(f"❌ {name} failed: {type(e).__name__} → {e}")
            fail_count += 1

    print(f"\n📊 GTFS Load Summary → ✅ {success_count} tables updated, ❌ {fail_count} failed.\n")


def run_static_gtfs_loader():
    print("🚀 Static GTFS module started.")

    if os.getenv("GTFS_STATIC_REFRESH", "true").lower() != "true":
        print("🔁 Static GTFS loop disabled by .env — running once")
        process_all_gtfs_files()
        return

    while True:
        print("🔁 Downloading and processing GTFS zip...")
        try:
            process_all_gtfs_files()
        except Exception as e:
            print(f"❌ Top-level GTFS load failure: {type(e).__name__} → {e}")

        print(f"😴 Sleeping {SLEEP_INTERVAL / 3600:.0f}h...\n")
        time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    run_static_gtfs_loader()
