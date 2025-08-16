from flask import Flask, jsonify
from db.db_connection import (
    # tables
    fetch_astar_routes, fetch_mapf_routes, fetch_trajectories, fetch_pois,
    fetch_predicted_pois_sequence, fetch_hotspots, fetch_user_patterns,
    fetch_stop_points, fetch_lines,

    # existing views
    fetch_view_routing_candidates_gtfsrt, fetch_view_static_gtfs_unified,
    fetch_view_active_clients_geodata, fetch_view_current_session_id_from_geodata,
    fetch_view_astar_eta, fetch_view_top_daily_poi, fetch_view_combined_pois,
    fetch_view_hotspots_heatmap, fetch_view_latest_client_trajectories,
    fetch_view_departure_candidates, fetch_view_predicted_routes_schedule,
    fetch_view_hotspot_overlay, fetch_view_daily_routing_summary,
    fetch_view_mapf_active_routes, fetch_view_routes_history,
    fetch_view_routes_unified_latest, fetch_view_routes_live, fetch_view_routes_unified,
    fetch_view_eta_active_points,

    # new views
    fetch_view_pois_nearest_stop, fetch_view_pois_stops_within_300m,
    fetch_view_latest_client_routes, fetch_view_latest_routes_and_trajectories,
    fetch_view_routes_astar_mapf_unified, fetch_view_routes_astar_mapf_latest,
    fetch_view_eta_accuracy_seconds, fetch_view_boarding_window_hit_rate,
    fetch_view_geodata_latest_point, fetch_view_stop_usage_7d,
    fetch_view_predicted_poi_nearest_stop, fetch_view_client_weekly_schedule_enriched,
    fetch_view_next_feasible_departure_per_client
)

app = Flask(__name__)


@app.route("/api/astar_routes", methods=["GET"])
def astar_routes():
    return jsonify(fetch_astar_routes())


@app.route("/api/mapf_routes", methods=["GET"])
def mapf_routes():
    return jsonify(fetch_mapf_routes())


@app.route("/api/trajectories", methods=["GET"])
def trajectories():
    return jsonify(fetch_trajectories())


@app.route("/api/pois", methods=["GET"])
def pois(): return jsonify(fetch_pois())


@app.route("/api/predicted_pois_sequence", methods=["GET"])
def predicted_pois_sequence():
    return jsonify(fetch_predicted_pois_sequence())


@app.route("/api/hotspots", methods=["GET"])
def hotspots():
    return jsonify(fetch_hotspots())


@app.route("/api/user_patterns", methods=["GET"])
def user_patterns():
    return jsonify(fetch_user_patterns())


@app.route("/api/stop_points", methods=["GET"])
def stop_points():
    return jsonify(fetch_stop_points())


@app.route("/api/lines", methods=["GET"])
def lines():
    return jsonify(fetch_lines())


# ----- Existing Views -----
@app.route("/api/view_routing_candidates_gtfsrt", methods=["GET"])
def view_routing_candidates_gtfsrt():
    return jsonify(fetch_view_routing_candidates_gtfsrt())


@app.route("/api/view_static_gtfs_unified", methods=["GET"])
def view_static_gtfs_unified():
    return jsonify(fetch_view_static_gtfs_unified())


@app.route("/api/view_active_clients_geodata", methods=["GET"])
def view_active_clients_geodata():
    return jsonify(fetch_view_active_clients_geodata())


@app.route("/api/view_current_session_id_from_geodata", methods=["GET"])
def view_current_session_id_from_geodata():
    return jsonify(fetch_view_current_session_id_from_geodata())


@app.route("/api/view_astar_eta", methods=["GET"])
def view_astar_eta():
    return jsonify(fetch_view_astar_eta())


@app.route("/api/view_top_daily_poi", methods=["GET"])
def view_top_daily_poi():
    return jsonify(fetch_view_top_daily_poi())


@app.route("/api/view_combined_pois", methods=["GET"])
def view_combined_pois():
    return jsonify(fetch_view_combined_pois())


@app.route("/api/view_hotspots_heatmap", methods=["GET"])
def view_hotspots_heatmap():
    return jsonify(fetch_view_hotspots_heatmap())


@app.route("/api/view_latest_client_trajectories", methods=["GET"])
def view_latest_client_trajectories():
    return jsonify(fetch_view_latest_client_trajectories())


@app.route("/api/view_departure_candidates", methods=["GET"])
def view_departure_candidates(): return jsonify(fetch_view_departure_candidates())


@app.route("/api/view_predicted_routes_schedule", methods=["GET"])
def view_predicted_routes_schedule():  # 🔧 fixed: previously returned top_daily_poi by mistake
    return jsonify(fetch_view_predicted_routes_schedule())


@app.route("/api/view_hotspot_overlay", methods=["GET"])
def view_hotspot_overlay():
    return jsonify(fetch_view_hotspot_overlay())


@app.route("/api/view_daily_routing_summary", methods=["GET"])
def view_daily_routing_summary():
    return jsonify(fetch_view_daily_routing_summary())


@app.route("/api/view_mapf_active_routes", methods=["GET"])
def view_mapf_active_routes(): return jsonify(fetch_view_mapf_active_routes())


@app.route("/api/view_routes_history", methods=["GET"])
def view_routes_history():
    return jsonify(fetch_view_routes_history())


@app.route("/api/view_routes_unified_latest", methods=["GET"])
def view_routes_unified_latest():
    return jsonify(fetch_view_routes_unified_latest())


@app.route("/api/view_routes_live", methods=["GET"])
def view_routes_live():
    return jsonify(fetch_view_routes_live())


@app.route("/api/view_routes_unified", methods=["GET"])
def view_routes_unified():
    return jsonify(fetch_view_routes_unified())


@app.route("/api/view_eta_active_points", methods=["GET"])
def view_eta_active_points():
    return jsonify(fetch_view_eta_active_points())


@app.route("/api/view_pois_nearest_stop", methods=["GET"])
def view_pois_nearest_stop():
    return jsonify(fetch_view_pois_nearest_stop())


@app.route("/api/view_pois_stops_within_300m", methods=["GET"])
def view_pois_stops_within_300m():
    return jsonify(fetch_view_pois_stops_within_300m())


@app.route("/api/view_latest_client_routes", methods=["GET"])
def view_latest_client_routes():
    return jsonify(fetch_view_latest_client_routes())


@app.route("/api/view_latest_routes_and_trajectories", methods=["GET"])
def view_latest_routes_and_trajectories():
    return jsonify(fetch_view_latest_routes_and_trajectories())


@app.route("/api/view_routes_astar_mapf_unified", methods=["GET"])
def view_routes_astar_mapf_unified():
    return jsonify(fetch_view_routes_astar_mapf_unified())


@app.route("/api/view_routes_astar_mapf_latest", methods=["GET"])
def view_routes_astar_mapf_latest():
    return jsonify(fetch_view_routes_astar_mapf_latest())


@app.route("/api/view_eta_accuracy_seconds", methods=["GET"])
def view_eta_accuracy_seconds():
    return jsonify(fetch_view_eta_accuracy_seconds())


@app.route("/api/view_boarding_window_hit_rate", methods=["GET"])
def view_boarding_window_hit_rate():
    return jsonify(fetch_view_boarding_window_hit_rate())


@app.route("/api/view_geodata_latest_point", methods=["GET"])
def view_geodata_latest_point():
    return jsonify(fetch_view_geodata_latest_point())


@app.route("/api/view_stop_usage_7d", methods=["GET"])
def view_stop_usage_7d():
    return jsonify(fetch_view_stop_usage_7d())


@app.route("/api/view_predicted_poi_nearest_stop", methods=["GET"])
def view_predicted_poi_nearest_stop():
    return jsonify(fetch_view_predicted_poi_nearest_stop())


@app.route("/api/view_client_weekly_schedule_enriched", methods=["GET"])
def view_client_weekly_schedule_enriched():
    return jsonify(fetch_view_client_weekly_schedule_enriched())


@app.route("/api/view_next_feasible_departure_per_client", methods=["GET"])
def view_next_feasible_departure_per_client():
    return jsonify(fetch_view_next_feasible_departure_per_client())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8181)
