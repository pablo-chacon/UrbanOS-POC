from flask import Flask, jsonify
from db.db_connection import (
    fetch_astar_routes,
    fetch_trajectories,
    fetch_pois,
    fetch_predicted_pois_sequence,
    fetch_hotspots,
    fetch_user_patterns,
    fetch_view_eta_active_points,
    fetch_view_latest_client_trajectories,
    fetch_view_daily_routing_summary,
    fetch_view_top_daily_poi,
    fetch_view_mapf_active_routes,
    fetch_view_astar_eta,
    fetch_lines, fetch_mapf_routes, fetch_stop_points
)

app = Flask(__name__)


@app.route("/api/astar_routes", methods=["GET"])
def astar_routes():
    return jsonify(fetch_astar_routes())


@app.route("/api/trajectories", methods=["GET"])
def trajectories():
    return jsonify(fetch_trajectories())


@app.route("/api/pois", methods=["GET"])
def pois():
    return jsonify(fetch_pois())


@app.route("/api/predicted_pois_sequence", methods=["GET"])
def predicted_pois_sequence():
    return jsonify(fetch_predicted_pois_sequence())


@app.route("/api/hotspots", methods=["GET"])
def hotspots():
    return jsonify(fetch_hotspots())


@app.route("/api/user_patterns", methods=["GET"])
def user_patterns():
    return jsonify(fetch_user_patterns())


@app.route("/api/view_eta_active_points", methods=["GET"])
def view_eta_active_points():
    return jsonify(fetch_view_eta_active_points())


@app.route("/api/view_latest_client_trajectories", methods=["GET"])
def view_latest_client_trajectories():
    return jsonify(fetch_view_latest_client_trajectories())


@app.route("/api/view_daily_routing_summary", methods=["GET"])
def view_daily_routing_summary():
    return jsonify(fetch_view_daily_routing_summary())


@app.route("/api/view_predicted_routes_schedule", methods=["GET"])
def view_predicted_routes_schedule():
    return jsonify(fetch_view_top_daily_poi())


@app.route("/api/view_top_daily_poi", methods=["GET"])
def view_top_daily_poi():
    return jsonify(fetch_view_top_daily_poi())


@app.route("/api/view_astar_eta", methods=["GET"])
def view_astar_eta():
    return jsonify(fetch_view_astar_eta())


@app.route("/api/mapf_routes", methods=["GET"])
def mapf_routes():
    return jsonify(fetch_mapf_routes())


@app.route("/api/view_mapf_active_routes", methods=["GET"])
def view_mapf_active_routes():
    return jsonify(fetch_view_mapf_active_routes())


@app.route("/api/view_best_route_per_pattern_client", methods=["GET"])
def view_best_route_per_pattern_client():
    return jsonify(fetch_lines())


@app.route("/api/stop_points", methods=["GET"])
def stop_points():
    return jsonify(fetch_stop_points())


@app.route("/api/lines", methods=["GET"])
def lines():
    return jsonify(fetch_lines())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8181)
