import os
import heapq
import logging
from geopandas import GeoDataFrame
from geopy.distance import geodesic
from shapely.geometry import Point
from osmnx.graph import graph_from_bbox
import osmnx as ox

ox.settings.use_cache = True
ox.settings.cache_folder = "/app/osm_cache"
ox.settings.timeout = 300

os.makedirs(ox.settings.cache_folder, exist_ok=True)
logging.basicConfig(level=logging.INFO)


def a_star(start, goal, graph):
    open_list = []
    heapq.heappush(open_list, (0, start))
    came_from = {}
    g_score = {node: float("inf") for node in graph.nodes}
    g_score[start] = 0
    f_score = {node: float("inf") for node in graph.nodes}
    f_score[start] = heuristic(start, goal, graph)

    while open_list:
        _, current = heapq.heappop(open_list)
        if current == goal:
            return reconstruct_path(came_from, current)

        for neighbor in graph.neighbors(current):
            edge_length = graph.edges[current, neighbor, 0].get(
                "length", calculate_edge_length(graph, current, neighbor)
            )
            tentative_g_score = g_score[current] + edge_length
            if tentative_g_score < g_score[neighbor]:
                came_from[neighbor] = current
                g_score[neighbor] = tentative_g_score
                f_score[neighbor] = g_score[neighbor] + heuristic(neighbor, goal, graph)
                if neighbor not in [i[1] for i in open_list]:
                    heapq.heappush(open_list, (f_score[neighbor], neighbor))

    logging.warning("⚠ No valid path found.")
    return None


def heuristic(node, goal, graph):
    try:
        node_coords = (graph.nodes[node]["y"], graph.nodes[node]["x"])
        goal_coords = (graph.nodes[goal]["y"], graph.nodes[goal]["x"])
        return geodesic(node_coords, goal_coords).meters
    except KeyError:
        logging.error(f"❌ Invalid node: {node} or goal: {goal} in graph.")
        return float("inf")


def calculate_edge_length(graph, node1, node2):
    try:
        coords_1 = (graph.nodes[node1]["y"], graph.nodes[node1]["x"])
        coords_2 = (graph.nodes[node2]["y"], graph.nodes[node2]["x"])
        return geodesic(coords_1, coords_2).meters
    except KeyError:
        logging.error(f"❌ Missing node coordinates for edge {node1}-{node2}.")
        return float("inf")


def reconstruct_path(came_from, current):
    total_path = [current]
    while current in came_from:
        current = came_from[current]
        total_path.append(current)
    total_path.reverse()
    return total_path


def compute_dynamic_bbox(points, buffer=0.01):
    """
    Compute a dynamic bounding box from a list of (lat, lon) points with buffer.
    Returns: (west, south, east, north)
    """
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    north = max(lats) + buffer
    south = min(lats) - buffer
    east = max(lons) + buffer
    west = min(lons) - buffer
    return (west, south, east, north)


def pathfinder(client_id, goal_lat, goal_lon, latest_location=None):
    if not latest_location:
        logging.error(f"❌ No start location for client_id {client_id}. Cannot compute path.")
        return None

    # ✅ Safely unpack with allowance for extra values like speed, timestamp
    start_lat, start_lon, *_ = latest_location

    try:
        points = [(start_lat, start_lon), (goal_lat, goal_lon)]
        bbox = compute_dynamic_bbox(points)
        G = graph_from_bbox(bbox=bbox, network_type="all", simplify=True)

        start_node = ox.distance.nearest_nodes(G, start_lon, start_lat)
        goal_node = ox.distance.nearest_nodes(G, goal_lon, goal_lat)
    except Exception as e:
        logging.error(f"❌ Error creating graph or finding nodes: {e}")
        return None

    route = a_star(start_node, goal_node, G)
    if not route:
        logging.warning(f"⚠ No valid route found for client_id {client_id}.")
        return None

    total_distance = sum([calculate_edge_length(G, route[i], route[i + 1]) for i in range(len(route) - 1)])
    avg_speed = 1.4  # m/s
    total_duration = total_distance / avg_speed

    route_coords = [(G.nodes[node]["x"], G.nodes[node]["y"]) for node in route]
    geometry = [Point(lon, lat) for lon, lat in route_coords]
    gdf = GeoDataFrame(
        {
            "lat": [lat for lon, lat in route_coords],
            "lon": [lon for lon, lat in route_coords],
            "distance": total_distance,
            "duration": total_duration,
        },
        geometry=geometry,
        crs="EPSG:4326",
    )

    logging.info(f"✅ Route computed for client_id {client_id}: {total_distance:.2f} meters.")
    return gdf
