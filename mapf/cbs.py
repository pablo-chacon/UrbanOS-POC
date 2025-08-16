import logging
import random
import time as timer
import heapq
from db.db_connection import fetch_astar_path  # 🧠 new helper for path loading

logging.basicConfig(level=logging.INFO)


class CBSSolver:
    def __init__(self, client_id, goals, get_sum_of_cost, max_time=None):
        """
        Initialize CBSSolver to fetch precomputed A* paths from DB instead of calling A* directly.
        """
        self.client_id = client_id
        self.goals = goals
        self.get_sum_of_cost = get_sum_of_cost
        self.max_time = max_time if max_time else float("inf")
        self.start_time = timer.time()
        self.open_list = []
        self.num_of_generated = 0
        self.num_of_expanded = 0

    def push_node(self, node):
        heapq.heappush(self.open_list, (node["cost"], len(node["collisions"]), self.num_of_generated, node))
        self.num_of_generated += 1

    def pop_node(self):
        _, _, _, node = heapq.heappop(self.open_list)
        self.num_of_expanded += 1
        return node

    def find_solution(self):
        """Use precomputed A* path from database to build CBS-compatible response."""
        root = {"cost": 0, "constraints": [], "paths": [], "collisions": []}

        for goal in self.goals:
            path = fetch_astar_path(self.client_id, goal)
            if path is None:
                raise Exception(f"❌ No A* path found for {self.client_id} to {goal}")
            root["paths"].append(path)

        root["cost"] = self.get_sum_of_cost(root["paths"])
        root["collisions"] = []  # CBS logic can be added if needed
        self.push_node(root)

        while self.open_list and timer.time() - self.start_time < self.max_time:
            node = self.pop_node()
            if not node["collisions"]:
                return node["paths"]
            # Collision resolution is skipped here for UrbanOS simplicity
        raise Exception("❌ MAPF failed or timed out")
