-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Drop existing tables
DROP TABLE IF EXISTS "mqtt_sessions" CASCADE;
DROP TABLE IF EXISTS "geodata" CASCADE;
DROP TABLE IF EXISTS "user_patterns" CASCADE;
DROP TABLE IF EXISTS "pois" CASCADE;
DROP TABLE IF EXISTS "optimized_routes" CASCADE;
DROP TABLE IF EXISTS "trajectories" CASCADE;
DROP TABLE IF EXISTS "hotspots" CASCADE;
DROP TABLE IF EXISTS "predicted_pois" CASCADE;
DROP TABLE IF EXISTS "predicted_pois_sequence" CASCADE;

-- MQTT sessions
CREATE TABLE mqtt_sessions (
                               "id" SERIAL PRIMARY KEY,  -- internal ID for DB management
                               "session_id" INTEGER GENERATED ALWAYS AS IDENTITY UNIQUE,  -- trusted session_id used throughout UrbanOS
                               "client_id" TEXT NOT NULL,
                               "start_time" TIMESTAMP DEFAULT NOW(),
                               "end_time" TIMESTAMP DEFAULT (NOW() + INTERVAL '26 hours'),
                               UNIQUE (client_id, start_time)
);

-- Incoming geodata (real-time)
CREATE TABLE geodata (
                         "id" SERIAL PRIMARY KEY,
                         "session_id" INTEGER NOT NULL REFERENCES "mqtt_sessions" (session_id) ON DELETE CASCADE,
                         "client_id" TEXT NOT NULL,
                         "lat" FLOAT NOT NULL,
                         "lon" FLOAT NOT NULL,
                         "elevation" FLOAT,
                         "speed" FLOAT,
                         "activity" VARCHAR(50),
                         "timestamp" TIMESTAMP DEFAULT NOW(),
                         "geom" GEOMETRY(Point, 4326),
                         "updated_at" TIMESTAMP DEFAULT NOW()
);

-- Long-term storage of client movement
CREATE TABLE "trajectories" (
                                "id" SERIAL PRIMARY KEY,
                                "client_id" TEXT NOT NULL,
                                "session_id" INTEGER NOT NULL,
                                "trajectory" JSONB NOT NULL,
                                "created_at" TIMESTAMP DEFAULT NOW(),
                                CONSTRAINT "unique_session_id" UNIQUE (session_id)
);

-- Routes from A*
CREATE TABLE IF NOT EXISTS "astar_routes" (
                                              id SERIAL PRIMARY KEY,
                                              client_id TEXT NOT NULL,
                                              stop_id TEXT,  -- ✅ NEW (was site_id)
                                              parent_station TEXT,  -- ✅ NEW (was stop_area_id)
                                              target_type TEXT CHECK (target_type IN ('poi', 'stop_point')) NOT NULL,
                                              poi_id INTEGER,
                                              origin_lat FLOAT NOT NULL,
                                              origin_lon FLOAT NOT NULL,
                                              destination_lat FLOAT NOT NULL,
                                              destination_lon FLOAT NOT NULL,
                                              path GEOMETRY(LineString, 4326) NOT NULL,
                                              distance FLOAT NOT NULL,
                                              efficiency_score FLOAT,
                                              decision_context TEXT CHECK (
                                                  decision_context IN (
                                                                       'initial_prediction', 'routed_to_poi', 'routed_to_departure',
                                                                       'deviation_detected', 'fallback_astar', 'fallback_stop_point', 'rerouted_mapf'
                                                      )
                                                  ) DEFAULT 'initial_prediction',
                                              predicted_eta TIMESTAMP,
                                              created_at TIMESTAMP DEFAULT NOW()
);


-- MAPF routing results
CREATE TABLE IF NOT EXISTS "mapf_routes" (
                                             "id" SERIAL PRIMARY KEY,
                                             "client_id" TEXT NOT NULL,
                                             "stop_id" TEXT,
                                             "destination_lat" DOUBLE PRECISION NOT NULL,
                                             "destination_lon" DOUBLE PRECISION NOT NULL,
                                             "path" GEOMETRY(LineString, 4326),
                                             "distance" DOUBLE PRECISION GENERATED ALWAYS AS (
                                                      ST_LengthSpheroid(path, 'SPHEROID["WGS 84",6378137,298.257223563]')
                                                      ) STORED,
                                             "success" BOOLEAN DEFAULT TRUE,
                                             "decision_context" TEXT CHECK (
                                                 "decision_context" IN (
                                                                        'initial_prediction',
                                                                        'routed_to_poi',
                                                                        'routed_to_departure',
                                                                        'deviation_detected',
                                                                        'fallback_astar',
                                                                        'rerouted_mapf',
                                                                        'mapf_predicted'  -- ✅ Added new valid option
                                                     )
                                                 ) DEFAULT 'initial_prediction',
                                             "created_at" TIMESTAMP DEFAULT NOW()
);

-- Stop targets.
CREATE TABLE IF NOT EXISTS "astar_stop_targets" (
                                                    "id" SERIAL PRIMARY KEY,
                                                    "client_id" TEXT NOT NULL,
                                                    "stop_lat" FLOAT NOT NULL,
                                                    "stop_lon" FLOAT NOT NULL,
                                                    "eta_seconds" INTEGER NOT NULL,
                                                    "created_at" TIMESTAMP DEFAULT NOW()
);

-- User location patterns
CREATE TABLE "user_patterns" (
                                 "pattern_id" SERIAL PRIMARY KEY,
                                 "client_id" TEXT NOT NULL,
                                 "lat" FLOAT NOT NULL,
                                 "lon" FLOAT NOT NULL,
                                 "pattern_type" VARCHAR(50),
                                 "geom" GEOMETRY(LineString, 4326),
                                 "timestamp" TIMESTAMP DEFAULT NOW()
);

-- Points of Interest
CREATE TABLE "pois" (
                        "poi_id" SERIAL PRIMARY KEY,
                        "client_id" TEXT NOT NULL,
                        "lat" FLOAT NOT NULL,
                        "lon" FLOAT NOT NULL,
                        "geom" GEOMETRY(Point, 4326) NOT NULL,
                        "time_spent" FLOAT NOT NULL,
                        "poi_rank" FLOAT NOT NULL,
                        "source" VARCHAR(50) DEFAULT 'detected',
                        "visit_start" TIMESTAMP,
                        "visit_count" INTEGER DEFAULT 0,
                        "created_at" TIMESTAMP DEFAULT NOW()
);

-- Area zones
CREATE TABLE "hotspots" (
                            "hotspot_id" SERIAL PRIMARY KEY,
                            "client_id" TEXT NOT NULL,
                            "lat" FLOAT NOT NULL,
                            "lon" FLOAT NOT NULL,
                            "radius" FLOAT NOT NULL,
                            "density" FLOAT,
                            "type" VARCHAR(50),
                            "time_spent" FLOAT,
                            "source_type" TEXT DEFAULT 'trajectory',
                            "geom" GEOMETRY(Point, 4326) NOT NULL,
                            "created_at" TIMESTAMP DEFAULT NOW(),
                            "updated_at" TIMESTAMP DEFAULT NOW(),
                            CONSTRAINT "unique_hotspot" UNIQUE ("client_id", "lat", "lon")
);

-- Predicted POI visit times
CREATE TABLE "predicted_pois_sequence" (
                                           "id" SERIAL PRIMARY KEY,
                                           "client_id" TEXT NOT NULL,
                                           "predicted_lat" FLOAT NOT NULL,
                                           "predicted_lon" FLOAT NOT NULL,
                                           "predicted_visit_time" TIMESTAMP NOT NULL,
                                           "prediction_type" VARCHAR(10) CHECK ("prediction_type" IN ('daily', 'weekly')),
                                           "geom" GEOMETRY(Point, 4326) NOT NULL,
                                           "poi_rank" FLOAT,
                                           "time_spent" FLOAT,
                                           "created_at" TIMESTAMP DEFAULT NOW(),
                                           UNIQUE ("client_id", "predicted_visit_time")
);

-- GTFS Static
CREATE TABLE IF NOT EXISTS "gtfs_routes" (
                                             "route_id" TEXT PRIMARY KEY,
                                             "agency_id" TEXT,
                                             "route_short_name" TEXT,
                                             "route_long_name" TEXT,
                                             "route_type" INTEGER,
                                             "route_desc" TEXT,
                                             "geom" GEOMETRY(LineString, 4326)
);


CREATE TABLE IF NOT EXISTS "gtfs_calendar" (
                                               "service_id" TEXT PRIMARY KEY,
                                               "monday" INTEGER,
                                               "tuesday" INTEGER,
                                               "wednesday" INTEGER,
                                               "thursday" INTEGER,
                                               "friday" INTEGER,
                                               "saturday" INTEGER,
                                               "sunday" INTEGER,
                                               "start_date" DATE,
                                               "end_date" DATE
);


CREATE TABLE IF NOT EXISTS "gtfs_calendar_dates" (
                                                     "service_id" TEXT REFERENCES "gtfs_calendar" ("service_id"),
                                                     "date" DATE,
                                                     "exception_type" INTEGER
);


CREATE TABLE IF NOT EXISTS "gtfs_stops" (
                                            "stop_id" TEXT PRIMARY KEY,
                                            "stop_code" TEXT,
                                            "stop_name" TEXT,
                                            "stop_desc" TEXT,
                                            "stop_lat" DOUBLE PRECISION,
                                            "stop_lon" DOUBLE PRECISION,
                                            "zone_id" TEXT,
                                            "stop_url" TEXT,
                                            "location_type" INTEGER,
                                            "parent_station" TEXT,
                                            "stop_timezone" TEXT,
                                            "wheelchair_boarding" INTEGER,
                                            "platform_code" TEXT,
                                            "geom" GEOMETRY(Point, 4326)
);


CREATE TABLE IF NOT EXISTS "gtfs_trips" (
                                            "trip_id" TEXT PRIMARY KEY,
                                            "route_id" TEXT REFERENCES "gtfs_routes" ("route_id"),
                                            "service_id" TEXT REFERENCES "gtfs_calendar" ("service_id"),
                                            "trip_headsign" TEXT,
                                            "direction_id" INTEGER,
                                            "shape_id" TEXT,
                                            "geom" GEOMETRY(LineString, 4326)
);


CREATE TABLE IF NOT EXISTS "gtfs_stop_times" (
                                                 "trip_id" TEXT REFERENCES "gtfs_trips" ("trip_id"),
                                                 "arrival_time" TEXT,
                                                 "departure_time" TEXT,
                                                 "stop_id" TEXT,  -- Optional FK
                                                 "stop_sequence" INTEGER,
                                                 "stop_headsign" TEXT,
                                                 "pickup_type" INTEGER,
                                                 "drop_off_type" INTEGER,
                                                 "shape_dist_traveled" FLOAT,
                                                 "timepoint" INTEGER,
                                                 "pickup_booking_rule_id" TEXT,
                                                 "drop_off_booking_rule_id" TEXT,
                                                 PRIMARY KEY ("trip_id", "stop_sequence")
);

-- GTFS-RT
CREATE TABLE IF NOT EXISTS vehicle_positions (
                                                 vehicle_id TEXT NOT NULL,
                                                 trip_id TEXT,
                                                 route_id TEXT,
                                                 stop_id TEXT,
                                                 lat DOUBLE PRECISION,
                                                 lon DOUBLE PRECISION,
                                                 speed REAL,
                                                 bearing REAL,
                                                 timestamp TIMESTAMP,
                                                 created_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS trip_updates (
                                            trip_id TEXT NOT NULL,
                                            stop_id TEXT,
                                            arrival_time TIMESTAMP,
                                            departure_time TIMESTAMP,
                                            delay_seconds INTEGER,
                                            status TEXT,
                                            created_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS service_alerts (
                                              alert_id TEXT PRIMARY KEY,
                                              cause TEXT,
                                              effect TEXT,
                                              header_text TEXT,
                                              description_text TEXT,
                                              affected_entity TEXT,
                                              start_time TIMESTAMP,
                                              end_time TIMESTAMP,
                                              created_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS "vehicle_arrivals" (
                                                  "vehicle_id" TEXT,
                                                  "trip_id" TEXT,
                                                  "route_id" TEXT,
                                                  "position_lat" FLOAT,
                                                  "position_lon" FLOAT,
                                                  "stop_id" TEXT,
                                                  "timestamp" TIMESTAMP,
                                                  "created_at" TIMESTAMP DEFAULT NOW()
);

-- Create optimized_routes
CREATE TABLE "optimized_routes" (
                                    "session_id" INTEGER,
                                    "client_id" TEXT NOT NULL,
                                    "stop_id" TEXT,
                                    "origin_lat" FLOAT NOT NULL,
                                    "origin_lon" FLOAT NOT NULL,
                                    "destination_lat" FLOAT NOT NULL,
                                    "destination_lon" FLOAT NOT NULL,
                                    "path" GEOMETRY(LineString, 4326) NOT NULL,
                                    "segment_type" VARCHAR(50) DEFAULT 'unknown',
                                    "created_at" TIMESTAMP DEFAULT NOW(),
                                    "is_valid" BOOLEAN DEFAULT TRUE,
                                    "is_chosen" BOOLEAN DEFAULT TRUE,
                                    PRIMARY KEY ("client_id", "stop_id", "segment_type")
);

-- Switch profile.
CREATE TABLE IF NOT EXISTS "client_switch_profiles" (
                                                        "client_id" TEXT NOT NULL,
                                                        "stop_id" TEXT NOT NULL,
                                                        "avg_switch_seconds" INTEGER NOT NULL,
                                                        "last_updated" TIMESTAMP DEFAULT NOW(),
                                                        PRIMARY KEY ("client_id", "stop_id")
);

-- client weekly schedule
CREATE TABLE IF NOT EXISTS "client_weekly_schedule" (
                                                        "id" SERIAL PRIMARY KEY,
                                                        "client_id" TEXT NOT NULL,
                                                        "visit_day" TEXT NOT NULL,
                                                        "predicted_time" TIMESTAMP NOT NULL,
                                                        "poi_lat" FLOAT,
                                                        "poi_lon" FLOAT,
                                                        "prediction_type" TEXT CHECK ("prediction_type" IN ('weekly', 'daily')) DEFAULT 'weekly',
                                                        "path" GEOMETRY(LineString, 4326),
                                                        "segment_type" TEXT,
                                                        "created_at" TIMESTAMP DEFAULT NOW()
);

-- Reroutes are append-only history of “we changed our mind”
CREATE TABLE IF NOT EXISTS reroutes (
                                        id BIGSERIAL PRIMARY KEY,
                                        client_id TEXT NOT NULL,
                                        stop_id TEXT,                            -- may be NULL for direct
                                        origin_lat FLOAT,
                                        origin_lon FLOAT,
                                        destination_lat FLOAT NOT NULL,
                                        destination_lon FLOAT NOT NULL,
                                        path GEOMETRY(LineString, 4326),         -- optional if we only log the decision
                                        segment_type VARCHAR(50) DEFAULT 'unknown',   -- 'direct' | 'multimodal' | 'fallback' ...
                                        reason TEXT,                              -- e.g. 'off_path_63m', 'delay_220s', 'departure_passed'
                                        previous_stop_id TEXT,
                                        previous_segment_type TEXT,
                                        is_chosen BOOLEAN DEFAULT TRUE,          -- mirrors optimized_routes shape
                                        created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes (spatial and performance)
CREATE INDEX IF NOT EXISTS "optimized_routes_path_idx" ON "optimized_routes" USING GIST ("path");
CREATE INDEX IF NOT EXISTS "trajectory_idx" ON "trajectories" USING GIN ("trajectory" jsonb_path_ops);
CREATE INDEX IF NOT EXISTS "optimized_routes_client_idx" ON "optimized_routes" ("client_id");
-- Pragmatic indexes
CREATE INDEX IF NOT EXISTS "reroutes_client_idx" ON "reroutes" ("client_id", "created_at" DESC);
CREATE INDEX IF NOT EXISTS "reroutes_dest_idx" ON "reroutes" ("destination_lat", "destination_lon");
CREATE INDEX IF NOT EXISTS "reroutes_geom_idx" ON "reroutes" USING GIST ("path");
-- Geodata table indexes
CREATE INDEX IF NOT EXISTS "geodata_mqtt_idx" ON "geodata" ("session_id");
CREATE INDEX IF NOT EXISTS "geodata_client_idx" ON "geodata" ("client_id");
CREATE INDEX IF NOT EXISTS "geodata_timestamp_idx" ON "geodata" ("timestamp");
CREATE INDEX IF NOT EXISTS "geodata_geom_idx" ON "geodata" USING GIST ("geom");
-- POI indexes
CREATE INDEX IF NOT EXISTS "pois_geom_idx" ON "pois" USING GIST ("geom");
CREATE INDEX IF NOT EXISTS "pois_client_idx" ON "pois" ("client_id");
CREATE INDEX IF NOT EXISTS "pois_client_lat_lon_idx" ON "pois" ("client_id", "lat", "lon");
CREATE INDEX IF NOT EXISTS "pois_rank_idx" ON "pois" ("poi_rank");
-- Hotspot and prediction indexes
CREATE INDEX IF NOT EXISTS "hotspots_geom_idx" ON "hotspots" USING GIST ("geom");
CREATE INDEX IF NOT EXISTS "hotspots_client_idx" ON "hotspots" ("client_id");
CREATE INDEX IF NOT EXISTS "hotspots_type_idx" ON "hotspots" ("type");
CREATE INDEX IF NOT EXISTS "hotspots_source_type_idx" ON "hotspots" ("source_type");

CREATE INDEX IF NOT EXISTS "pois_sequence_client_idx" ON "predicted_pois_sequence" ("client_id");
CREATE INDEX IF NOT EXISTS "predicted_pois_sequence_time_idx" ON "predicted_pois_sequence" ("predicted_visit_time");
CREATE INDEX IF NOT EXISTS "predicted_pois_sequence_type_idx" ON "predicted_pois_sequence" ("prediction_type");
-- Routing indexes
CREATE INDEX IF NOT EXISTS "astar_routes_stop_id_idx" ON astar_routes ("stop_id");
CREATE INDEX IF NOT EXISTS "astar_routes_client_idx" ON "astar_routes" ("client_id");
CREATE INDEX IF NOT EXISTS idx_astar_stop_eta ON astar_routes (stop_id, predicted_eta);
CREATE INDEX IF NOT EXISTS idx_trip_updates_stop_trip ON trip_updates (stop_id, trip_id, departure_time);

CREATE INDEX IF NOT EXISTS "astar_routes_eta_idx" ON "astar_routes" ("distance");
CREATE INDEX IF NOT EXISTS "astar_routes_geom_idx" ON "astar_routes" USING GIST ("path");
CREATE INDEX IF NOT EXISTS "mapf_routes_client_idx" ON "mapf_routes" ("client_id");
CREATE INDEX IF NOT EXISTS "mapf_routes_coords_idx" ON "mapf_routes" ("destination_lat", "destination_lon");
CREATE INDEX IF NOT EXISTS "trajectories_created_idx" ON "trajectories" ("created_at");
CREATE INDEX IF NOT EXISTS "optimized_routes_segment_idx" ON "optimized_routes" ("segment_type");
CREATE INDEX IF NOT EXISTS "reroutes_segment_idx" ON "reroutes" ("segment_type");
CREATE INDEX IF NOT EXISTS "geodata_client_time_idx" ON "geodata" ("client_id","timestamp" DESC, "updated_at" DESC);
CREATE INDEX IF NOT EXISTS "mqtt_sessions_client_bounds_idx" ON "mqtt_sessions" ("client_id","start_time","end_time");
CREATE INDEX IF NOT EXISTS "optimized_routes_client_time_idx" ON "optimized_routes" ("client_id","created_at" DESC);
CREATE INDEX IF NOT EXISTS "reroutes_client_time_idx" ON "reroutes" ("client_id","created_at" DESC);


CREATE OR REPLACE VIEW "view_routing_candidates_gtfsrt" AS
SELECT
    ar.client_id,
    ar.stop_id,
    ar.predicted_eta,
    va.trip_id,
    va.route_id,
    tu.arrival_time,
    tu.departure_time,
    tu.delay_seconds
FROM astar_routes ar
         JOIN vehicle_arrivals va
              ON ar.stop_id = va.stop_id
         JOIN trip_updates tu
              ON va.trip_id = tu.trip_id
WHERE ar.predicted_eta IS NOT NULL;


CREATE OR REPLACE VIEW "lines" AS
SELECT
    route_id AS line_id,
    CASE
        WHEN route_type = 0 THEN 'tram'
        WHEN route_type = 1 THEN 'subway'
        WHEN route_type = 2 THEN 'rail'
        WHEN route_type = 3 THEN 'bus'
        WHEN route_type = 4 THEN 'ferry'
        ELSE 'unknown'
        END AS transport_type,
    to_jsonb(gtfs_routes.*) AS content
FROM gtfs_routes;


CREATE OR REPLACE VIEW "view_static_gtfs_unified" AS
SELECT
    s.stop_id AS stop_point_id,
    s.stop_name AS stop_point_name,
    s.stop_lat,
    s.stop_lon,
    s.zone_id,
    s.platform_code,
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    t.trip_id,
    t.direction_id,
    st.stop_sequence,
    st.arrival_time,
    st.departure_time
FROM
    gtfs_stops s
        JOIN
    gtfs_stop_times st ON s.stop_id = st.stop_id
        JOIN
    gtfs_trips t ON st.trip_id = t.trip_id
        JOIN
    gtfs_routes r ON t.route_id = r.route_id;


-- Active Clients view.
CREATE OR REPLACE VIEW "view_active_clients_geodata" AS
SELECT client_id
FROM (
         SELECT client_id,
                MAX(timestamp) AS last_ts,
                MAX(updated_at) AS last_seen
         FROM geodata
         GROUP BY client_id
     ) sub
WHERE last_ts >= NOW() - INTERVAL '26 hours'
   OR last_seen >= NOW() - INTERVAL '2 seconds';


CREATE OR REPLACE VIEW "view_current_session_id_from_geodata" AS
SELECT DISTINCT ON (g."client_id")
    g."client_id",
    g."session_id"
FROM "geodata" AS g
ORDER BY g."client_id", g."timestamp" DESC, g."updated_at" DESC;



CREATE OR REPLACE VIEW "view_astar_eta" AS
SELECT
    ar.client_id,
    ar.destination_lat,
    ar.destination_lon,
    ar.distance AS route_distance,
    gd.speed AS current_speed,
    ar.created_at,
    CASE
        WHEN gd.speed > 0 THEN ar.created_at + (ar.distance / gd.speed) * INTERVAL '1 second'
        ELSE NULL
        END AS estimated_eta
FROM astar_routes ar
         JOIN LATERAL (
    SELECT speed
    FROM geodata
    WHERE geodata.client_id = ar.client_id
    ORDER BY timestamp DESC
    LIMIT 1
    ) gd ON true;


CREATE OR REPLACE VIEW view_top_daily_poi AS
SELECT DISTINCT ON ("client_id") *
FROM "predicted_pois_sequence"
WHERE "prediction_type" = 'daily'
ORDER BY "client_id", "predicted_visit_time" ASC;


-- Combined POIs for routing
CREATE OR REPLACE VIEW view_combined_pois AS
-- stable POIs (no predicted time)
SELECT
    p.client_id,
    p.lat,
    p.lon,
    p.poi_rank,
    NULL::FLOAT AS time_spent,
    p.geom,
    'stable' AS poi_type,
    p.created_at,
    NULL::timestamp AS predicted_visit_time
FROM pois p

UNION ALL

-- predicted sequence (daily/weekly)
SELECT
    s.client_id,
    s.predicted_lat  AS lat,
    s.predicted_lon  AS lon,
    0.5              AS poi_rank,
    NULL::FLOAT      AS time_spent,
    s.geom,
    ('predicted_' || s.prediction_type) AS poi_type,
    s.created_at,
    s.predicted_visit_time
FROM predicted_pois_sequence s;


CREATE OR REPLACE VIEW "view_hotspots_heatmap" AS
SELECT
    hotspot_id,
    client_id,
    lat,
    lon,
    radius,
    density,
    type,
    source_type,
    time_spent,
    created_at,
    updated_at,
    ST_AsGeoJSON(geom)::json AS geojson
FROM hotspots;


CREATE OR REPLACE VIEW view_latest_client_trajectories AS
SELECT *
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) AS rn
         FROM trajectories
     ) sub
WHERE rn <= 8;


CREATE OR REPLACE VIEW view_departure_candidates AS
SELECT
    ar.client_id,
    ar.predicted_eta,
    ar.target_type,
    ar.stop_id,
    ar.parent_station,
    ar.decision_context,

    tu.trip_id,
    tu.departure_time,
    tu.arrival_time,
    tu.delay_seconds,
    tu.status,

    gt.route_id,
    gt.direction_id,
    gt.trip_headsign
FROM astar_routes ar
         JOIN trip_updates tu
              ON ar.stop_id = tu.stop_id
         JOIN gtfs_trips gt
              ON tu.trip_id = gt.trip_id
WHERE
    ar.target_type = 'stop_point'
  AND ar.predicted_eta IS NOT NULL
  AND tu.departure_time >= (ar.predicted_eta + INTERVAL '40 seconds')
  AND tu.departure_time <= (ar.predicted_eta + INTERVAL '90 seconds');


CREATE OR REPLACE VIEW "view_predicted_routes_schedule" AS
SELECT
    p."client_id",
    p."predicted_visit_time",
    p."predicted_lat",
    p."predicted_lon",
    p."prediction_type",
    COALESCE(m."path", a."path") AS "route_path",
    COALESCE(m."decision_context", a."decision_context") AS "decision_context",
    COALESCE(m."created_at", a."created_at") AS "route_created_at"
FROM "predicted_pois_sequence" p
         LEFT JOIN "mapf_routes" m
                   ON p."client_id" = m."client_id"
                       AND p."predicted_lat" = m."destination_lat"
                       AND p."predicted_lon" = m."destination_lon"
         LEFT JOIN "astar_routes" a
                   ON p."client_id" = a."client_id"
                       AND p."predicted_lat" = a."destination_lat"
                       AND p."predicted_lon" = a."destination_lon"
WHERE p."predicted_visit_time" >= NOW()
ORDER BY p."client_id", p."predicted_visit_time";


CREATE OR REPLACE VIEW "view_hotspot_overlay" AS
SELECT
    h.client_id,
    h.lat AS hotspot_lat,
    h.lon AS hotspot_lon,
    h.radius,
    h.density,
    h.source_type,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    ST_Distance(
            ST_SetSRID(ST_MakePoint(h.lon, h.lat), 4326),
            ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326)
    ) AS meters_to_stop
FROM hotspots h
         LEFT JOIN gtfs_stops s
                   ON ST_DWithin(
                           ST_SetSRID(ST_MakePoint(h.lon, h.lat), 4326),
                           ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326),
                           300
                      )
WHERE h.created_at >= NOW() - INTERVAL '24 hours';


CREATE OR REPLACE VIEW "view_latest_client_trajectories" AS
SELECT *
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) AS rn
         FROM trajectories
     ) sub
WHERE rn <= 8;


CREATE OR REPLACE VIEW "view_daily_routing_summary" AS
SELECT
    "client_id",
    "destination_lat",
    "destination_lon",
    "created_at",
    'MAPF' AS "method"
FROM "mapf_routes"
UNION
SELECT
    "client_id",
    "destination_lat",
    "destination_lon",
    "created_at",
    'A*' AS "method"
FROM "astar_routes";


CREATE OR REPLACE VIEW "view_mapf_active_routes" AS
SELECT *
FROM (
         SELECT
             mr.*,
             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) AS rn
         FROM mapf_routes mr
         WHERE mr.success = TRUE
           AND mr.created_at >= NOW() - INTERVAL '1 hour'
     ) sub
WHERE rn = 1;

-- Flat history with a 'source' tag
CREATE OR REPLACE VIEW view_routes_history AS
SELECT
    'optimized'::text AS source,
    client_id, stop_id, origin_lat, origin_lon,
    destination_lat, destination_lon, path,
    segment_type, is_chosen, created_at, NULL::text AS reason,
    NULL::text AS previous_stop_id, NULL::text AS previous_segment_type
FROM optimized_routes
UNION ALL
SELECT
    'reroute'::text AS source,
    client_id, stop_id, origin_lat, origin_lon,
    destination_lat, destination_lon, path,
    segment_type, is_chosen, created_at, reason,
    previous_stop_id, previous_segment_type
FROM reroutes;

-- “Latest view” per (client_id, stop_id, segment_type, destination)
-- Prefers the most recent record across both tables
CREATE OR REPLACE VIEW view_routes_unified_latest AS
SELECT *
FROM (
         SELECT
             h.*,
             ROW_NUMBER() OVER (
                 PARTITION BY client_id, COALESCE(stop_id,'∅'), segment_type,
                     destination_lat, destination_lon
                 ORDER BY created_at DESC
                 ) AS rn
         FROM view_routes_history h
     ) ranked
WHERE rn = 1;

-- “Live chosen per client” (one row per client, last decision)
CREATE OR REPLACE VIEW view_routes_live AS
SELECT *
FROM (
         SELECT
             h.*,
             ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY created_at DESC) AS rn
         FROM view_routes_history h
         WHERE is_chosen = TRUE
     ) x
WHERE rn = 1;

-- Optimized routes and re-routes
CREATE OR REPLACE VIEW view_routes_unified AS
SELECT
    client_id,
    stop_id,
    destination_lat,
    destination_lon,
    path,
    segment_type,
    created_at
FROM (
         SELECT
             r.*,
             ROW_NUMBER() OVER (
                 PARTITION BY client_id, destination_lat, destination_lon, COALESCE(segment_type,'')
                 ORDER BY created_at DESC
                 ) AS rn
         FROM (
                  SELECT client_id, stop_id, destination_lat, destination_lon, path, segment_type, created_at
                  FROM optimized_routes
                  WHERE is_valid = TRUE

                  UNION ALL

                  SELECT client_id, stop_id, destination_lat, destination_lon, path, segment_type, created_at
                  FROM reroutes
              ) r
     ) x
WHERE rn = 1;

-- POIs ⟷ GTFS stops (nearest + within radius)
CREATE OR REPLACE VIEW "view_pois_nearest_stop" AS
SELECT
    p."poi_id",
    p."client_id",
    p."lat",
    p."lon",
    p."geom"        AS poi_geom,
    p."poi_rank",
    p."time_spent",
    p."source",
    p."visit_start",
    p."visit_count",
    p."created_at"  AS poi_created_at,
    s."stop_id",
    s."stop_name",
    s."parent_station",
    s."platform_code",
    s."stop_lat",
    s."stop_lon",
    s."geom"        AS stop_geom,
    ST_DistanceSphere(p."geom", s."geom")::float AS meters_to_stop
FROM "pois" p
         LEFT JOIN LATERAL (
    SELECT
        gs."stop_id", gs."stop_name", gs."parent_station", gs."platform_code",
        gs."stop_lat", gs."stop_lon", gs."geom"
    FROM "gtfs_stops" gs
    ORDER BY p."geom" <-> gs."geom"   -- KNN, uses GiST on gtfs_stops.geom if present
    LIMIT 1
    ) s ON TRUE;


CREATE OR REPLACE VIEW "view_pois_stops_within_300m" AS
SELECT
    p."poi_id",
    p."client_id",
    p."lat",
    p."lon",
    p."geom"        AS poi_geom,
    p."poi_rank",
    p."time_spent",
    s."stop_id",
    s."stop_name",
    s."parent_station",
    s."platform_code",
    s."stop_lat",
    s."stop_lon",
    s."geom"        AS stop_geom,
    ST_DistanceSphere(p."geom", s."geom")::float AS meters_to_stop
FROM "pois" p
         JOIN "gtfs_stops" s
              ON ST_DWithin(p."geom"::geography, s."geom"::geography, 300);


-- 2) Latest routes (optimized + reroutes) per client, like view_latest_client_trajectories
CREATE OR REPLACE VIEW "view_latest_client_routes" AS
SELECT *
FROM (
         SELECT
             h.*,
             ROW_NUMBER() OVER (PARTITION BY h."client_id" ORDER BY h."created_at" DESC) AS rn
         FROM "view_routes_history" h
     ) sub
WHERE rn <= 8;



-- A* and MAPF unified
CREATE OR REPLACE VIEW "view_routes_astar_mapf_unified" AS
SELECT
    ar."client_id",
    ar."stop_id",
    ar."destination_lat",
    ar."destination_lon",
    ar."path",
    ar."distance"::double precision          AS distance_meters,
    ar."decision_context",
    ar."predicted_eta",
    TRUE                                     AS success,
    'astar'::text                            AS method,
    ar."created_at"
FROM "astar_routes" ar

UNION ALL

SELECT
    mr."client_id",
    mr."stop_id",
    mr."destination_lat",
    mr."destination_lon",
    mr."path",
    mr."distance"::double precision          AS distance_meters,
    mr."decision_context",
    NULL::timestamp                          AS predicted_eta,
    mr."success",
    'mapf'::text                             AS method,
    mr."created_at"
FROM "mapf_routes" mr;


-- Latest (one) per client across A*+MAPF
CREATE OR REPLACE VIEW "view_routes_astar_mapf_latest" AS
SELECT *
FROM (
         SELECT
             u.*,
             ROW_NUMBER() OVER (PARTITION BY u."client_id" ORDER BY u."created_at" DESC) AS rn
         FROM "view_routes_astar_mapf_unified" u
     ) x
WHERE rn = 1;


-- ETA accuracy vs actual departure (error in seconds, + means ETA was early)
--   Matches A* predicted_eta with nearest departure at same stop within ±5 minutes.
CREATE OR REPLACE VIEW "view_eta_accuracy_seconds" AS
WITH eta_base AS (
    SELECT
        ar."client_id",
        ar."stop_id",
        ar."predicted_eta",
        ar."created_at" AS route_created_at
    FROM "astar_routes" ar
    WHERE ar."predicted_eta" IS NOT NULL
),
     nearest_departure AS (
         SELECT
             e."client_id",
             e."stop_id",
             e."predicted_eta",
             tu."departure_time",
             tu."trip_id",
             tu."delay_seconds",
             tu."status",
             ROW_NUMBER() OVER (
                 PARTITION BY e."client_id", e."stop_id", e."predicted_eta"
                 ORDER BY ABS(EXTRACT(EPOCH FROM (tu."departure_time" - e."predicted_eta"))) ASC
                 ) AS rn
         FROM eta_base e
                  JOIN "trip_updates" tu
                       ON tu."stop_id" = e."stop_id"
                           AND tu."departure_time" BETWEEN (e."predicted_eta" - INTERVAL '5 minutes')
                              AND (e."predicted_eta" + INTERVAL '5 minutes')
     )
SELECT
    n."client_id",
    n."stop_id",
    n."trip_id",
    n."predicted_eta",
    n."departure_time",
    COALESCE(n."delay_seconds", 0) AS delay_seconds,
    n."status",
    (EXTRACT(EPOCH FROM (n."departure_time" - n."predicted_eta"))::int) AS eta_error_seconds
FROM nearest_departure n
WHERE n.rn = 1;


-- Per-client boarding-window hits (40–90s window), last 24h
CREATE OR REPLACE VIEW "view_boarding_window_hit_rate" AS
WITH candidates AS (
    SELECT
        d."client_id",
        d."stop_id",
        d."trip_id",
        d."departure_time",
        d."predicted_eta",
        (d."departure_time" BETWEEN (d."predicted_eta" + INTERVAL '40 seconds')
            AND     (d."predicted_eta" + INTERVAL '90 seconds')) AS hit
    FROM "view_departure_candidates" d
    WHERE d."departure_time" >= NOW() - INTERVAL '24 hours'
)
SELECT
    "client_id",
    COUNT(*)                       AS total_candidates,
    SUM(CASE WHEN hit THEN 1 ELSE 0 END) AS hits,
    ROUND(100.0 * SUM(CASE WHEN hit THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS hit_rate_pct
FROM candidates
GROUP BY "client_id";


-- Latest live position per client (point geom)
CREATE OR REPLACE VIEW "view_geodata_latest_point" AS
SELECT
    g."client_id",
    g."session_id",
    g."lat",
    g."lon",
    COALESCE(g."geom", ST_SetSRID(ST_MakePoint(g."lon", g."lat"), 4326)) AS geom,
    g."speed",
    g."activity",
    g."timestamp",
    g."updated_at"
FROM (
         SELECT
             gg.*,
             ROW_NUMBER() OVER (PARTITION BY gg."client_id" ORDER BY gg."timestamp" DESC, gg."updated_at" DESC) AS rn
         FROM "geodata" gg
     ) g
WHERE g.rn = 1;


-- Stop usage by client (last 7 days)
CREATE OR REPLACE VIEW "view_stop_usage_7d" AS
SELECT
    h."client_id",
    COALESCE(h."stop_id", '∅') AS stop_id,
    COUNT(*)                   AS route_count_7d,
    MIN(h."created_at")        AS first_seen_7d,
    MAX(h."created_at")        AS last_seen_7d
FROM "view_routes_history" h
WHERE h."created_at" >= NOW() - INTERVAL '7 days'
GROUP BY h."client_id", COALESCE(h."stop_id", '∅');


-- Predicted POIs → nearest stops (prep for schedule/departure lookups)
CREATE OR REPLACE VIEW "view_predicted_poi_nearest_stop" AS
SELECT
    p."id"               AS predicted_id,
    p."client_id",
    p."predicted_visit_time",
    p."prediction_type",
    p."predicted_lat",
    p."predicted_lon",
    p."geom"             AS predicted_geom,
    s."stop_id",
    s."stop_name",
    s."parent_station",
    s."platform_code",
    s."geom"             AS stop_geom,
    ST_DistanceSphere(p."geom", s."geom")::float AS meters_to_stop
FROM "predicted_pois_sequence" p
         LEFT JOIN LATERAL (
    SELECT gs.*
    FROM "gtfs_stops" gs
    ORDER BY p."geom" <-> gs."geom"
    LIMIT 1
    ) s ON TRUE;


-- Weekly plan joined with POI labels + nearest stop
CREATE OR REPLACE VIEW "view_client_weekly_schedule_enriched" AS
SELECT
    w."id",
    w."client_id",
    w."visit_day",
    w."predicted_time",
    w."prediction_type",
    w."poi_lat",
    w."poi_lon",
    w."path",
    w."segment_type",
    w."created_at",
    pn."stop_id"         AS nearest_stop_id,
    pn."stop_name"       AS nearest_stop_name,
    pn."meters_to_stop"  AS poi_to_stop_meters
FROM "client_weekly_schedule" w
         LEFT JOIN LATERAL (
    SELECT
        gs."stop_id",
        gs."stop_name",
        ST_DistanceSphere(
                ST_SetSRID(ST_MakePoint(w."poi_lon", w."poi_lat"), 4326),
                gs."geom"
        )::float AS meters_to_stop
    FROM "gtfs_stops" gs
    ORDER BY ST_SetSRID(ST_MakePoint(w."poi_lon", w."poi_lat"), 4326) <-> gs."geom"
    LIMIT 1
    ) pn ON TRUE;


-- “Feasible next departures per client” (one best per client right now)
CREATE OR REPLACE VIEW "view_next_feasible_departure_per_client" AS
SELECT *
FROM (
         SELECT
             d.*,
             ROW_NUMBER() OVER (PARTITION BY d."client_id" ORDER BY d."departure_time" ASC) AS rn
         FROM "view_departure_candidates" d
         WHERE d."departure_time" >= NOW()
     ) q
WHERE rn = 1;

