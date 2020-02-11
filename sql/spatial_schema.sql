CREATE SCHEMA IF NOT EXISTS public;
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
SET SEARCH_PATH TO "public";

CREATE TABLE IF NOT EXISTS Hour
(
  time_key     SERIAL PRIMARY KEY,
  time         TIME    NOT NULL,
  date         DATE    NOT NULL,
  day_of_week  INT     NOT NULL CHECK (day_of_week >= 0 AND day_of_week <= 6),
  month        INT     NOT NULL CHECK (month >= 1 AND month <= 12),
  year         INT     NOT NULL CHECK (year >= 2013 AND year <= 2017),
  weekend      BOOLEAN NOT NULL,
  holiday      BOOLEAN NOT NULL,
  holiday_name TEXT CHECK (holiday_name != '')
);

CREATE TABLE IF NOT EXISTS Location
(
  location_key  SERIAL PRIMARY KEY,
  latitude      FLOAT NOT NULL CHECK (latitude >= -90 AND latitude <= 90),
  longitude     FLOAT NOT NULL CHECK ( longitude >= -180 AND longitude <= 180),
  road_name     TEXT  NOT NULL CHECK (road_name != ''),
  intersection1 TEXT  NOT NULL CHECK (intersection1 != ''),
  intersection2 TEXT,
  neighbourhood TEXT  NOT NULL CHECK (neighbourhood != ''),
  geom          GEOMETRY(POINT, 4326)
);

CREATE TABLE IF NOT EXISTS Accident
(
  accident_key    SERIAL PRIMARY KEY,
  accident_time   TIME NOT NULL,
  environment     TEXT NOT NULL CHECK (environment != ''),
  road_surface    TEXT NOT NULL CHECK (road_surface != ''),
  traffic_control TEXT NOT NULL CHECK (traffic_control != ''),
  visibility      TEXT NOT NULL CHECK (visibility != ''),
  impact_type     TEXT NOT NULL CHECK (impact_type != '')
);

CREATE TABLE IF NOT EXISTS Weather
(
  weather_key    SERIAL PRIMARY KEY,
  station_name   TEXT  NOT NULL CHECK (station_name != ''),
  latitude       FLOAT NOT NULL CHECK (latitude >= -90 AND latitude <= 90),
  longitude      FLOAT NOT NULL CHECK (longitude >= -180 AND longitude <= 180),
  temperature    FLOAT NOT NULL,
  type           TEXT  NOT NULL CHECK (type != ''),
  visibility     FLOAT NOT NULL CHECK (visibility >= -1),
  wind_speed     INT   NOT NULL CHECK (wind_speed >= -1),
  wind_direction INT   NOT NULL CHECK (wind_direction >= -1 AND wind_direction <= 36),
  pressure       FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS Event
(
  event_key  SERIAL PRIMARY KEY,
  name       TEXT NOT NULL CHECK (name != ''),
  start_date DATE NOT NULL,
  end_date   DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS Fact_Table
(
  time_key        INT     NOT NULL,
  FOREIGN KEY (time_key) REFERENCES Hour (time_key) ON UPDATE CASCADE ON DELETE CASCADE,
  location_key    INT     NOT NULL,
  FOREIGN KEY (location_key) REFERENCES Location (location_key) ON UPDATE CASCADE ON DELETE CASCADE,
  accident_key    INT     NOT NULL,
  FOREIGN KEY (accident_key) REFERENCES Accident (accident_key) ON UPDATE CASCADE ON DELETE CASCADE,
  weather_key     INT     NOT NULL,
  FOREIGN KEY (weather_key) REFERENCES Weather (weather_key) ON UPDATE CASCADE ON DELETE CASCADE,
  event_key       INT[],
  fatal           BOOLEAN NOT NULL,
  intersection    BOOLEAN NOT NULL,
  num_pedestrians INT     NOT NULL
);