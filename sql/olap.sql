-- 1.
SELECT F.fatal, H.year, COUNT(*)
FROM fact_table F,
     hour H
WHERE F.fatal = True
  and F.time_key = H.time_key
GROUP BY F.fatal, H.year;

SELECT F.fatal, H.year, COUNT(*)
FROM fact_table F,
     hour H
WHERE F.fatal = True
  and F.time_key = H.time_key
  and H.year = 2015
GROUP BY F.fatal, H.year;

SELECT F.fatal, H.year, W.type, COUNT(*)
FROM fact_table F,
     hour H,
     weather W
WHERE F.fatal = True
  and F.time_key = H.time_key
  and F.weather_key = W.weather_key
  and H.year = 2014
  and (W.type like '%Ice%' or W.type like '%Freezing%')
GROUP BY F.fatal, H.year, W.type;

SELECT F.fatal, H.month, H.year, W.type, COUNT(*)
FROM fact_table F,
     hour H,
     weather W
WHERE F.fatal = True
  and F.time_key = H.time_key
  and F.weather_key = W.weather_key
  and H.year = 2014
  and H.month = 12
  and (W.type like '%Ice%' or W.type like '%Freezing%')
GROUP BY F.fatal, H.month, H.year, W.type;

SELECT F.fatal, H.month, H.year, L.neighbourhood, W.type, COUNT(*)
FROM fact_table F,
     hour H,
     weather W,
     location L
WHERE F.fatal = True
  and F.time_key = H.time_key
  and F.weather_key = W.weather_key
  and H.year = 2014
  and (W.type like '%Ice%' or W.type like '%Freezing%')
  and L.neighbourhood like '%Centretown%'
GROUP BY F.fatal, H.month, H.year, L.neighbourhood, W.type;

SELECT F.fatal, H.month, H.year, L.neighbourhood, W.type, COUNT(*)
FROM fact_table F,
     hour H,
     weather W,
     location L
WHERE F.fatal = True
  and F.time_key = H.time_key
  and F.weather_key = W.weather_key
  and F.location_key = L.location_key
  and H.year = 2014
  and H.month = 12
  and (W.type like '%Ice%' or W.type like '%Freezing%')
  and L.neighbourhood like '%Centretown%'
GROUP BY F.fatal, H.month, H.year, L.neighbourhood, W.type;


-- 2.
SELECT H.day_of_week, COUNT(*)
FROM fact_table F,
     hour H
WHERE F.time_key = H.time_key
  and (H.day_of_week = 0 or H.day_of_week = 4)
GROUP BY H.day_of_week;

SELECT H.day_of_week, F.fatal, COUNT(*)
FROM fact_table F,
     hour H
WHERE F.time_key = H.time_key
  and (H.day_of_week = 0 or H.day_of_week = 4)
  and F.fatal = True
GROUP BY H.day_of_week, F.fatal;

SELECT L.neighbourhood, F.fatal, H.year, COUNT(*)
FROM fact_table F,
     location L,
     hour H
WHERE F.location_key = L.location_key
  and F.time_key = H.time_key
  and ((L.neighbourhood like '%Orleans%' and H.year = 2014) or (L.neighbourhood like '%Nepean%' and H.year = 2017))
GROUP BY L.neighbourhood, F.fatal, H.year;

SELECT L.neighbourhood, F.fatal, H.day_of_week, COUNT(*)
FROM fact_table F,
     location L,
     hour H
WHERE F.location_key = L.location_key
  and F.time_key = H.time_key
  and L.neighbourhood like '%Nepean%'
  and (H.day_of_week = 0 or H.day_of_week = 4)
GROUP BY L.neighbourhood, F.fatal, H.day_of_week;

SELECT L.neighbourhood, F.fatal, H.day_of_week, H.time, COUNT(*)
FROM fact_table F,
     location L,
     hour H
WHERE F.location_key = L.location_key
  and F.time_key = H.time_key
  and (L.neighbourhood like '%Nepean%'
  or L.neighbourhood like '%Orleans%')
  and H.day_of_week = 0
  and (H.time between '05:00:00'::time and '08:00:00'::time)
GROUP BY L.neighbourhood, F.fatal, H.day_of_week, H.time;

SELECT F.intersection, COUNT(*)
FROM fact_table F
GROUP BY F.intersection;

SELECT F.fatal, F.intersection, COUNT(*)
FROM fact_table F
WHERE F.fatal = True
GROUP BY F.intersection, F.fatal;

SELECT F.fatal, F.intersection, L.neighbourhood, COUNT(*)
FROM fact_table F,
     location L
WHERE F.fatal = True
  and F.location_key = L.location_key
  and L.neighbourhood LIKE '%Centretown%'
GROUP BY F.intersection, F.fatal, L.neighbourhood;

-- 3.
SELECT H.month = 7 as summer, H.month = 10 as fall, COUNT(*)
FROM fact_table F,
     Hour H
WHERE F.time_key = H.time_key
  and (H.month = 7
  or H.month = 10)
GROUP BY H.month;

SELECT A.road_surface, COUNT(*) as count
FROM fact_table F,
     accident A
WHERE F.accident_key = A.accident_key
GROUP BY GROUPING SETS ((A.road_surface), ())
ORDER BY count DESC;

SELECT W.type, COUNT(*) as count
FROM fact_table F,
     weather W
WHERE F.weather_key = W.weather_key
  and W.type LIKE '%Rain%' -- FIXME returns all types of rain, not just heavy
GROUP BY W.type
ORDER BY count DESC;

-- SELECT A.environment, W.wind_speed, COUNT(*)
-- FROM accident A,
--      weather W
-- GROUP BY GROUPING SETS ((A.environment), (W.wind_speed));

-- SELECT L.neighbourhood, A.environment, F.fatal, COUNT(*)
-- FROM fact_table F,
--      location L,
--      Accident A
-- WHERE F.location_key = L.location_key
--   and F.accident_key = A.accident_key
--   and (L.neighbourhood = 'Centretown' or L.neighbourhood like 'Orleans Industrial')
--   and (A.environment = '02 - Rain' or A.environment = '03 - Snow')
-- GROUP BY ROLLUP ((L.neighbourhood), (A.environment), (F.fatal))
-- ORDER BY COUNT(*) DESC

-- SELECT A.visibility, F.fatal, COUNT(*)
-- FROM fact_table F,
--      weather W,
--      Accident A
-- WHERE F.weather_key = W.weather_key
--   and F.accident_key = A.accident_key
--   and W.pressure < 100
-- GROUP BY ROLLUP ((A.visibility), (F.fatal))
-- ORDER BY COUNT(*) DESC

-- SELECT F.intersection, F.num_pedestrians, F.fatal, COUNT(*)
-- FROM fact_table F
-- WHERE F.num_pedestrians != -1
-- GROUP BY ROLLUP ((F.intersection), (F.num_pedestrians), (F.fatal))
-- ORDER BY COUNT(*) DESC

-- 4.
SELECT L.road_name, L.intersection1, L.intersection2, COUNT(*) as count
FROM location L
GROUP BY L.road_name, L.intersection1, L.intersection2
ORDER BY count DESC
limit 10;

SELECT L.neighbourhood, H.time, COUNT(*) as count
FROM fact_table F,
     location L,
     hour H
WHERE F.location_key = L.location_key
  and F.time_key = H.time_key
  and H.time = '17:00:00'::time
GROUP BY L.neighbourhood, H.time
ORDER BY count DESC
limit 10;

SELECT L.road_name, L.intersection1, L.intersection2, H.month, A.visibility, COUNT(*) as count
FROM fact_table F,
     location L,
     hour H,
     accident A
WHERE F.location_key = L.location_key
  and F.time_key = H.time_key
  and F.accident_key = A.accident_key
  and H.month = 10
  and A.visibility LIKE '%Dusk%'
GROUP BY L.road_name, L.intersection1, L.intersection2, H.month, A.visibility
ORDER BY count DESC
limit 10;

SELECT L.road_name, L.intersection1, L.intersection2, W.visibility, COUNT(*) as count
FROM fact_table F,
     location L,
     weather W
WHERE F.location_key = L.location_key
  and F.weather_key = W.weather_key
  and L.intersection1 LIKE '%highway%'
  and W.visibility between 0 and 1
GROUP BY L.road_name, L.intersection1, L.intersection2, W.visibility
ORDER BY count DESC
limit 10;

-- 5.
SELECT H.month, COUNT(*)
FROM fact_table F,
     hour H
WHERE F.time_key = H.time_key
  and F.fatal
GROUP BY H.month
ORDER BY H.month;

SELECT H.year, COUNT(*)
FROM fact_table F,
     hour H
WHERE F.time_key = H.time_key
  and F.fatal
GROUP BY H.year
ORDER BY H.year;

SELECT H.time, COUNT(*) as count
FROM fact_table F,
     hour H
WHERE F.time_key = H.time_key
  and F.fatal
GROUP BY H.time
ORDER BY H.time;