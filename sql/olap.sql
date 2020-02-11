%1%
SELECT COUNT(*) FROM fact_table F
WHERE F.fatal = True;

SELECT COUNT(*) FROM fact_table F, hour H
WHERE F.fatal = True and H.year = 2015

SELECT COUNT(*) FROM fact_table F, hour H, weather W
WHERE F.time_key = H.time_key and H.year = 2014 and W.type = 'ice_storm';

SELECT COUNT(*) FROM fact_table F, hour H
WHERE F.time_key = H.time_key and H.year = 2014, H.month = 'December', W.type = 'ice_storm';

SELECT COUNT(*) FROM fact_table F, hour H, location L
WHERE F.time_key = H.time_key and H.year = 2014, L.neighbourhood = 'Downtown', W.type = 'ice_storm';

SELECT COUNT(*) FROM fact_table F, hour H, location L
WHERE F.time_key = H.time_key and H.year = 2014, H.month = 'December', L.neighbourhood = 'Downtown', W.type = 'ice_storm';


%2%
SELECT H.day_of_week, COUNT(*) FROM fact_table F, hour H
WHERE F.time_key = H.time_key and (H.day_of_week = 'monday' or H.day_of_week = 'friday')
GROUP BY H.day_of_week;

%3.
SELECT H.month=7, H.month=10, COUNT(*) FROM fact_table F, Hour H
WHERE F.time_key = H.time_key
GROUP BY GROUPING SETS ((H.month = 7), (H.month = 10), ())

SELECT A.road_surface, COUNT(*) FROM fact_table F, accident A
WHERE F.accident_key = A.accident_key
GROUP BY GROUPING SETS ((A.road_surface), ())
ORDER BY COUNT(*) DESC

SELECT A.environment, W.wind_speed, COUNT(*) FROM accident A, weather W
GROUP BY GROUPING SETS ((A.environment), (W.wind_speed))

SELECT L.neighbourhood, A.environment, F.fatal, COUNT(*) FROM fact_table F, location L, Accident A
WHERE F.location_key = L.location_key and F.accident_key = A.accident_key and 
		(L.neighbourhood = 'Centretown' or L.neighbourhood like 'Orleans Industrial') and (A.environment = '02 - Rain' or A.environment = '03 - Snow')
GROUP BY ROLLUP ((L.neighbourhood), (A.environment), (F.fatal))
ORDER BY COUNT(*) DESC

SELECT A.visibility, F.fatal, COUNT(*) FROM fact_table F, weather W, Accident A
WHERE F.weather_key = W.weather_key and F.accident_key = A.accident_key and W.pressure < 100
GROUP BY ROLLUP ((A.visibility), (F.fatal))
ORDER BY COUNT(*) DESC

SELECT F.intersection, F.num_pedestrians, F.fatal, COUNT(*) FROM fact_table F
WHERE F.num_pedestrians != -1
GROUP BY ROLLUP ((F.intersection), (F.num_pedestrians), (F.fatal))
ORDER BY COUNT(*) DESC

%4
SELECT L.road_name, L.intersection1, L.intersection2, COUNT(*) FROM location L
GROUP BY (L.road_name, L.intersection1, L.intersection2)
ORDER BY COUNT(*) DESC
limit 10

SELECT L.neighbourhood, A.visibility, COUNT(*) FROM location L, fact_table F, accident A
WHERE F.location_key = L.location_key and F.accident_key = A.accident_key and A.visibility != '00 - Unknown'
GROUP BY (L.neighbourhood, A.visibility)
ORDER BY COUNT(*) DESC
limit 10