SELECT visibility, COUNT(*) FROM accident A
GROUP BY visibility
ORDER BY COUNT(*) DESC;

SELECT environment, COUNT(*) FROM accident A
GROUP BY environment
ORDER BY COUNT(*) DESC;

SELECT road_surface, COUNT(*) FROM accident A
GROUP BY road_surface
ORDER BY COUNT(*) DESC;

SELECT visibility, environment, road_surface, COUNT(*) FROM accident A
GROUP BY visibility, environment, road_surface
ORDER BY COUNT(*) DESC;

SELECT A.road_surface, W.type, Count(*) FROM fact_table F, weather W, accident A
WHERE F.weather_key = W.weather_key and F.accident_key = A.accident_key
GROUP BY A.road_surface, W.type
ORDER BY COUNT(*) DESC