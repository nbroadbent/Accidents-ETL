UPDATE location SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326 );
CREATE INDEX location_gist
	ON location
  	USING gist (geom);