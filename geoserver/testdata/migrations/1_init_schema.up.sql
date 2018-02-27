-- Create table to contain test data
CREATE TABLE test_data (
  id SERIAL PRIMARY KEY,
  geom GEOMETRY(Point, 26910),
  name VARCHAR(128)
);

-- Insert some points
INSERT INTO test_data (geom, name) VALUES (
  ST_GeomFromText('POINT(0 0)', 26910),
  'Point A'
);