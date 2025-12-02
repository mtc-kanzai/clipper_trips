SELECT t.*
INTO TABLE baypass.clipper_trips
FROM (
  SELECT * FROM baypass.clipper_trips_2019_to_2022
  UNION
  SELECT * FROM baypass.clipper_trips_2022_to_2024
  UNION
  SELECT * FROM baypass.clipper_trips_2024_to_2026
) AS t;