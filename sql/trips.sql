-- Step 6: Combine all trips into a unified table
SELECT t.*,
  EXTRACT(YEAR FROM t.ride_start_time_pst) AS fare_year,
  CASE WHEN EXTRACT(MONTH FROM t.ride_start_time_pst) > 6 THEN 7 ELSE 1 END AS fare_month
INTO TABLE baypass.trips
FROM (
  SELECT *, 'Tag-off Only' AS trip_type FROM baypass.tagoffs_only
  UNION
  SELECT *, 'Tag-on Only' AS trip_type FROM baypass.tagons_only
  UNION
  SELECT *, 'Tag-on and Tag-off' AS trip_type FROM baypass.complete_trips
) AS t;