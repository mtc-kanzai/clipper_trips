-- Step 7: Identify transfers (second tag-on within 2 hours of the first)
SELECT 
  trip1.anonymized_card_serial_number,
  CASE 
    WHEN (op1.participant_name = 'Golden Gate Transit' AND rt.route_type = 'Marin Transit Route') THEN 'Marin Transit'
    WHEN (op1.participant_name = 'Napa Solano' AND rt.route_type IN ('FAST Local', 'Napa VINE 29 to BART', 'Napa VINE Express', 'Napa VINE Local')) THEN 'Napa Vine'
    WHEN (op1.participant_name = 'Napa Solano' AND rt.route_type IN ('SolTrans Express', 'SolTrans S82')) THEN 'SolTrans'
    WHEN (op1.participant_name = 'Napa Solano' AND rt.route_type = 'Vacaville Local') THEN 'Vacaville'
    WHEN (op1.participant_name = 'East Bay' AND rt.route_type LIKE 'County Connection%') THEN 'County Connection'
    WHEN (op1.participant_name = 'East Bay' AND rt.route_type LIKE 'TriDelta%') THEN 'Tri Delta'
    WHEN (op1.participant_name = 'East Bay' AND rt.route_type LIKE 'WestCAT%') THEN 'WestCAT'
    WHEN (op1.participant_name = 'East Bay' AND rt.route_type = 'Wheels Local') THEN 'Wheels'
    WHEN (op1.participant_name = 'Corridor 101' AND rt.route_type = 'Petaluma Transit Local Route') THEN 'Petaluma'
    WHEN (op1.participant_name = 'Corridor 101' AND rt.route_type = 'Santa Rosa City Bus Local Route') THEN 'Santa Rosa'
    ELSE op1.participant_name
  END AS trip_1_operator,
  trip1.ride_start_time_pst AS trip_1_tag_on_time,
  trip2.ride_start_time_pst AS trip_2_tag_on_time,
  COALESCE(loc1.location_name, 'Unknown') AS trip_1_origin_location_name,
  COALESCE(loc2.location_name, 'Unknown') AS trip_1_destination_location_name,
  1 AS transfer_flag
INTO TABLE baypass.transfers
FROM baypass.tagons AS trip1
JOIN baypass.tagons AS trip2 ON trip1.anonymized_card_serial_number = trip2.anonymized_card_serial_number
  AND trip1.ride_start_time_pst < trip2.ride_start_time_pst
  AND trip2.ride_start_time_pst < (trip1.ride_start_time_pst + INTERVAL '2 hour')
LEFT JOIN clipper.participant AS op1 ON op1.participant_id = trip1.operator_id
JOIN clipper.participant AS op2 ON op2.participant_id = trip2.operator_id
LEFT JOIN clipper.location AS loc1 ON loc1.participant_id = trip1.operator_id AND loc1.location_code = trip1.origin_location
LEFT JOIN clipper.location AS loc2 ON loc2.participant_id = trip1.operator_id AND loc2.location_code = trip1.destination_location
LEFT JOIN clipper.route AS route1 ON route1.particpant_id = trip1.operator_id AND route1.route_id = trip1.route_id
LEFT JOIN clipper.routename_routetype AS rt ON rt.participant_name = op1.participant_name AND 
  (rt.route_name = route1.route_name OR (rt.route_name IS NULL AND route1.route_name IS NULL))
WHERE NOT EXISTS (
  SELECT 1
  FROM baypass.tagons AS trip3
  WHERE trip1.anonymized_card_serial_number = trip3.anonymized_card_serial_number
    AND trip1.ride_start_time_pst < trip3.ride_start_time_pst
    AND trip2.ride_start_time_pst > trip3.ride_start_time_pst
);