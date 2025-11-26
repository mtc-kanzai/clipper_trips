-- Step 7: Identify transfers (second tag-on within 2 hours of the first)
CREATE TABLE baypass.transfers AS
WITH trip1_enriched AS (
  SELECT
    trip1.anonymized_card_serial_number,
    -- Build the operator label once so we can reuse it downstream
    CASE 
      WHEN (op1.participant_name = 'Golden Gate Transit' AND rt.route_type = 'Marin Transit Route') THEN 'Marin Transit'
      WHEN (op1.participant_name = 'Napa Solano' AND rt.route_type IN ('FAST Local')) THEN 'FAST'
      WHEN (op1.participant_name = 'Napa Solano' AND rt.route_type IN ('Napa VINE 29 to BART', 'Napa VINE Express', 'Napa VINE Local')) THEN 'Napa Vine'
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
    COALESCE(loc1.location_name, 'Unknown') AS trip_1_origin_location_name,
    COALESCE(loc2.location_name, 'Unknown') AS trip_1_destination_location_name
  FROM baypass.tagons AS trip1
  LEFT JOIN clipper.participant AS op1
    ON op1.participant_id = trip1.operator_id
  LEFT JOIN clipper.location AS loc1
    ON loc1.participant_id = trip1.operator_id
   AND loc1.location_code = trip1.origin_location
  LEFT JOIN clipper.location AS loc2
    ON loc2.participant_id = trip1.operator_id
   AND loc2.location_code = trip1.destination_location
  LEFT JOIN clipper.route AS route1
    ON route1.particpant_id = trip1.operator_id        -- to do: fix typo: participant_id
   AND route1.route_id = trip1.route_id
  LEFT JOIN clipper.routename_routetype AS rt
    ON rt.participant_name = op1.participant_name
   AND (rt.route_name = route1.route_name OR (rt.route_name IS NULL AND route1.route_name IS NULL))
),
candidate_pairs AS (
  SELECT
    e.anonymized_card_serial_number,
    e.trip_1_operator,
    e.trip_1_tag_on_time,
    e.trip_1_origin_location_name,
    e.trip_1_destination_location_name,
    trip2.ride_start_time_pst AS trip_2_tag_on_time,
    ttr.allowable_transfer_time,
    ROW_NUMBER() OVER (
      PARTITION BY e.anonymized_card_serial_number, e.trip_1_tag_on_time
      ORDER BY trip2.ride_start_time_pst ASC
    ) AS rn_next
  FROM trip1_enriched e
  JOIN baypass.tagons AS trip2
    ON trip2.anonymized_card_serial_number = e.anonymized_card_serial_number
   AND trip2.ride_start_time_pst > e.trip_1_tag_on_time
  LEFT JOIN baypass.transfer_time_rules AS ttr
    ON ttr.operator_transferred_from = e.trip_1_operator
  WHERE trip2.ride_start_time_pst <
        e.trip_1_tag_on_time + (CAST(ttr.allowable_transfer_time AS INT) || ' minutes')::interval
)
SELECT
  anonymized_card_serial_number,
  trip_1_operator,
  trip_1_tag_on_time,
  trip_2_tag_on_time,
  trip_1_origin_location_name,
  trip_1_destination_location_name,
  allowable_transfer_time,
  1 AS transfer_flag
FROM candidate_pairs
WHERE rn_next = 1;