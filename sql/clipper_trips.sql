-- Step 8: Enrich trips with metadata and transfer flags
SELECT
  t.anonymized_card_serial_number,
  ride_start_time_pst,
  t.trip_type,
  t.fare_discount,
  t.fare_charged,
  t.fare_category_name,
  t.institution_id,
  CASE 
    WHEN (p.participant_name = 'Golden Gate Transit' AND rt.route_type = 'Marin Transit Route') THEN 'Marin Transit'
    WHEN (p.participant_name = 'Napa Solano' AND rt.route_type IN ('FAST Local', 'Napa VINE 29 to BART', 'Napa VINE Express', 'Napa VINE Local')) THEN 'Napa Vine'
    WHEN (p.participant_name = 'Napa Solano' AND rt.route_type IN ('SolTrans Express', 'SolTrans S82')) THEN 'SolTrans'
    WHEN (p.participant_name = 'Napa Solano' AND rt.route_type = 'Vacaville Local') THEN 'Vacaville'
    WHEN (p.participant_name = 'East Bay' AND rt.route_type LIKE 'County Connection%') THEN 'County Connection'
    WHEN (p.participant_name = 'East Bay' AND rt.route_type LIKE 'TriDelta%') THEN 'Tri Delta'
    WHEN (p.participant_name = 'East Bay' AND rt.route_type LIKE 'WestCAT%') THEN 'WestCAT'
    WHEN (p.participant_name = 'East Bay' AND rt.route_type = 'Wheels Local') THEN 'Wheels'
    WHEN (p.participant_name = 'Corridor 101' AND rt.route_type = 'Petaluma Transit Local Route') THEN 'Petaluma'
    WHEN (p.participant_name = 'Corridor 101' AND rt.route_type = 'Santa Rosa City Bus Local Route') THEN 'Santa Rosa'
    ELSE p.participant_name
  END AS operator_name,
  COALESCE(l1.location_name, 'Unknown') AS origin_location_name,
  COALESCE(l2.location_name, 'Unknown') AS destination_location_name,
  prod.description AS product_name,
  CASE WHEN contract_id IN (SELECT id FROM clipper.clipper_coupon_contract_ids) THEN 1 ELSE 0 END AS start_flag,
  CASE WHEN contract_id = 520 THEN 1 ELSE 0 END AS baypass_flag,
  COALESCE(rt.route_name, 'N/A') AS route_name,
  COALESCE(rt.route_type, 'N/A') AS route_type_name,
  ft.adult_fare AS estimated_adult_fare,
  COALESCE(tr.transfer_flag, 0) AS transfer_flag,
  tr.trip_1_operator AS operator_transferred_from,
  CASE WHEN tr.transfer_flag IS NOT NULL AND tr.trip_1_operator != p.participant_name THEN 1 ELSE 0 END AS interop_transfer_flag,
  CASE WHEN tr.transfer_flag IS NOT NULL AND tr.trip_1_operator = p.participant_name THEN 1 ELSE 0 END AS intraop_transfer_flag,
  CASE WHEN tr.transfer_flag IS NOT NULL THEN COALESCE(case when trip_1_destination_location_name <> 'Unknown' then trip_1_destination_location_name ELSE NULL END, origin_location_name) ELSE NULL END AS transfer_location,
  tr.trip_1_origin_location_name,
  tr.trip_1_destination_location_name
INTO TABLE baypass.clipper_trips
FROM baypass.trips AS t
LEFT JOIN clipper.participant AS p ON t.operator_id = p.participant_id
LEFT JOIN clipper.location AS l1 ON t.operator_id = l1.participant_id AND t.origin_location = l1.location_code
LEFT JOIN clipper.location AS l2 ON t.operator_id = l2.participant_id AND t.destination_location = l2.location_code
LEFT JOIN clipper.contract_prodtype_map AS cp ON t.contract_id = cp.contract_type
LEFT JOIN clipper.product AS prod ON prod.issuer_id = cp.issuer_id AND prod.product_type = cp.product_type
LEFT JOIN clipper.route AS r ON t.operator_id = r.particpant_id AND t.route_id = r.route_id
LEFT JOIN clipper.routename_routetype AS rt ON rt.participant_name = p.participant_name AND 
  (rt.route_name = r.route_name OR (rt.route_name IS NULL AND r.route_name IS NULL))
LEFT JOIN baypass.fare_table AS ft ON 
  ft.operator = p.participant_name AND
  ft.origin_location = COALESCE(l1.location_name, '0') AND
  ft.destination_location = COALESCE(l2.location_name, '0') AND
  ft.route_type = COALESCE(rt.route_type, '0') AND
  ft.fare_year = COALESCE(t.fare_year, 0) AND
  ft.fare_month = COALESCE(t.fare_month, 0)
LEFT JOIN baypass.transfers AS tr ON t.ride_start_time_pst = tr.trip_2_tag_on_time AND t.anonymized_card_serial_number = tr.anonymized_card_serial_number;