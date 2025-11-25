-- Scheduled Job Optimized Version of Trip Processing Query, optimized from existing code using Copilot

-- Step 1: Filter fare transactions from January 2, 2024 onward
DROP TABLE IF EXISTS anonymized_fare_transaction_subset;

CREATE TEMP TABLE anonymized_fare_transaction_subset
DISTSTYLE KEY DISTKEY(anonymized_card_serial_number)
SORTKEY(ride_start_time_pst)
AS
SELECT
    aft.anonymized_card_serial_number,
    aft.ride_start_time_pst,
    aft.operator_id,
    aft.origin_location,
    aft.destination_location,
    aft.fare_transaction_sub_type,
    aft.route_id,
    aft.fare_category,
    aft.fare_discount,
    aft.purse_amount,
    aft.contract_id,
    aft.institution_id,
    CASE aft.fare_category
        WHEN 0 THEN 'Adult'
        WHEN 1 THEN 'Senior'
        WHEN 2 THEN 'RTC'
        WHEN 3 THEN 'Youth'
        ELSE 'Undefined'
    END AS fare_category_name
FROM clipper.anonymized_fare_transaction aft
LEFT JOIN clipper.clipper_coupon_contract_ids cc
    ON aft.contract_id = cc.id
WHERE aft.ride_start_time_pst >= TIMESTAMP '2024-01-02 00:00:00'
-- 	AND aft.ride_start_time_pst < TIMESTAMP '2024-01-02 00:00:00'
;



-- Step 2: Separate tag-ons and tag-offs
DROP TABLE IF EXISTS tagons;
SELECT * INTO TEMP TABLE tagons
FROM anonymized_fare_transaction_subset
WHERE fare_transaction_sub_type IN (1, 2, 4);

DROP TABLE IF EXISTS tagoffs;
SELECT * INTO TEMP TABLE tagoffs
FROM anonymized_fare_transaction_subset
WHERE fare_transaction_sub_type IN (3, 5);

-- Step 3: Identify complete trips by matching tag-ons and tag-offs
DROP TABLE IF EXISTS complete_trips;
SELECT tagoffs.*,
  CASE 
    WHEN tagoffs.fare_transaction_sub_type = 3 THEN tagons.purse_amount - tagoffs.purse_amount
    WHEN tagoffs.fare_transaction_sub_type = 5 THEN tagoffs.purse_amount
  END AS fare_charged
INTO TEMP TABLE complete_trips
FROM tagoffs
JOIN tagons ON tagoffs.anonymized_card_serial_number = tagons.anonymized_card_serial_number
  AND tagoffs.ride_start_time_pst = tagons.ride_start_time_pst;

-- Step 4: Identify tag-offs without matching tag-ons
DROP TABLE IF EXISTS tagoffs_only;
SELECT tagoffs.*,
  CASE 
    WHEN tagoffs.fare_transaction_sub_type = 3 THEN -tagoffs.purse_amount
    WHEN tagoffs.fare_transaction_sub_type = 5 THEN tagoffs.purse_amount
  END AS fare_charged
INTO TEMP TABLE tagoffs_only
FROM tagoffs
LEFT JOIN tagons ON tagoffs.anonymized_card_serial_number = tagons.anonymized_card_serial_number
  AND tagoffs.ride_start_time_pst = tagons.ride_start_time_pst
WHERE tagons.ride_start_time_pst IS NULL;

-- Step 5: Identify tag-ons without matching tag-offs
DROP TABLE IF EXISTS tagons_only;
SELECT tagons.*,
  tagons.purse_amount AS fare_charged
INTO TEMP TABLE tagons_only
FROM tagons
LEFT JOIN tagoffs ON tagoffs.anonymized_card_serial_number = tagons.anonymized_card_serial_number
  AND tagoffs.ride_start_time_pst = tagons.ride_start_time_pst
WHERE tagoffs.ride_start_time_pst IS NULL;

-- Step 6: Combine all trips into a unified table
DROP TABLE IF EXISTS trips;
SELECT t.*,
  EXTRACT(YEAR FROM t.ride_start_time_pst) AS fare_year,
  CASE WHEN EXTRACT(MONTH FROM t.ride_start_time_pst) > 6 THEN 7 ELSE 1 END AS fare_month
INTO TEMP TABLE trips
FROM (
  SELECT *, 'Tag-off Only' AS trip_type FROM tagoffs_only
  UNION
  SELECT *, 'Tag-on Only' AS trip_type FROM tagons_only
  UNION
  SELECT *, 'Tag-on and Tag-off' AS trip_type FROM complete_trips
) AS t;

-- Step 7: Identify transfers (second tag-on within 2 hours of the first)
DROP TABLE IF EXISTS transfers;
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
INTO TEMP TABLE transfers
FROM tagons AS trip1
JOIN tagons AS trip2 ON trip1.anonymized_card_serial_number = trip2.anonymized_card_serial_number
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
  FROM tagons AS trip3
  WHERE trip1.anonymized_card_serial_number = trip3.anonymized_card_serial_number
    AND trip1.ride_start_time_pst < trip3.ride_start_time_pst
    AND trip2.ride_start_time_pst > trip3.ride_start_time_pst
);

-- Step 8: Enrich trips with metadata and transfer flags
DROP TABLE IF EXISTS baypass.clipper_trips;
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
FROM trips AS t
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
LEFT JOIN transfers AS tr ON t.ride_start_time_pst = tr.trip_2_tag_on_time AND t.anonymized_card_serial_number = tr.anonymized_card_serial_number;