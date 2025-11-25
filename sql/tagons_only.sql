-- Step 5: Identify tag-ons without matching tag-offs
SELECT n.*,
  n.purse_amount AS fare_charged
INTO TABLE baypass.tagons_only
FROM baypass.tagons as n
LEFT JOIN baypass.tagoffs as f ON f.anonymized_card_serial_number = n.anonymized_card_serial_number
  AND f.ride_start_time_pst = n.ride_start_time_pst
WHERE f.ride_start_time_pst IS NULL;