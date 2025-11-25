-- Step 4: Identify tag-offs without matching tag-ons
SELECT f.*,
  CASE 
    WHEN f.fare_transaction_sub_type = 3 THEN -f.purse_amount
    WHEN f.fare_transaction_sub_type = 5 THEN f.purse_amount
  END AS fare_charged
INTO TABLE baypass.tagoffs_only
FROM baypass.tagoffs as f
LEFT JOIN baypass.tagons as n ON f.anonymized_card_serial_number = n.anonymized_card_serial_number
  AND f.ride_start_time_pst = n.ride_start_time_pst
WHERE n.ride_start_time_pst IS NULL;