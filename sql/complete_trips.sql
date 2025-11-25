-- Step 3: Identify complete trips by matching tag-ons and tag-offs
SELECT f.*,
  CASE 
    WHEN f.fare_transaction_sub_type = 3 THEN n.purse_amount - f.purse_amount
    WHEN f.fare_transaction_sub_type = 5 THEN f.purse_amount
  END AS fare_charged
INTO TABLE baypass.complete_trips
FROM baypass.tagoffs as f
JOIN baypass.tagons as n ON f.anonymized_card_serial_number = n.anonymized_card_serial_number
  AND f.ride_start_time_pst = n.ride_start_time_pst;