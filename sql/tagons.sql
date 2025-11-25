-- Step 2: Separate tag-ons and tag-offs
SELECT * INTO TABLE baypass.tagons
FROM baypass.anonymized_fare_transaction_subset
WHERE fare_transaction_sub_type IN (1, 2, 4);