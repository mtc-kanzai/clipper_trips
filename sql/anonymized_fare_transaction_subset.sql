-- Step 1: Filter fare transactions from January 2, 2024 onward
CREATE TABLE baypass.anonymized_fare_transaction_subset
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
;