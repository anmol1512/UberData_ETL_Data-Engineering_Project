DROP TABLE IF EXISTS Uber_dataset.bigquery_analytics_query;
CREATE TABLE Uber_dataset.bigquery_analytics_query AS
(
SELECT
fact.trip_id,
fact.VendorID,
datetime_dim.tpep_pickup_datetime,
datetime_dim.tpep_dropoff_datetime,
passenger_count_dim.passenger_count,
trip_distance_dim.trip_distance,
ratecode_dim.ratecode_name,
pickup.pickup_latitude,
pickup.pickup_longitude,
dropoff.dropoff_latitude,
dropoff.dropoff_longitude,
payment.payment_type_name,
fact.fare_amount,
fact.extra,
fact.mta_tax,
fact.tip_amount,
fact.tolls_amount,
fact.improvement_surcharge,
fact.total_amount

from
theta-arcana-396802.Uber_dataset.trip_table fact 
join theta-arcana-396802.Uber_dataset.datetime_dim datetime_dim on fact.datetime_id=datetime_dim.datetime_id
join theta-arcana-396802.Uber_dataset.passenger_count_dim passenger_count_dim on fact.passenger_count_id=passenger_count_dim.passenger_count_id
join theta-arcana-396802.Uber_dataset.trip_distance_dim trip_distance_dim on fact.trip_distance_id=trip_distance_dim.trip_distance_id
join theta-arcana-396802.Uber_dataset.ratecode_dim ratecode_dim on fact.ratecode_id=ratecode_dim.ratecode_id
join theta-arcana-396802.Uber_dataset.pickup_location_dim pickup on fact.pickup_location_id=pickup.pickup_location_id
join theta-arcana-396802.Uber_dataset.dropoff_location_dim dropoff on fact.dropoff_location_id=dropoff.dropoff_location_id
join theta-arcana-396802.Uber_dataset.payment_type_dim payment on fact.payment_type_id=payment.payment_type_id
);