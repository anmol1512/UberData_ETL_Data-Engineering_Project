import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    # Dropping duplicate rows in the table
    df=df.drop_duplicates(ignore_index=True)
    # Dropping rows with atleast one Null value/missing value
    df.dropna(inplace=True)


    # converting datatype of pickup and dropoff datetime col to datetime.
    df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
    df['trip_id']=df.index # generating a trip_id col in dataset

    # return value
    output_tables={}

    # datetime_dime CREATION
    datetime_dim = df.loc[ : , ['tpep_pickup_datetime' , 'tpep_dropoff_datetime']]
    datetime_dim['pickup_day'] = datetime_dim.loc[ : , 'tpep_pickup_datetime'].dt.day
    datetime_dim['pickup_month'] = datetime_dim.loc[ : , 'tpep_pickup_datetime'].dt.month
    datetime_dim['pickup_year'] = datetime_dim.loc[ : , 'tpep_pickup_datetime'].dt.year
    datetime_dim['pickup_hour'] = datetime_dim.loc[ : , 'tpep_pickup_datetime'].dt.hour
    datetime_dim['pickup_weekday'] = datetime_dim.loc[ : , 'tpep_pickup_datetime'].dt.weekday

    datetime_dim['drop_day'] = datetime_dim.loc[ : , 'tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim.loc[ : , 'tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim.loc[ : , 'tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_hour'] = datetime_dim.loc[ : , 'tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_weekday'] = datetime_dim.loc[ : , 'tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim.loc[ : , ['datetime_id' , 'tpep_pickup_datetime', 'pickup_day' , 'pickup_month' , 'pickup_year' , 'pickup_hour' , 'pickup_weekday' , 'tpep_dropoff_datetime' , 'drop_day' , 'drop_month' , 'drop_year' , 'drop_hour' , 'drop_weekday']]
    output_tables['datetime_dim']=datetime_dim.to_dict(orient='dict')

    # passenger_count_dim CREATION
    passenger_count_dim = df.loc[ : ,['passenger_count']]
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim.loc[ : , ['passenger_count_id' , 'passenger_count']]
    output_tables['passenger_count_dim']=passenger_count_dim.to_dict(orient='dict')

    # trip_distance_dim CREATION
    trip_distance_dim = df.loc[ : , ['trip_distance']]
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim.loc[ : , ['trip_distance_id' , 'trip_distance']]
    output_tables['trip_distance_dim']=trip_distance_dim.to_dict(orient='dict')

    # pickup_location_dim CREATION
    pickup_location_dim = df.loc[ : , ['pickup_longitude' , 'pickup_latitude']]
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim.loc[ : , ['pickup_location_id' , 'pickup_longitude' , 'pickup_latitude']]
    output_tables['pickup_location_dim']=pickup_location_dim.to_dict(orient='dict')

    # dropoff_location_dim CREATION
    dropoff_location_dim = df.loc[ : , ['dropoff_longitude' , 'dropoff_latitude']]
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim.loc[ : , ['dropoff_location_id' , 'dropoff_longitude' , 'dropoff_latitude']]
    output_tables['dropoff_location_dim']=dropoff_location_dim.to_dict(orient='dict')

    # ratecode_dim CREATION
    rate_code_type = {
    1 : "Standard rate",
    2 : "JFK",
    3 : "Newark",
    4 : "Nassau or Westchester",
    5 : "Negotiated fare",
    6 : "Group ride"
    }

    ratecode_dim = df.loc[ : , ['RatecodeID']]
    ratecode_dim['ratecode_id'] = ratecode_dim.index
    ratecode_dim['ratecode_name'] = ratecode_dim.loc[ : ,'RatecodeID'].map(rate_code_type)
    ratecode_dim = ratecode_dim.loc[ : , ['ratecode_id' , 'RatecodeID' , 'ratecode_name']]
    output_tables['ratecode_dim']=ratecode_dim.to_dict(orient='dict')

    # payment_type_dim CREATION
    payment_type_name = {
    1 : "Credit card",
    2 : "Cash",
    3 : "No charge",
    4 : "Dispute",
    5 : "Unknown",
    6 : "Voided trip"
    }

    payment_type_dim = df.loc[ : , ['payment_type']]
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim.loc[ : , 'payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim.loc[ : , ['payment_type_id' , 'payment_type' , 'payment_type_name']]
    output_tables['payment_type_dim']=payment_type_dim.to_dict(orient='dict')

    # trip_table CREATION
    trip_table=df.merge(passenger_count_dim , left_on = 'trip_id' , right_on = 'passenger_count_id') \
             .merge(trip_distance_dim , left_on = 'trip_id' , right_on = 'trip_distance_id') \
             .merge(ratecode_dim , left_on = 'trip_id' , right_on = 'ratecode_id') \
             .merge(pickup_location_dim , left_on = 'trip_id' , right_on = 'pickup_location_id') \
             .merge(dropoff_location_dim , left_on = 'trip_id' , right_on = 'dropoff_location_id') \
             .merge(datetime_dim , left_on = 'trip_id' , right_on = 'datetime_id') \
             .merge(payment_type_dim , left_on = 'trip_id' , right_on = 'payment_type_id') \
             [['trip_id' , 'VendorID' , 'datetime_id' , 'passenger_count_id' , 'trip_distance_id' , 'pickup_location_id' , 'dropoff_location_id' , 'ratecode_id' , 'payment_type_id' , 'store_and_fwd_flag' , 'fare_amount' , 'extra' , 'mta_tax' , 'tip_amount' , 'tolls_amount' , 'improvement_surcharge' , 'total_amount']]
    output_tables['trip_table']=trip_table.to_dict(orient='dict')
    return output_tables


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
