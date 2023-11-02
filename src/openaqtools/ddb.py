import logging
import decimal
import boto3
from boto3.dynamodb.conditions import Attr, Key
import polars as pl

from datetime import datetime, timedelta



logger = logging.Logger(__name__)
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sh)

# read data from dynamoDb

def get_measurements(
    table_name: str,
    region_name: str,
    parameters: list,
    start_time: datetime = None,
    stop_time: datetime = None,
    location: str = None
    ) -> pl.DataFrame:
    """Function queries aws dynamodb for measurements.
    The database needs to have a specific format.

    Args:
        table_name (str): table name
        region_name (str): region name
        parameters (list): parameters to query
        start_time (datetime, optional): Start time for query. Defaults to None.
        stop_time (datetime, optional): Stop time for query. Defaults to None.
        location (str, optional): Location to query. Defaults to None.

    Returns:
        pl.DataFrame: measurements in dataframe with coordinates and date unpacked
    """


    table = boto3.resource('dynamodb', region_name=region_name).Table(table_name)
    
    # Define time expression. Depending on availability of start and stop time
    if start_time is None and stop_time is None:
        time_exp = Key("timestamp").gt(decimal.Decimal(0))
    elif stop_time == None:
        time_exp = Key("timestamp").gt(decimal.Decimal(start_time.timestamp()))
    elif start_time == None:
        time_exp = Key("timestamp").lt(decimal.Decimal(stop_time.timestamp()))

    dfs = []
    # Query for all parameters. Needs to be iterative as aws does not allow querying multiple key parameter values
    for parameter in parameters:

        key_exp = Key("parameter").eq(parameter) & time_exp

        # If location is provided, a filter expression is added
        if location is None:
            data = table.query(KeyConditionExpression=key_exp)
        else:
            filt_exp = Attr("location").eq(location)
            data = table.query(
                KeyConditionExpression=key_exp,
                FilterExpression=filt_exp
                )

        items = data["Items"]
        if items is not None:
            dfs.append(pl.from_dicts(data["Items"]))

    # Concatenate the dataframes and unpack the columns
    df = pl.concat(dfs, rechunk=True)
    df = df.with_columns(df["coordinates"].str.json_extract()).unnest("coordinates")
    df = df.with_columns(df["date"].str.json_extract()).unnest("date")
    df = df.with_columns(pl.col(["utc", "local"]).str.to_datetime())

    logger.info(df)

    return df