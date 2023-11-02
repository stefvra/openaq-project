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

def get_measurements(table_name, region_name, parameters, start_time=None, stop_time=None, max_nr_records=None):


    table = boto3.resource('dynamodb', region_name=region_name).Table(table_name)
    
    if start_time is None and stop_time is None:
        time_filter = Key("timestamp").gt(decimal.Decimal(0))
    elif stop_time == None:
        time_filter = Key("timestamp").gt(decimal.Decimal(start_time.timestamp()))
    elif start_time == None:
        time_filter = Key("timestamp").lt(decimal.Decimal(stop_time.timestamp()))

    dfs = []
    for parameter in parameters:
        filt = Key("parameter").eq(parameter) & time_filter
        if max_nr_records is None:
            data = table.query(KeyConditionExpression=filt)
        else:
            data = table.query(KeyConditionExpression=filt, limit=max_nr_records)
        dfs.append(pl.from_dicts(data["Items"]))

    df = pl.concat(dfs, rechunk=True)
    df = df.with_columns(df["coordinates"].str.json_extract()).unnest("coordinates")
    df = df.with_columns(df["date"].str.json_extract()).unnest("date")
    df = df.with_columns(pl.col(["utc", "local"]).str.to_datetime())

    return df