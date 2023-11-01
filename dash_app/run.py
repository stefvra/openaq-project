import sys
import decimal

import boto3
import datetime
from boto3.dynamodb.conditions import Attr, Key
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import polars as pl

sys.path.append("config")
from _secrets import profile_name, region_name, queue_url, table_name


# read data from dynamoDb
boto3.setup_default_session(profile_name=profile_name)
table = boto3.resource('dynamodb', region_name=region_name).Table(table_name)
local_time = datetime.datetime.now() - datetime.timedelta(days=3)
filt = Key("parameter").eq("pm10") & Key("timestamp").gt(decimal.Decimal(local_time.timestamp()))
data = table.query(KeyConditionExpression=filt, Limit=100)
df = pl.from_dicts(data["Items"])



# Initialize the app
app = Dash(__name__)

# App layout
app.layout = html.Div([
    html.Div(children='My First App with Data'),
    dash_table.DataTable(data=df.to_dicts(), page_size=10)
])

# Run the app
if __name__ == '__main__':
    app.run(debug=True)