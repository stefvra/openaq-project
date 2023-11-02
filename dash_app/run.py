import sys
import decimal

import boto3
import datetime
from boto3.dynamodb.conditions import Attr, Key
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import polars as pl

sys.path.append("config")
from _secrets import profile_name, region_name, queue_url, table_name


# read data from dynamoDb

boto3.setup_default_session(profile_name=profile_name)
table = boto3.resource('dynamodb', region_name=region_name).Table(table_name)
local_time = datetime.datetime.now() - datetime.timedelta(hours=6)

parameters = ["pm25", "o3", "so2", "pm10", "co", "no2"]
dfs = []
for parameter in parameters:
    filt = Key("parameter").eq(parameter) & Key("timestamp").gt(decimal.Decimal(local_time.timestamp()))
    data = table.query(KeyConditionExpression=filt)
    dfs.append(pl.from_dicts(data["Items"]))
df = pl.concat(dfs, rechunk=True)
df = df.with_columns(df["coordinates"].str.json_extract()).unnest("coordinates")
df = df.with_columns(df["date"].str.json_extract()).unnest("date")
df = df.with_columns(pl.col(["utc", "local"]).str.to_datetime())



colors = {
    "black": "#33334d",
    "white": "#ffffff"
}

# Initialize the app
app = Dash(__name__)


options = df["parameter"].unique().to_list()
selection_box = dcc.RadioItems(
    options=options,
    value=options[0],
    id="parameter-selection",
    )





# App layout
app.layout = html.Div(
    children=[
        html.Div(),
        html.H1(
            children="AirMax - Air Quality Dashboard",
            id="title"
        ),
        html.Div(
            id="grid-container",
            children=[
                html.Div(
                    id="map-div",
                    children=[
                        dcc.Graph(figure={}, id="map-graph"),
                    ]
                ),
                html.Div(
                    id="right-column",
                    children=html.Div(
                        children=[
                            html.H3("Select Pollutant"),
                            selection_box
                            ]
                        )
                    )
                ]
            ),
        html.Div(
            className="graph-div",
            children=[
                dcc.Graph(figure={}, id="graph-graph")
            ]
        ),
        html.Hr()
        ]
    )

@callback(
    Output(component_id='map-graph', component_property='figure'),
    Input(component_id='parameter-selection', component_property='value')
)
def update_map(parameter):
    size = 7
    df_filtered = df.filter(pl.col("parameter") == parameter)
    map = px.scatter_mapbox(
        data_frame=df_filtered,
        lat="latitude",
        lon="longitude",
        color="value",
        size=len(df_filtered)*[size],
        hover_name="location",
        mapbox_style="open-street-map",
        zoom=6,
        )
    
    map.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0}

        )
    return map


@callback(
    Output(component_id='graph-graph', component_property='figure'),
    Input(component_id='parameter-selection', component_property='value'),
    Input(component_id='map-graph', component_property='clickData')
)
def update_graph(parameter, clickData):
    if clickData is not None:
        location = clickData["points"][0]["hovertext"]
        df_filtered = df.filter((pl.col("parameter") == parameter) & (pl.col("location") == location))
        graph = px.scatter(
            df_filtered,
            x="utc",
            y="value",
            )
        title = f"7 Day history for {parameter} in {location}"
    else:
        graph = px.scatter()
        title = "No Location Selected"
    
    graph.update_layout(
        title={
            'text': title,
            'x': .5
        }
    )

    return graph


# Run the app
if __name__ == '__main__':
    app.run(debug=True)