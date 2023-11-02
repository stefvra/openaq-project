import sys
import json

import boto3
from datetime import datetime, timedelta
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import polars as pl

sys.path.append("config")
from _secrets import profile_name, region_name, queue_url, table_name

from openaqtools.ddb import get_measurements
from config import graph_days_back, map_hours_back, parameters

# read data from dynamoDb
boto3.setup_default_session(profile_name=profile_name)
start_time = datetime.now() - timedelta(hours=map_hours_back)
df = get_measurements(table_name, region_name, parameters, start_time=start_time)

# aggregate to one value
df = df.group_by(["location", "parameter"]).agg(
    pl.col("value").mean(),
    pl.col("latitude").mean(),
    pl.col("longitude").mean(),
)

# Determine the parameters that are available
options = df["parameter"].unique().to_list()

# Initialize the app
app = Dash(__name__)



# App layout
app.layout = html.Div(
    id="main-body",
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
                            dcc.RadioItems(
                                options=options,
                                value=options[0],
                                id="parameter-selection",
                                )
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
        )
        ]
    )


@callback(
    Output(component_id='map-graph', component_property='figure'),
    Input(component_id='parameter-selection', component_property='value')
)
def update_map(parameter):
    """Callback that updates the map when a different pollutant is selected.
    Update is done via filtering on data that is in memory.
    """
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
    """Callback that updates the line graph when location is selected on
    the map or other pollutant is selected. As longer history is needed,
    data is queried from the database.

    """
    if clickData is not None:
        location = clickData["points"][0]["hovertext"]
        start_time_local = datetime.now() - timedelta(days=graph_days_back)
        df_local = get_measurements(table_name, region_name, [parameter], location=location, start_time=start_time_local)
        unit = json.loads(df_local.item(0, "attributes"))["unit"]
        graph = px.line(
            df_local,
            x="utc",
            y="value",
            markers=True,
            labels=dict(utc="Time", value=f"{parameter} [{unit}]")
            )
        title = f"7 Day history for {parameter} in {location}"
    else:
        graph = px.line()
        title = "No Location Selected"
    
    graph.update_layout(
        title={
            "text": title,
            "x": .5,
        }
    )

    return graph


# Run the app
if __name__ == '__main__':
    app.run(debug=True)