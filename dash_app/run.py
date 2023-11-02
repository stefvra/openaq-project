import sys

import boto3
from datetime import datetime, timedelta
from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import polars as pl

sys.path.append("config")
from _secrets import profile_name, region_name, queue_url, table_name

from openaqtools.ddb import get_measurements


# read data from dynamoDb

boto3.setup_default_session(profile_name=profile_name)

start_time = datetime.now() - timedelta(hours=6)
parameters = ["pm25", "o3", "so2", "pm10", "co", "no2"]
df = get_measurements(table_name, region_name, parameters, start_time=start_time)


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