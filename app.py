import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
from datetime import datetime
import pandas as pd
import plotly.graph_objs as go
import pymongo
import os
from functools import reduce


def query_data():
    cursor = db["weekly"].find({}).sort("_id", pymongo.ASCENDING)
    df = pd.DataFrame.from_records(cursor)
    return df.loc[:, df.columns != "_id"].to_json()


def convert_from_datetime(d):
    return datetime.strftime(d, print_format)


def convert_to_datetime(d):
    return datetime.strptime(d, parse_format)


def generate_layout(data):
    return html.Div(
        children=[
            html.H1(children="CRC Weekly"),
            html.Div(
                [
                    html.Div(
                        [dcc.Graph(id="smp-graph", figure=generate_smp(data))],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="gpu-graph", figure=generate_gpu(data))],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="mpi-graph", figure=generate_mpi(data))],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="htc-graph", figure=generate_htc(data))],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="sus-graph", figure=generate_sus(data))],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="storage-graph", figure=generate_storage(data))],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                ]
            ),
            html.Div(id="data", style={"display": "none"}),
            dcc.Interval(id="interval-component", interval=36000000, n_intervals=0),
        ]
    )


def generate_for_cluster(cluster, data):
    df = pd.read_json(data)

    # Sort by the end_date (convert to datetime object
    df["end_date"] = df["end_date"].apply(lambda x: convert_to_datetime(x))
    df.sort_values(["end_date"], inplace=True)

    # Convert back to string
    df["end_date"] = df["end_date"].apply(lambda x: convert_from_datetime(x))

    # Get data
    df["percent_alloc"] = df[cluster].apply(
        lambda x: 100.0 * float(x["mean_alloc"]) / float(x["mean_total"])
    )
    df["unique_users_count"] = df[cluster].apply(lambda x: x["unique_users_count"])
    # df["mean_wait_time"] = df[cluster].apply(lambda x: x["mean_wait_time"])

    # Generate a 6 week mean
    df["six_week_mean"] = df["percent_alloc"].rolling(window=window).mean().fillna(0.0)

    # Generate the traces
    traces = [
        go.Scatter(
            x=df["end_date"], y=df["percent_alloc"], name="used", mode="lines+markers"
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["unique_users_count"],
            name="unique users",
            mode="lines+markers",
            yaxis="y2",
        ),
        # go.Scatter(
        #    x=df["end_date"],
        #    y=df["mean_wait_time"],
        #    name="wait time (hrs)",
        #    mode="lines+markers",
        #    yaxis="y2",
        # ),
        go.Scatter(
            x=df["end_date"][6:],
            y=df["six_week_mean"][6:],
            name="rolling avg. (6 week)",
            mode="lines+markers",
        ),
    ]

    # The layout
    layout = go.Layout(
        title=cluster,
        titlefont={"size": 12},
        yaxis={
            "ticksuffix": "%",
            "title": "Percent",
            "titlefont": {"size": 12},
            "tickfont": {"size": 12},
            "range": [0, 100],
        },
        yaxis2={
            "title": "Number",
            "overlaying": "y",
            "side": "right",
            "titlefont": {"size": 12},
            "tickfont": {"size": 12},
            "range": [0, df["unique_users_count"].max() + 5],
        },
        xaxis={
            "title": "Week End Date (MM/DD/YY)",
            "tickangle": 45,  # 'nticks': 4,
            "titlefont": {"size": 12},
            "tickfont": {"size": 12},
        },
        legend={"font": {"size": 12}},
    )

    # Return the plotly data
    return {"data": traces, "layout": layout}


def generate_smp(data):
    return generate_for_cluster("smp", data)


def generate_gpu(data):
    return generate_for_cluster("gpu", data)


def generate_mpi(data):
    return generate_for_cluster("mpi", data)


def generate_htc(data):
    return generate_for_cluster("htc", data)


def generate_sus(data):
    df = pd.read_json(data)

    # Sort by the end_date (convert to datetime object
    df["end_date"] = df["end_date"].apply(lambda x: convert_to_datetime(x))
    df.sort_values(["end_date"], inplace=True)

    # Only keep end dates greater than 04/15/19-00:00:00
    df = df[df["end_date"] > convert_to_datetime("04/15/19-00:00:00")]

    # Convert back to string
    df["end_date"] = df["end_date"].apply(lambda x: convert_from_datetime(x))

    # Remove any blanks from theoretical_max_sus
    def remove_blanks(row):
        if row["smp"]["theoretical_max_sus"] == "":
            row["smp"] = {}
        return row

    df = df.apply(remove_blanks, axis=1)
    df = df[df["smp"] != {}]

    # Aggregate results
    def reduce_column(row, col):
        return reduce(lambda x, y: x + y, [float(row[c][col]) for c in clusters])

    def reduce_theoretical_max_sus(x):
        return reduce_column(x, "theoretical_max_sus")

    def reduce_consumed_sus(x):
        return reduce_column(x, "consumed_sus")

    df["theoretical_max_sus"] = df.apply(reduce_theoretical_max_sus, axis=1)
    df["consumed_sus"] = df.apply(reduce_consumed_sus, axis=1)

    df["projected"] = df["consumed_sus"].rolling(window=window).mean().fillna(0.0)

    rolling_points = df.shape[0] - window + 1

    traces = [
        go.Scatter(
            x=df["end_date"],
            y=df["theoretical_max_sus"] * 52.0,
            name="Total SUs / year",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["allocated_sus"],
            name="Currently Allocated",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["slurm_consumed"],
            name="Slurm Consumed SUs",
            mode="lines+markers",
        ),
        # go.Scatter(
        #    x=df["end_date"][-rolling_points:],
        #    y=df["projected"][-rolling_points:] * 52.0,
        #    name="Projected Consumed SUs",
        #    mode="lines+markers",
        # ),
    ]

    layout = go.Layout(
        title="Service Units",
        titlefont={"size": 12},
        yaxis={"title": "Number", "titlefont": {"size": 12}, "tickfont": {"size": 12}},
        xaxis={
            "title": "Week End Date (MM/DD/YY)",
            "tickangle": 45,  # 'nticks': 4,
            "titlefont": {"size": 12},
            "tickfont": {"size": 12},
        },
        legend={"font": {"size": 12}},
    )

    # Return the plotly data
    return {"data": traces, "layout": layout}


def generate_storage(data):
    df = pd.read_json(data)

    # Sort by the end_date (convert to datetime object
    df["end_date"] = df["end_date"].apply(lambda x: convert_to_datetime(x))
    df.sort_values(["end_date"], inplace=True)

    # Only keep end dates greater than 04/25/20-00:00:00
    df = df[df["end_date"] > convert_to_datetime("04/25/20-00:00:00")]

    # Convert back to string
    df["end_date"] = df["end_date"].apply(lambda x: convert_from_datetime(x))

    traces = [
        go.Scatter(
            x=df["end_date"], y=df["zfs1_used"], name="ZFS-1 Used", mode="lines+markers"
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["zfs1_total"],
            name="ZFS-1 Total",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["zfs1_committed"],
            name="ZFS-1 Committed",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"], y=df["zfs2_used"], name="ZFS-2 Used", mode="lines+markers"
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["zfs2_total"],
            name="ZFS-2 Total",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["zfs2_committed"],
            name="ZFS-2 Committed",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["bgfs_meta_used"],
            name="BGFS Metadata Used",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["bgfs_meta_total"],
            name="BGFS Metadata Total",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["bgfs_stor_used"],
            name="BGFS Storage Used",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["bgfs_stor_total"],
            name="BGFS Storage Total",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["bgfs_committed"],
            name="BGFS Committed",
            mode="lines+markers",
        ),
    ]

    layout = go.Layout(
        title="Storage Usage",
        titlefont={"size": 12},
        yaxis={
            "title": "Capacity (TB)",
            "titlefont": {"size": 12},
            "tickfont": {"size": 12},
        },
        xaxis={
            "title": "Week End Date (MM/DD/YY)",
            "tickangle": 45,  # 'nticks': 4,
            "titlefont": {"size": 12},
            "tickfont": {"size": 12},
        },
        legend={"font": {"size": 12}},
    )

    # Return the plotly data
    return {"data": traces, "layout": layout}


# Initialize the Dash app
app = dash.Dash(
    __name__, external_stylesheets=["https://codepen.io/barrymoo/pen/rbaKVJ.css"]
)
server = app.server

# Ready the database
uri = os.environ["MONGO_URI"]
client = pymongo.MongoClient(uri)
db = client.get_database()

# Time formats
parse_format = "%m/%d/%y-%H:%M:%S"
print_format = "%m/%d/%y"

# Clusters
clusters = ["smp", "gpu", "mpi", "htc"]

# Window
window = 6

initial_data = query_data()

# The app layout
app.layout = lambda: generate_layout(initial_data)


@app.callback(Output("data", "children"), [Input("interval-component", "n_intervals")])
def query_data_callback(_):
    return query_data()


# Update the plot every interval tick
@app.callback(
    Output("smp-graph", "figure"),
    [Input("interval-component", "n_intervals"), Input("data", "children")],
)
def update_smp(_, data):
    return generate_smp(data)


@app.callback(
    Output("gpu-graph", "figure"),
    [Input("interval-component", "n_intervals"), Input("data", "children")],
)
def update_gpu(_, data):
    return generate_gpu(data)


@app.callback(
    Output("mpi-graph", "figure"),
    [Input("interval-component", "n_intervals"), Input("data", "children")],
)
def update_mpi(_, data):
    return generate_mpi(data)


@app.callback(
    Output("htc-graph", "figure"),
    [Input("interval-component", "n_intervals"), Input("data", "children")],
)
def update_htc(_, data):
    return generate_htc(data)


@app.callback(
    Output("sus-graph", "figure"),
    [Input("interval-component", "n_intervals"), Input("data", "children")],
)
def update_sus(_, data):
    return generate_sus(data)


@app.callback(
    Output("storage-graph", "figure"),
    [Input("interval-component", "n_intervals"), Input("data", "children")],
)
def update_storage(_, data):
    return generate_storage(data)


# Our main function
if __name__ == "__main__":
    app.run_server()
