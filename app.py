import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Event, Input
from datetime import datetime
import pandas as pd
import plotly.graph_objs as go
import pymongo
import os


def convert_from_datetime(d):
    return datetime.strftime(d, print_format)


def convert_to_datetime(d):
    return datetime.strptime(d, parse_format)


def generate_layout():
    return html.Div(
        children=[
            html.H1(children="CRC Weekly"),
            html.Div(
                [
                    html.Div(
                        [dcc.Graph(id="smp-graph", figure=generate_smp())],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="gpu-graph", figure=generate_gpu())],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="mpi-graph", figure=generate_mpi())],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="htc-graph", figure=generate_htc())],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                    html.Div(
                        [dcc.Graph(id="sus-graph", figure=generate_sus())],
                        style={"width": "49%", "display": "inline-block"},
                    ),
                ]
            ),
            dcc.Interval(id="interval-component", interval=60 * 60 * 1000),
        ]
    )


def generate_for_cluster(cluster):
    # Generate cursor and dataframe
    cursor = db["statistics"].find({"cluster": cluster}).sort("_id", pymongo.ASCENDING)
    df = pd.DataFrame.from_records(cursor)

    # Sort by the end_date (convert to datetime object
    df["end_date"] = df["end_date"].apply(lambda x: convert_to_datetime(x))
    df.sort_values(["end_date"], inplace=True)

    # Convert back to string
    df["end_date"] = df["end_date"].apply(lambda x: convert_from_datetime(x))

    # Generate a 6 week mean
    df["six_week_mean"] = df["allocated"].rolling(window=6).mean().fillna(0.0)

    # Generate the traces
    traces = [
        go.Scatter(
            x=df["end_date"], y=df["allocated"], name="used", mode="lines+markers"
        ),
        go.Scatter(x=df["end_date"], y=df["down"], name="down", mode="lines+markers"),
        go.Scatter(
            x=df["end_date"],
            y=df["unique_users"],
            name="unique users",
            mode="lines+markers",
            yaxis="y2",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["wait_time"],
            name="wait time (hrs)",
            mode="lines+markers",
            yaxis="y2",
        ),
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
            "range": [0, df["unique_users"].max() + 5],
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


def generate_smp():
    return generate_for_cluster("smp")


def generate_gpu():
    return generate_for_cluster("gpu")


def generate_mpi():
    return generate_for_cluster("mpi")


def generate_htc():
    return generate_for_cluster("htc")


def generate_sus():
    # Generate cursor and dataframe
    cursor = db2["sus"].find({}).sort("_id", pymongo.ASCENDING)
    df = pd.DataFrame.from_records(cursor)

    # Sort by the end_date (convert to datetime object
    df["end_date"] = df["end_date"].apply(
        lambda x: convert_to_datetime("{}-00:00:00".format(x))
    )
    df.sort_values(["end_date"], inplace=True)

    # Convert back to string
    df["end_date"] = df["end_date"].apply(lambda x: convert_from_datetime(x))

    traces = [
        go.Scatter(
            x=df["end_date"],
            y=df["sus_per_year"],
            name="Total SUs / year * 0.85",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"],
            y=df["alloc_sus"],
            name="Currently Allocated",
            mode="lines+markers",
        ),
        go.Scatter(
            x=df["end_date"], y=df["used_sus"], name="Used SUs", mode="lines+markers"
        ),
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


# Initialize the Dash app
app = dash.Dash(__name__)
server = app.server
# -> This part is important for Heroku deployment
server.secret_key = os.environ["SECRET_KEY"]

# Ready the database
uri = os.environ["MONGO_URI_STATS"]
client = pymongo.MongoClient(uri)
db = client.get_default_database()

uri2 = os.environ["MONGO_URI_SUS"]
client2 = pymongo.MongoClient(uri2)
db2 = client2.get_default_database()

# Time formats
parse_format = "%m/%d/%y-%H:%M:%S"
print_format = "%m/%d/%y"

# The app layout
app.layout = generate_layout


# Update the plot every interval tick
@app.callback(
    Output("smp-graph", "figure"), events=[Event("interval-component", "interval")]
)
def update_smp():
    return generate_smp()


@app.callback(
    Output("gpu-graph", "figure"), events=[Event("interval-component", "interval")]
)
def update_gpu():
    return generate_gpu()


@app.callback(
    Output("mpi-graph", "figure"), events=[Event("interval-component", "interval")]
)
def update_mpi():
    return generate_mpi()


@app.callback(
    Output("htc-graph", "figure"), events=[Event("interval-component", "interval")]
)
def update_htc():
    return generate_htc()


@app.callback(
    Output("sus-graph", "figure"), events=[Event("interval-component", "interval")]
)
def update_htc():
    return generate_sus()


# Our main function
if __name__ == "__main__":
    app.run_server()
