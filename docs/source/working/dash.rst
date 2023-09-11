.. -*- mode: rst -*-

Working with Dash and Plotly
============================
From  `Plotly's getting start guide: <https://plotly.com/python/getting-started/>`_ "The plotly Python library is an interactive, open-source plotting library that supports over 40 unique chart types covering a wide range of statistical, financial, geographic, scientific, and 3-dimensional use-cases."

These tools are usable in jupyter notebooks and can also be ran as their own standalone apps using plotly-dash.


Below are two examples using the standard API and the Arrow enabled API to retrieve data as a pandas.DataFrame, and then plotting the results.
These examples are based off of the `minimal dash app <https://dash.plotly.com/minimal-app>`_.

Non-Multistream API
-------------------

.. code-block:: python

    from dash import Dash, html, dcc, callback, Output, Input
    import plotly.express as px
    import pandas as pd
    import btrdb

    conn = btrdb.connect()
    streams = conn.streams_in_collection("YOUR_COLLECTION_HERE")
    streamset = conn.streams(*[s.uuid for s in streams])
    latest = streamset.latest()
    end = min([pt.time for pt in latest])
    start = end - btrdb.utils.timez.ns_delta(minutes=5)

    df = streamset.filter(start=start, end=end).to_dataframe()

    app = Dash(__name__)

    app.layout = html.Div([
        html.H1(children='Title of Dash App', style={'textAlign':'center'}),
        dcc.Dropdown(df.columns, id='dropdown-selection'),
        dcc.Graph(id='graph-content')
    ])

    @callback(
        Output('graph-content', 'figure'),
        Input('dropdown-selection', 'value')
    )
    def update_graph(value):
        dff = df[value]
        return px.line(dff, x=dff.index, y=value)

    if __name__ == '__main__':
        app.run(debug=True)



Multistream API
---------------
.. code-block:: python

    from dash import Dash, html, dcc, callback, Output, Input
    import plotly.express as px
    import pandas as pd
    import btrdb

    conn = btrdb.connect()
    streams = conn.streams_in_collection("YOUR_COLLECTION_HERE")
    streamset = conn.streams(*[s.uuid for s in streams])
    latest = streamset.latest()
    end = min([pt.time for pt in latest])
    start = end - btrdb.utils.timez.ns_delta(minutes=5)

    df = streamset.filter(start=start, end=end).arrow_to_dataframe()
    df = df.set_index('time')

    app = Dash(__name__)

    app.layout = html.Div([
        html.H1(children='Title of Dash App', style={'textAlign':'center'}),
        dcc.Dropdown(df.columns, id='dropdown-selection'),
        dcc.Graph(id='graph-content')
    ])

    @callback(
        Output('graph-content', 'figure'),
        Input('dropdown-selection', 'value')
    )
    def update_graph(value):
        dff = df[value]
        return px.line(dff, x=dff.index, y=value)

    if __name__ == '__main__':
        app.run(debug=True)
