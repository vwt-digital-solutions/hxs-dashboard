import dash_html_components as html
import dash_table
from datetime import datetime as dt
import pandas as pd
import sqlalchemy as sa
import dash_bootstrap_components as dbc
from db.models import czFilterOptions, czSubscriptions, czImportKeys
from connection import Connection


def generate_table(dataframe, table_id='table'):
    '''Algemene functie om een DataTable te maken van een dataframe'''
    return dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in dataframe.columns],
        data=dataframe.to_dict("rows"),
        sorting=True,
        style_table={'overflowX': 'scroll'},
        style_header={
            'backgroundColor': 'white',
            'fontWeight': 'bold',
            'borderBottom': '1px solid black'},
        style_cell={'font-family': 'helvetica', 'boxShadow': '0 0'},
    )


site_colors = {
    'black': '#000000',
    'indigo': '#1E22AA',
    'cyan': '#009FDF',
    'grey80': '#575757',
    'grey60': '#878787',
    'grey40': '#B2B2B2',
    'grey20': '#DADADA',
    'white': '#FFFFFF',
    'silver': '#AFA9A0',
}

# colors used for the marks in a graph
colors_graph = [
    '#808080',
    '#000000',
    '#ff0000',
    '#800000',
    '#ffff00',
    '#707030',
    '#00ee00',
    '#009000',
    '#00eeee',
    '#00a0a0',
    '#0000ff',
    '#000080',
    '#ff00ff',
    '#900090',
    '#00FF00',
    '#006400',
    '#008B8B',
    '#0000CD',
    '#DA70D6',
    '#4B0082',
    '#CD853F',
]

# layout for tables
styles = {
    'page': {
        'margin-left': '3%',
        'margin-right': '3%',
        'margin-top': '1%',
        "text-align": 'left',
    },
    'box': {
        'margin-left': '3%',
        'margin-right': '3%',
        'margin-top': '1%',
        'margin-bottom': '1%',
        'backgroundColor': site_colors['grey20'],
        'border-style': 'solid solid solid solid',
        'border-color': '#BEBEBE',
        'border-width': '1px',
        "text-align": "center",
    },
    'box_header': {
        'margin-left': '5%',
        'margin-right': '5%',
        'margin-top': '2%',
        'margin-bottom': '2%',
        'backgroundColor': site_colors['grey20'],
        'border-style': 'solid solid solid solid',
        'border-color': '#BEBEBE',
        'border-width': '1px',
        "text-align": "center",
    },
    'table_page': {
        'margin-left': '5%',
        'margin-right': '5%',
        'margin-top': '2%',
        'margin-bottom': '2%',
        'textAlign': 'center',
    },
    'graph_page': {
        'margin-left': '5%',
        'margin-right': '5%',
        'margin-top': '2%',
        'margin-bottom': '2%',
        'textAlign': 'center',
    },
    'alert': {
        'margin-left': '5%',
        'margin-right': '5%',
        'margin-top': '2%',
        'margin-bottom': '1%',
    },
}

table_style_header = {
    'backgroundColor': 'white',
    'fontWeight': 'bold',
    'borderBottom': '1px solid black',
    'color': site_colors['indigo'],
    'align': 'left',
}

table_style_cell_problem = {
    'font-family': 'helvetica',
    'boxShadow': '0 0',
    'backgroundColor': site_colors['grey20'],
    'textAlign': 'left',
    'minWidth': '100px',
    'maxWidth': '500px',
}

table_style_cell_action = table_style_cell_problem.copy()
table_style_cell_action['maxWidth'] = '300px'

table_styles = {
    'filter': {
        'font-family': 'helvetica',
        'backgroundColor': site_colors['grey20'],
        'textAlign': 'left',
    },
    'header': table_style_header,
    'cell': {
        'problem': table_style_cell_problem,
        'action': table_style_cell_action,
        'conditional': [
            {'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(256, 256, 256)'},
            {'if': {'row_index': 'even'},
                'backgroundColor': 'rgb(240, 240, 240)'},
            ],
    },
}


def button(children, _id='', backgroundcolor=site_colors['grey40'], textcolor=site_colors['white']):
    return html.Button(
        children,
        id=_id,
        style={
                'background-color': backgroundcolor,
                'color': textcolor,
                'border-radius': '8px',
                'display': 'inline-block',
                'padding': '7px',
                'text-align': 'center',
                'margin-top': '10px',
                'margin-bottom': '10px',
                'margin-left': '10px',
                'margin-right': '10px',
                },
    )


def alert(message, color, dismissable=True):
    return dbc.Alert(
        message,
        is_open=True,
        color=color,
        dismissable=True,
        style=styles['alert'],
    )


def get_filter_options(kind, session=None):
    q = sa.select([czFilterOptions.value]).\
        where(czFilterOptions.kind == kind)
    if session is None:
        with Connection('r', 'read dropdownvalues "{}"'.format(kind)) as session:
            sql = session.execute(q)
    else:
        sql = session.execute(q)

    values = [r for r, in sql]

    return values


def toggle(n, is_open):
    if n:
        return not is_open
    return is_open


def get_footer():
    with Connection('r', 'get_footer') as session:
        q = session.query(
            czSubscriptions.stagingSourceTag,
            czSubscriptions.sourceTag).\
            order_by(czSubscriptions.sourceTag.desc())
        tags = pd.read_sql(q.statement, session.bind)
        for i, row in tags.iterrows():
            q = session.query(
                czImportKeys.version).\
                filter(czImportKeys.sourceTag == row['stagingSourceTag']).\
                order_by(czImportKeys.version.desc()).\
                limit(1)
            version = [r for r, in session.execute(q)]
            if len(version) == 0:
                tags.at[i, 'version'] = ''
            else:
                tags.at[i, 'version'] = version[0]

    tags = tags.drop('stagingSourceTag', axis=1).rename(columns={
        'sourceTag': 'databron',
        'version': 'laatste nieuwe data',
    })

    table = dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in tags.columns],
            data=tags.to_dict("rows"),
            style_table={'overflowX': 'auto'},
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'borderBottom': '1px solid black',
                'align': 'center',
                'font-size': '12px',
            },
            style_cell={
                'font-family': 'helvetica',
                'boxShadow': '0 0',
                'backgroundColor': site_colors['grey20'],
                'textAlign': 'center',
                'minWidth': '100px',
                'maxWidth': '200px',
                'font-size': '12px',
            },
            style_cell_conditional=table_styles['cell']['conditional'],
        )

    return [
        html.Div(
            html.A("\u00a9 {} {}".format("VIQTOR DAVIS", str(dt.now().year))),
            style={
                'border-style': 'solid none solid none',
                'border-width': '1px',
            }
        ),
        html.Div(
            table,
            style={
                'margin-left': '30%',
                'margin-right': '30%',
                'margin-top': '2%',
                'margin-bottom': '2%',
                'textAlign': 'center',
            }
        ),
    ]
