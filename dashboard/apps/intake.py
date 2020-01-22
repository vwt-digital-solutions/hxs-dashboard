from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from datetime import datetime as dt
import pandas as pd
import dash_bootstrap_components as dbc
from apps.elements import table_styles, styles
from apps.elements import get_filter_options
from app import app
from connection import Connection
from db.queries import read

filterchecklist_options = [
    {"label": "Connect", "value": "Aanvraagdatum Connect"},
    {"label": "ChangePoint", "value": "Aanvraagdatum CP"},
]

fix_column_order = [
    'bpnr',
    'con_opdrachtid',
    'categorie',
    'Aanvraagdatum Connect',
    'Aanvraagdatum CP',
    'Con. St. Aanvraag',
    'Projectstructuur constateringen',
]

option2 = 'Selecteer een Connect aanvraagstatus:'

query = """SELECT DISTINCT H.parentKindKey AS 'con_opdrachtid', M.value AS 'Con. St. Aanvraag' FROM czHierarchy H
    JOIN czImportKeys K
        ON K.sourceKey = H.kindKey
    JOIN czImportMeasureValues M
        ON M.importId = K.id
    WHERE
    H.versionEnd IS NULL
    AND M.measure = 'status_request'
    AND H.parentKind = 'con_opdrachtid'
    AND H.kind = 'con_objectid'
    AND K.versionEnd IS NULL
    AND H.parentKindKey IN ('{}')
"""

con_options = [
    'Opdracht',
    'Gereed',
    'Geen opdracht',
    'Controleren',
    'Offerte verzonden',
    'Geen status',
]


def get_body():
    intake_options = get_filter_options('dropdown_intake')

    intake_dropdown = dcc.Dropdown(
        id='intake_dropdown',
        options=[{'label': i, 'value': i}
                 for i in intake_options],
        multi=True,
        value=intake_options,
        style={'width': '90%', 'margin': '8px'},
    )

    intake_con_status = dcc.Dropdown(
        id='intake_con_status',
        options=[{'label': i, 'value': i}
                 for i in con_options],
        multi=True,
        value=con_options,
        style={'width': '90%', 'margin': '8px'},
    )

    from_date = dcc.DatePickerSingle(
        id='from_date_intake',
        date='2016-01-01',
        display_format='Y-M-D',
    )
    to_date = dcc.DatePickerSingle(
        id='to_date_intake',
        date=dt.now(),
        display_format='Y-M-D',
    )

    filter_checklist = dbc.FormGroup(
        [
            dbc.Label("Filter datum:"),
            dbc.Checklist(
                id='filter_date_system',
                options=filterchecklist_options,
                values=[],
                inline=True,
            ),
        ]
    )

    # Returned tab_connect
    intake_page = html.Div([
        html.Div(
            [
                html.P(""),
                html.H4('Selecteer een intake optie:',
                        className='lead',
                        ),
                intake_dropdown,
                html.P(""),
                html.H4(option2,
                        className='lead',
                        ),
                intake_con_status,
                dbc.Row([
                    dbc.Col([
                        html.A('Vanaf datum '),
                        from_date,
                    ]),
                    dbc.Col([
                        html.A('Tot en met datum '),
                        to_date,
                    ]),
                    dbc.Col([
                        filter_checklist,
                    ]),
                ]),
                html.P(""),
                html.H4(id='counter_intake',
                        className='lead',
                        ),
                dcc.Store(id='intake_store'),
            ],
            style=styles['box'],
        ),

        html.Div(id='intake_table',
                 style=styles['table_page'],
                 ),
    ]
    )

    return intake_page


@app.callback(
    [
        Output('intake_table', 'children'),
        Output('counter_intake', 'children'),
        Output('intake_store', 'data'),
    ],
    [
        Input('intake_dropdown', 'value'),
        Input('from_date_intake', 'date'),
        Input('to_date_intake', 'date'),
        Input('filter_date_system', 'values'),
        Input('intake_con_status', 'value'),
    ],
    [
        State('intake_store', 'data')
    ]
)
def upload_con(dd_intake, from_date, to_date, system, dd_con, df_in):
    if df_in is None:
        with Connection('r', 'read intake') as session:
            df = read(session, 'intake')
            q = query.format("','".join(df['con_opdrachtid'].fillna('').drop_duplicates().tolist()))
            opdrachten = pd.read_sql(q, session.bind)
        df = df.merge(opdrachten, how='left', on='con_opdrachtid')
        df_in = df.to_dict()
    else:
        df = pd.DataFrame(df_in)

    # Filter data
    df = df[df['categorie'].isin(dd_intake)]
    con_mask = df['Con. St. Aanvraag'].isin(dd_con)
    if 'Geen status' in dd_con:
        con_mask |= df['Con. St. Aanvraag'].isna()
    df = df[con_mask]
    mask = df['categorie'].notna()
    if len(system) > 0:
        for el in system:
            mask = mask & ((df[el] >= from_date) & (df[el] <= to_date))
    df = df[mask]

    # Fix column order
    df = df[[i for i in fix_column_order if i in list(df)]]
    counter = 'Aantal opdrachten: {}'.format(len(df))
    return [
        dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows"),
            sorting=True,
            filtering=True,
            style_table={'overflowX': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_cell_conditional=table_styles['cell']['conditional'],
            style_filter=table_styles['filter'],
            pagination_settings={
                "displayed_pages": 1,
                "current_page": 0,
                "page_size": 40,
            },
        ),
        counter,
        df_in,
    ]
