import dash_table
import pandas as pd
import time
import traceback
import traceback
import sqlalchemy as sa
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from app import app
from dash.dependencies import Input, Output, State
from datetime import datetime as dt
from db.queries import read
from connection import Connection
from db.models import czHierarchy, czLog
from apps.elements import button, site_colors, styles, table_styles, toggle


SOURCETAG = "ChangePoint"


def get_body():
    # TABBLAD CP
    uitleg_cp = html.Div(
        html.A(
            [
                button(
                    'Uitleg {}'.format(SOURCETAG),
                    'uitleg_{}_button'.format(SOURCETAG),
                    site_colors['cyan']),
                dbc.Collapse(
                    dbc.Card(
                        dbc.CardBody("""
                        In de bovenstaande lijst kun je alle beschikbare ChangePoint bouwplannummers van aannemer veranderen.
                        Er zijn drie smaken: Connect-Z Utrecht, Connect-Z Montfoort of Others.
                        Klik op 'veranderen' om de verandering op te slaan.
                        """),
                        style={'background-color': site_colors['grey20']},
                    ),
                    id="uitleg_{}".format(SOURCETAG),
                )
            ]
        )
    )

    # Objects
    cp_dropdown1 = dcc.Dropdown(
        id='cp_dropdown1',
        style={'width': '90%', 'margin': '8px'},
    )

    choose_cpnr_radio = dbc.FormGroup(
        [
            dbc.Label('{} Nieuwbouw orders:'.format(SOURCETAG)),
            dbc.RadioItems(
                options=[
                    {'label': 'alle', 'value': 'all'},
                    {'label': 'zonder contractor', 'value': 'empty'},
                ],
                value='all',
                id='choose_cpnr',
            ),
        ]
    )

    contractor_dropdown = dcc.Dropdown(
        id='contractor_dropdown',
        style={'width': '90%', 'margin': '8px'},
    )

    plaats_invul = dcc.Input(
        id='plaats_invul',
        placeholder='Vul een plaatsnaam in',
        type='text',
        value='',
        style={'width': '90%', 'margin': '8px'},
    )

    contractor_button = button('Verander', 'contractor_button', site_colors['indigo'])

    # Returned tab_connect
    tab_cp = html.Div([
        dbc.Row([
            dbc.Col([
                html.Div(
                    [
                        html.P(''),
                        html.H4(
                            'Selecteer een {} bouwplannummer:'.format(SOURCETAG),
                            className='lead',
                        ),
                        cp_dropdown1,
                        choose_cpnr_radio,
                    ],
                    style=styles['box'],
                ),
                html.Div(
                    children=[
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Col([
                                    html.Div(
                                        html.H5(
                                            'Aannemer:',
                                            className='lead',
                                            style={'textAlign': 'center'},
                                        ),
                                    ),
                                    html.Div(contractor_dropdown, style={'textAlign': 'center'}),
                                    html.Div(
                                        html.H5(
                                            'Plaatsnaam:',
                                            className='lead',
                                            style={'textAlign': 'center'},
                                        ),
                                    ),
                                    html.Div(plaats_invul),
                                    html.Div(contractor_button, style={'textAlign': 'center'}),
                                    html.Div(uitleg_cp, style={'textAlign': 'center'}),
                                ]),
                            ],
                            style=styles['page'],
                        ),
                    ],
                    style=styles['box'],
                ),
            ],
                width={"size": 3, "order": 1}),


            dbc.Col([
                html.Div(
                    id='cp_content',
                    style=styles['table_page'])
            ],
                width={"size": 8, "order": 2},
            ),
        ]
        )
    ])

    return tab_cp


@app.callback(
    [
        Output('cp_dropdown1', 'options'),
        Output('cp_dropdown1', 'value'),
    ],
    [
        Input('choose_cpnr', 'value'),
    ],
)
def choose_cpnr(value):
    # Get all values:
    q_cpnr_all = sa.select([czHierarchy.parentKindKey, czHierarchy.kindKey]).\
        where(czHierarchy.kind == 'cp_id').\
        where(czHierarchy.parentKind == 'cpnr_extracted').\
        where(czHierarchy.versionEnd.is_(None)).\
        where(sa.not_(czHierarchy.parentKindKey.contains('REC'))).\
        distinct()

    q_cpnr = sa.select([czHierarchy.parentKindKey, czHierarchy.kindKey]).\
        where(czHierarchy.parentKind == 'contractor').\
        where(czHierarchy.kind == 'cpnr').\
        where(czHierarchy.versionEnd.is_(None))
        
    with Connection('r', 'get cpnrs') as session:
        cpnr_dropdown = pd.read_sql(q_cpnr_all, session.bind)

        if value == 'empty':
            with Connection('r', 'get contractors') as session:
                cpnr_contractor = pd.read_sql(q_cpnr, session.bind)
            cpnr_dropdown = cpnr_dropdown.merge(cpnr_contractor, how='left', left_on='parentKindKey', right_on='kindKey', suffixes=['', '_'])
            cpnr_dropdown = cpnr_dropdown[((cpnr_dropdown['parentKindKey_'].isna()) | (cpnr_dropdown['parentKindKey_'].isin(['', 'Connect-Z'])))]

    options = [{'label': el['parentKindKey'], 'value': el['kindKey'] + '|' + el['parentKindKey']}
               for el in cpnr_dropdown.to_dict(orient='records')]

    return options, '|'


@app.callback(
    [
        Output('cp_content', 'children'),
        Output('contractor_dropdown', 'value'),
        Output('contractor_dropdown', 'options'),
        Output('plaats_invul', 'placeholder'),
        Output('plaats_invul', 'value'),
    ],
    [
        Input('cp_dropdown1', 'value'),
        Input('contractor_button', 'n_clicks_timestamp'),
    ],
    [
        State('contractor_dropdown', 'value'),
        State('plaats_invul', 'value'),
        State('plaats_invul', 'placeholder')
    ]
)
def cp_page_content(value, ts_button, contractor, plaats_invul, plaats_invul_ph):
    now = int(str(int(time.time()))[:10])
    
    if ts_button != None:
        ts_button = int(str(ts_button)[:10])
    elif ts_button == None:
        ts_button = 1

    if value is None:
        value = '|'
    value = value.split('|')

    content = html.P('')
    new_value = None
    new_value_place = 'Vul een plaatsnaam in'
    options = []

    # Use placeholder for placename if already there
    if plaats_invul_ph != 'Vul een plaatsnaam in':
        if plaats_invul == '':
            plaats_invul = plaats_invul_ph

    if (value[0] != ''):
        # Get extra information from source
        measures = ['project_id', 'project_name', 'location', 'project_class', 'executing_partner']
        with Connection('r', '{} data {}'.format(SOURCETAG, value[0])) as session:
            df = read(session, SOURCETAG, key=value[0], measure=measures)

        try:
            q_select = sa.select([czHierarchy.kindKey, czHierarchy.kind, czHierarchy.parentKindKey, czHierarchy.parentKind]).\
                where(czHierarchy.versionEnd.is_(None)).\
                where(czHierarchy.kind == 'cpnr').\
                where(sa.or_(czHierarchy.parentKind == 'contractor',
                      czHierarchy.parentKind == 'plaats')).\
                where(czHierarchy.kindKey == value[1])

            with Connection('w', 'get contractor from czHierarchy') as session:
                dataframe = pd.read_sql(q_select, session.bind)
                # Add contractor and place
                if 'contractor' not in dataframe['parentKind'].tolist():
                    dataframe = dataframe.append(pd.DataFrame(
                        [[value[1], 'cpnr', '', 'contractor']], columns=list(dataframe)), ignore_index=True)
                old_contractor = dataframe.loc[dataframe['parentKind'] == 'contractor', 'parentKindKey'].values[0]
                if 'plaats' not in dataframe['parentKind'].tolist():
                    dataframe = dataframe.append(pd.DataFrame(
                        [[value[1], 'cpnr', '', 'plaats']], columns=list(dataframe)), ignore_index=True)
                old_place = dataframe.loc[dataframe['parentKind'] == 'plaats', 'parentKindKey'].values[0]
                
                # If button is pressed
                if (ts_button >= (now-1)) & ((old_contractor != contractor) | ((old_place) != plaats_invul)):
                    dataframe.at[dataframe['parentKind'] == 'plaats', 'parentKindKey'] = plaats_invul
                    dataframe.at[dataframe['parentKind'] == 'contractor', 'parentKindKey'] = contractor

                    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
                    q_update = sa.update(czHierarchy).\
                        where(czHierarchy.versionEnd.is_(None)).\
                        where(czHierarchy.kind == 'cpnr').\
                        where(czHierarchy.kindKey == value[1]).\
                        where(czHierarchy.parentKind.in_(dataframe['parentKind'].tolist())).\
                        values(versionEnd=ts)

                    session.execute(q_update)
                    czHierarchy.insert(dataframe, session, created=ts)
                    czLog.insert([{
                        'action': 'upload',
                        'description': 'cpnr-contractor&cpnr-place',
                        'parameter': contractor + '&' + plaats_invul,
                        'created': ts,
                    }], session)
                    session.commit()
                    dataframe = pd.read_sql(q_select, session.bind)

            new_value = dataframe.loc[dataframe['parentKind'] == 'contractor', 'parentKindKey'].values[0]
            new_value_place = dataframe.loc[dataframe['parentKind'] == 'plaats', 'parentKindKey'].values[0]
            new_value_place = 'Vul een plaatsnaam in' if new_value_place == '' else new_value_place

            companies = ['Connect-Z Utrecht', 'Connect-Z Montfoort', 'Other']
            options = [{'label': i, 'value': i} for i in companies]

            dataframe = dataframe.rename(columns={'kindKey': 'cpnr'}).drop('kind', axis=1)
            dataframe = dataframe.pivot(index='cpnr', columns='parentKind', values='parentKindKey').reset_index()

            content = [
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in dataframe.columns],
                    data=dataframe.to_dict("rows"),
                    sorting=True,
                    style_table={'overflowX': 'auto'},
                    style_header=table_styles['header'],
                    style_cell=table_styles['cell']['action'],
                    style_cell_conditional=table_styles['cell']['conditional'],
                )]
            if len(df) > 0:
                content.append(
                    html.Div(
                        html.H4(
                            '{}: {}'.format(
                                SOURCETAG,
                                df.reset_index().loc[0, 'project_name']
                            ),
                            className="lead",
                        ),
                        style={
                            'margin-left': '10%',
                            'margin-right': '10%',
                            'margin-top': '1%',
                            'margin-bottom': '1%',
                            'textAlign': 'center',
                        }
                    ))
                content.append(
                    dash_table.DataTable(
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict("rows"),
                        sorting=True,
                        style_table={'overflowX': 'auto'},
                        style_header=table_styles['header'],
                        style_cell=table_styles['cell']['action'],
                        style_cell_conditional=table_styles['cell']['conditional'],
                    ))
        except Exception as e:
            traceback.print_exc()
            content = [html.H4('An error occured, please retry')]
            
    return content, new_value, options, new_value_place, ''


@app.callback(
    Output("uitleg_{}".format(SOURCETAG), "is_open"),
    [Input("uitleg_{}_button".format(SOURCETAG), "n_clicks")],
    [State("uitleg_{}".format(SOURCETAG), "is_open")],
)
def toggle_collapse_cp(n, is_open):
    return toggle(n, is_open)
