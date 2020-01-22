import dash_table

import pandas as pd
import numpy as np
import time

import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc

from datetime import datetime as dt
from collections import OrderedDict
from dash.dependencies import Input, Output, State
from apps.elements import table_styles
from apps.datamanagement_problems import get_table_explain
from apps.elements import alert, styles, site_colors, button, get_filter_options, toggle
from app import app, get_user
from connection import Connection
from db.models import czHierarchy, czLog, czCleaning, czComment
from db.queries import read
import sqlalchemy as sa


system_tab = OrderedDict([
    ('ln', ['LN project', 'ln', 'InforLN', 'ln_table', 'dropdown_ln', 'ln_id', 'alert_clean_ln']),
    ('cp', ['CP nummer', 'cp', 'ChangePoint', 'cp_table', 'dropdown_cp', 'bpnr', 'alert_clean_cp']),
    ('connect', ['Connect Opdracht ID', 'connect', 'Connect', 'connect_table', 'dropdown1_con', 'con_opdrachtid', 'alert_clean_con']),
])


def get_dropdown(_id, column):
    return dcc.Dropdown(
        id=_id,
        options=[{'label': i, 'value': i}
                 for i in get_filter_options(_id)],
        value='',
    )


def get_state_dropdown(system):

    set_options = {
        'Geen': '',
        'In behandeling': 'In behandeling',
        'Afgerond': 'Afgerond',
        'Niet af te ronden': 'Niet af te ronden',
        'Control': 'Control'
    }

    return dcc.Dropdown(
        id='set_state_dropdown_' + system,
        options=[{'label': i, 'value': set_options[i]} for i in set_options],
        value='',
        style={'width': '90%', 'margin': '8px'},
    )


def get_state_button(system):
    return button(
        "Bevestig status",
        'button_status_' + system,
        backgroundcolor=site_colors['indigo'],
    )


def get_comment_collapse(system):
    return html.Div(children=[
            button(
                'Opmerking toevoegen',
                'toggle_comment_' + system,
                backgroundcolor=site_colors['indigo'],
            ),
            html.Br(),
            dbc.Collapse(
                dbc.Card([
                    dbc.Textarea(
                        id='textarea_comment_' + system,
                        className='mb-3',
                        placeholder="Vul je opmerking in",
                    ),
                    button(
                        'Opmerking opslaan',
                        'commit_comment_' + system,
                        backgroundcolor=site_colors['indigo']
                    )],
                    body=True,
                    style={
                        'backgroundColor': site_colors['grey20'],
                    }
                ),
                id='collapse_comment_' + system
            )
            ],
            style={
            "textAlign": "left",
        }
    )


def get_tab(system):
    naam, id_, label, table, dropdowna, dropdownb, alert = \
        system_tab.get(system)

    dropdown = get_dropdown(dropdowna, dropdownb)

    state_dropdown = get_state_dropdown(system)
    state_button = get_state_button(system)
    add_comment = get_comment_collapse(system)

    return dbc.Tab(
        html.Div([
            html.Div(
                id=alert,
                style=styles['alert'],
            ),
            html.Div([
                html.Div([
                    html.H4(
                        'Selecteer een {}:'.format(naam),
                        className="lead"),
                    dropdown,
                    ],
                    style={
                        'margin-left': '1%',
                        'margin-top': '1%',
                        'margin-bottom': '1%',
                        'textAlign': 'center',
                        'display': 'inline-block',
                        }),
                ],
                style=styles['box_header']),
            html.Div(
                [
                    html.Div([
                            html.H4('Selecteer de bewerkingsstatus waarin het {} zich bevindt'.format(
                                naam), className="lead"),
                            ],
                            style={'margin-left': '1%', 'margin-top': '1%', 'textAlign': 'left'}),
                    dbc.Row([
                        dbc.Col([
                            state_dropdown,
                        ]),
                        dbc.Col([
                            dbc.Row([
                                state_button,
                            ],
                                style={'text-align': 'left'},
                            )
                        ]),
                        dbc.Col([
                            add_comment,
                        ]),
                    ]),
                ],
                style=styles['box_header'],
            ),
            html.Div(
                id=table,
                style=styles['table_page'],
            ),
        ]),
        label=label,
        tab_id=id_,
        style={'tabClassName': "ml-auto"},
    )


def get_body():
    tab_lncpcon = html.Div([
        html.Div(id='test123'),
        html.Br(),
        dbc.Tabs(
            id="tabs_lncpcon",
            key='lncpcon',
            children=[get_tab(system) for system in system_tab.keys()],
        ),
    ])

    return tab_lncpcon


def get_table_noofobjects(df):
    connectids = list(set(df['con_opdrachtid']) - set(['', np.nan]))
    q = sa.select([czHierarchy.kindKey]).\
        where(czHierarchy.kind == 'con_objectid').\
        where(czHierarchy.parentKind == 'con_opdrachtid').\
        where(czHierarchy.parentKindKey.in_(connectids)).\
        where(czHierarchy.versionEnd.is_(None))

    no_of_object_groupby_cols = ['status_request', 'status_object', 'status_payment']
    table_connect = None
    if len(connectids) > 0:
        with Connection('r', 'get_no_of_object') as session:
            keys = [r for r, in session.execute(q)]
            # print('keys:', keys)
            dataframe = read(
                session, 'Connect',
                key=keys,
                measure=['con_objectid', 'con_opdrachtid'] + no_of_object_groupby_cols,
                ).\
                fillna('')

        if len(dataframe) > 0:
            cols = no_of_object_groupby_cols
            for col in cols:
                if col not in list(dataframe):
                    dataframe[col] = ''
            dataframe = dataframe.groupby(['con_opdrachtid'] + cols)['con_objectid'].count().reset_index()
            dataframe = dataframe.rename(columns={'con_objectid': 'aantal objecten'})
            table_explain = dash_table.DataTable(
                columns=[{"name": i, "id": i} for i in dataframe.columns],
                data=dataframe.to_dict("rows"),
                pagination_mode='fe',
                sorting=True,
                css=[{
                    'selector': '.dash-cell div.dash-cell-value',
                    'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                }],
                style_data={'whiteSpace': 'normal'},
                style_table={'overflowX': 'scroll'},
                style_as_list_view=True,
                style_header=table_styles['header'],
                style_cell=table_styles['cell']['action'],
                style_cell_conditional=table_styles['cell']['conditional'],
            )

            table_connect = html.Div(
                children=[
                    html.Br(),
                    html.H4(
                        "Aantal objecten per con_opdrachtid",
                        className="lead"),
                    html.P(""),
                    table_explain,
                ],
                )

    return table_connect


# Functies tabblad LNCPConnect
def get_table_partition_df(lnnr=None, bpnr=None, con_opdrachtid=None):
    with Connection('r', 'read overview: get_table_partition_df') as session:
        overview = read(session, 'projectstructure').fillna('')
        col_order = ['ln_id', 'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen', 'koppeling']
        overview = overview[col_order]
    # Create datatable
    if lnnr is not None:
        temp = overview[overview['ln_id'].isin(lnnr)]
    if bpnr is not None:
        temp = overview[overview['bpnr'].isin(bpnr)]
    if con_opdrachtid is not None:
        temp = overview[overview['con_opdrachtid'].isin(con_opdrachtid)]

    lnnr = set(temp['ln_id'].tolist()) - set(['', ' ', np.nan])
    bpnr = set(temp['bpnr'].tolist()) - set(['', ' ', np.nan])
    con_opdrachtid = set(temp['con_opdrachtid'].tolist()) - set(['', ' ', np.nan])

    # Extra iteration for extreme cases
    temp = overview[
        (overview['con_opdrachtid'].isin(con_opdrachtid)) |
        (overview['ln_id'].isin(lnnr)) |
        (overview['bpnr'].isin(bpnr))
    ]
    lnnr = set(temp['ln_id'].tolist()) - set([''])
    bpnr = set(temp['bpnr'].tolist()) - set([''])
    con_opdrachtid = set(temp['con_opdrachtid'].tolist()) - set([''])

    # Get dataframe
    dataframe = overview[
        (overview['con_opdrachtid'].isin(con_opdrachtid)) |
        (overview['ln_id'].isin(lnnr)) |
        (overview['bpnr'].isin(bpnr))
    ]

    dataframe['key'] = dataframe['ln_id'] + '|' + \
        dataframe['bpnr'] + '|' + dataframe['con_opdrachtid']

    return dataframe


def get_comments(df):
    q_select = sa.select([czComment.kind, czComment.kindKey, czComment.comment, czComment.user, czComment.created]).\
        where(sa.or_(
            sa.and_(czComment.kind == 'cpnr', czComment.kindKey.in_(df['bpnr'].tolist())),
            sa.and_(czComment.kind == 'con_opdrachtid', czComment.kindKey.in_(df['con_opdrachtid'].tolist())),
            sa.and_(czComment.kind == 'ln_id', czComment.kindKey.in_(df['ln_id'].tolist()))
        )).\
        where(czComment.versionEnd.is_(None))

    with Connection() as session:
        dataframe = pd.read_sql(q_select, session.bind)

    if len(dataframe) == 0:
        return None
    else:
        dataframe['opmerking bij'] = dataframe['kind'] + ' ' + dataframe['kindKey']
        dataframe['user'] = dataframe['user'].str.split('@', expand=True)[0]
        dataframe['created'] = dataframe['created'] + pd.DateOffset(hours=2)
        dataframe = dataframe[[
            'created',
            'user',
            'comment',
            'opmerking bij',
        ]].rename(columns={
            'user': 'door',
            'created': 'dag en tijd',
            'comment': 'opmerking',
        })

        return html.Div(
            children=[
                html.H4(
                    "Opmerkingen",
                    className="lead",
                ),
                html.P(""),
                dash_table.DataTable(
                    columns=[{"name": i, "id": i} for i in dataframe.columns],
                    data=dataframe.to_dict("rows"),
                    sorting=True,
                    sort_by=[dict(column_id='dag en tijd', direction='desc')],
                    css=[{
                        'selector': '.dash-cell div.dash-cell-value',
                        'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                    }],
                    style_data={'whiteSpace': 'normal'},
                    style_table={'overflowX': 'scroll'},
                    style_header=table_styles['header'],
                    style_cell=table_styles['cell']['problem'],
                ),
            ],
            style={
                "textAlign": "center"}
        )


def add_comment(kind, kindKey, comment):
    
    if comment is None:
        color = 'warning'
        message = 'Er is geen opmerking ingevoerd'
    elif len(comment) > 10000:
        color = 'warning'
        message = 'Het maximum van 10.000 letters in de opmerking is overschreden'
    elif (kindKey != '') and (comment != '') and (comment is not None) and (kindKey is not None):
        try:
            with Connection('w') as session:
                czComment.insert({
                    'kind': kind,
                    'kindKey': kindKey,
                    'comment': comment,
                }, session)
                czLog.insert([{
                    'action': 'add_comment',
                    'description': kind,
                    'parameter': kindKey,
                }], session)

            message = 'Opmerking "{}" toegevoegd aan {}'.format(
                comment,
                kindKey
            )
            color = 'success'
        except Exception as e:
            color = 'danger'
            message = 'Foutmelding, opmerking toevoegen mislukt'
    else:
        color = 'warning'
        message = 'Geen {} of opmerking ingevoerd'.format(kind)

    return [
        dbc.Alert(
            message,
            is_open=True,
            color=color,
            dismissable=True
        ),
    ]


def get_table_partition(status, lnnr=None, bpnr=None, con_opdrachtid=None):
    # Table with a set of correlated projects
    dataframe = get_table_partition_df(lnnr, bpnr, con_opdrachtid)
    dataframe = status.merge(dataframe, on='key', how='right').drop('key', axis=1)

    return [
        dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in dataframe.columns],
            data=dataframe.to_dict("rows"),
            sorting=True,
            css=[{
                'selector': '.dash-cell div.dash-cell-value',
                'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
            }],
            style_data={'whiteSpace': 'normal'},
            style_table={'overflowX': 'scroll'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['problem'],
            style_cell_conditional=table_styles['cell']['conditional'],
        ),
        html.Br(),
        get_comments(dataframe[['ln_id', 'bpnr', 'con_opdrachtid']]),
        get_table_noofobjects(dataframe),
        get_table_explain(dataframe),
    ]


def update_cleaning(system, value, set_state_dropdown):
    names = {
        'ln': 'InforLN project',
        'cp': 'ChangePoint bouwplannummer',
        'con': 'Connect Opdracht ID'
    }
    if system in names:
        name = names[system]
    else:
        raise ValueError('No correct system in function update_cleaning')

    if value != '':
        try:
            if system == 'ln':
                df = get_table_partition_df(lnnr=[value])
            elif system == 'con':
                df = get_table_partition_df(con_opdrachtid=[value])
            elif system == 'cp':
                df = get_table_partition_df(bpnr=[value])

            name = names[system]
            ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            new_status = ''
            if set_state_dropdown != '':
                new_status = set_state_dropdown + ' ' + get_user().split('@')[0]
            q_update = sa.update(czCleaning).\
                where(czCleaning.versionEnd.is_(None)).\
                where(czCleaning.kind == 'ln|cp|con').\
                where(czCleaning.key.in_(df['key'].tolist())).\
                values(status=new_status, updated=ts)

            with Connection('w', 'update czCleaning') as session:
                session.execute(q_update)

                czLog.insert([{
                    'action': 'update',
                    'description': "cleaning-ln|cp|con, system: {}, status: '{}'".format(system, set_state_dropdown),
                    'parameter': value,
                    'created': ts,
                }], session)

            color = 'success'
            if set_state_dropdown == '':
                set_state_dropdown = 'Geen'
            message = 'Status voor projecten gerelateerd aan {} {} succesvol geüpdate naar {}'.format(
                name, value, set_state_dropdown)
        except Exception as e:
            print(e)
            color = 'danger'
            message = 'Foutmelding, status niet geüpdate'
    else:
        color = 'warning'
        message = 'Geen {} gegeven'.format(name)

    return [
        alert(message, color)
    ]


@app.callback(
    Output('collapse_comment_connect', 'is_open'),
    [Input('toggle_comment_connect', 'n_clicks')],
    [State('collapse_comment_connect', 'is_open')],
)
def toggle_connect(n, is_open):
    return toggle(n, is_open)


@app.callback(
    Output('collapse_comment_ln', 'is_open'),
    [Input('toggle_comment_ln', 'n_clicks')],
    [State('collapse_comment_ln', 'is_open')],
)
def toggle_ln(n, is_open):
    return toggle(n, is_open)


@app.callback(
    Output('collapse_comment_cp', 'is_open'),
    [Input('toggle_comment_cp', 'n_clicks')],
    [State('collapse_comment_cp', 'is_open')],
)
def toggle_cp(n, is_open):
    return toggle(n, is_open)


@app.callback(
    [
        Output('ln_table', 'children'),
        Output('alert_clean_ln', 'children'),
        Output('cp_table', 'children'),
        Output('alert_clean_cp', 'children'),
        Output('connect_table', 'children'),
        Output('alert_clean_con', 'children'),
        Output('textarea_comment_ln', 'value'),
        Output('textarea_comment_cp', 'value'),
        Output('textarea_comment_connect', 'value'),
    ],
    [
        Input('dropdown_ln', 'value'),
        Input('button_status_ln', 'n_clicks_timestamp'),
        Input('commit_comment_ln', 'n_clicks_timestamp'),
        Input('dropdown_cp', 'value'),
        Input('button_status_cp', 'n_clicks_timestamp'),
        Input('commit_comment_cp', 'n_clicks_timestamp'),
        Input('dropdown1_con', 'value'),
        Input('button_status_connect', 'n_clicks_timestamp'),
        Input('commit_comment_connect', 'n_clicks_timestamp'),
    ],
    [
        State('set_state_dropdown_ln', 'value'),
        State('set_state_dropdown_cp', 'value'),
        State('set_state_dropdown_connect', 'value'),
        State('textarea_comment_ln', 'value'),
        State('textarea_comment_cp', 'value'),
        State('textarea_comment_connect', 'value'),
    ]
)
def update_ln_table(
    dropdown_value_ln, click_ts_ln, click_ts_comment_ln,
    dropdown_value_cp, click_ts_cp, click_ts_comment_cp,
    dropdown_value_con, click_ts_con, click_ts_comment_con,
    set_state_dropdown_ln, set_state_dropdown_cp, set_state_dropdown_con,
    text_ln, text_cp, text_con,
):
    now = str(int(time.time()))[:10]

    click_ts_ln = transform_timestamp(click_ts_ln)
    click_ts_comment_ln = transform_timestamp(click_ts_comment_ln)
    click_ts_cp = transform_timestamp(click_ts_cp)
    click_ts_comment_cp = transform_timestamp(click_ts_comment_cp)
    click_ts_con = transform_timestamp(click_ts_con)
    click_ts_comment_con = transform_timestamp(click_ts_comment_con)

    # for click in clicks:
    #     if click != None:
    #         click = str(click)[:10]
    #     else:
    #         click = 1

    # if click_ts_ln != None:
    #     click_ts_ln = str(click_ts_ln)[:10]
    # elif click_ts_ln == None:
    #     click_ts_ln = 1
    
    # if click_ts_comment_ln != None:
    #     click_ts_comment_ln = str(click_ts_comment_ln)[:10]
    # elif click_ts_comment_ln == None:
    #     click_ts_comment_ln = 1

    # if click_ts_cp != None:
    #     click_ts_cp = str(click_ts_cp)[:10]
    # elif click_ts_cp == None:
    #     click_ts_cp = 1
    
    # if click_ts_comment_cp != None:
    #     click_ts_comment_cp = str(click_ts_comment_cp)[:10]
    # elif click_ts_comment_cp == None:
    #     click_ts_comment_cp = 1

    # if click_ts_con != None:
    #     click_ts_con = str(click_ts_con)[:10]
    # elif click_ts_con == None:
    #     click_ts_con = 1
    
    # if click_ts_comment_con != None:
    #     click_ts_comment_con = str(click_ts_comment_con)[:10]
    # elif click_ts_comment_con == None:
    #     click_ts_comment_con = 1

    comment = dict(
        ln=text_ln,
        cp=text_cp,
        con=text_con
    )

    # First update data in database: only when the click is within 1s of the call of this function
    update_ln = None
    update_cp = None
    update_con = None

    if (int(now)-1) <= int(click_ts_ln):
        update_ln = update_cleaning(
            'ln', dropdown_value_ln, set_state_dropdown_ln)

    if (int(now)-1) <= int(click_ts_cp):
        update_cp = update_cleaning(
            'cp', dropdown_value_cp, set_state_dropdown_cp)

    if (int(now)-1) <= int(click_ts_con):
        update_con = update_cleaning(
            'con', dropdown_value_con, set_state_dropdown_con)

    if (int(now)-1) <= int(click_ts_comment_ln):
        update_ln = add_comment(
            'ln_id', dropdown_value_ln, comment['ln'])
        comment['ln'] = ''

    if (int(now)-1) <= int(click_ts_comment_cp):
        update_cp = add_comment(
            'cpnr', dropdown_value_cp, comment['cp'])
        comment['cp'] = ''

    if (int(now)-1) <= int(click_ts_comment_con):
        update_con = add_comment(
            'con_opdrachtid', dropdown_value_con, comment['con'])
        comment['con'] = ''

    # Get data from czCleaning
    q_select = sa.select([czCleaning.key, czCleaning.status, czCleaning.updated]).\
        where(czCleaning.versionEnd.is_(None)).\
        where(czCleaning.kind == 'ln|cp|con')

    with Connection('r', 'read complete czCleaning') as session:
        status = pd.read_sql(q_select, session.bind).\
            fillna("").\
            rename(columns={'status': 'bewerkingsstatus'})

    # Update tables
    table_ln = None
    if dropdown_value_ln != '':
        table_ln = get_table_partition(status.drop('updated', axis=1), lnnr=[dropdown_value_ln])

    table_cp = None
    if dropdown_value_cp != '':
        table_cp = get_table_partition(status.drop('updated', axis=1), bpnr=[dropdown_value_cp])

    table_con = None
    if dropdown_value_con != '':
        table_con = get_table_partition(status.drop('updated', axis=1), con_opdrachtid=[dropdown_value_con])

    return [
        table_ln,
        update_ln,
        table_cp,
        update_cp,
        table_con,
        update_con,
        comment['ln'], comment['cp'], comment['con'],
    ]

def transform_timestamp(ts):
    if ts is None:
        ts = 1
    else:
        ts = str(ts)[:10]
    return ts