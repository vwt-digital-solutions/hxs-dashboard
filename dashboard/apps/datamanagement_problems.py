import dash_table

import pandas as pd
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc

from dash.dependencies import Input, Output, State
from apps.elements import styles, table_styles, button, site_colors
from apps.elements import get_filter_options
from app import app
from db.models import czCleaning
from db.queries import read
import sqlalchemy as sa
from connection import Connection

import utils
import config


def get_body():
    fouten = get_filter_options('projectstructuur_fout_dropdown') + ['Geen constateringen']
    projectstructuur_fout_dropdown = dcc.Dropdown(
        id='projectstructuur_fout_dropdown',
        options=[
            {'label': i, 'value': i} for i in fouten
        ],
        value=[],
        style={
            'width': '80%',
            'margin': '8px',
            'margin-left': '12%',
            'margin-right': '15%',
        },
        multi=True,
    )

    categories = get_filter_options('category_dropdown')
    category_dropdown = dcc.Dropdown(
        id='category_dropdown',
        options=[
            {'label': i, 'value': i} for i in categories
        ],
        value=categories,
        style={
            'width': '80%',
            'margin': '8px',
            'margin-left': '12%',
            'margin-right': '15%',
        },
        multi=True,
    )

    # Tabblad Problemen
    tab_problems = \
        html.Div([
            html.Div([
                html.Div(
                    [html.H4(
                        "Selecteer hieronder een combinatie van constateringen",
                        className="lead",
                            )
                     ],
                    style={
                        'textAlign': 'center',
                        'margin-top': '1%',
                        },
                    ),
                dbc.Row([
                    projectstructuur_fout_dropdown,
                ],
                ),
                ],
                style=styles['box_header'],
            ),
            html.Div([
                html.Div(
                            [
                                html.H4('Selecteer een of meerdere categoriën',
                                        className='lead',
                                        )
                             ],
                            style={
                                     'textAlign': 'center',
                                     'margin-top': '1%',
                                   },
                         ),
                dbc.Row([category_dropdown]),
            ],
                style=styles['box_header'],
            ),
            html.Div(id='problem_count',
                     style=styles['box_header']),
            html.Div(
                html.Div([
                    dash_table.DataTable(
                        id='datatable_problem',
                        pagination_mode='fe',
                        filtering=True,
                        pagination_settings={
                            "displayed_pages": 1,
                            "current_page": 0,
                            "page_size": 40,
                        },
                        navigation="page",
                        sorting=True,
                        sort_by={},
                        css=[{
                            'selector': '.dash-cell div.dash-cell-value',
                            'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'
                        }],
                        style_data={'whiteSpace': 'normal'},
                        style_table={'overflowX': 'scroll'},
                        style_as_list_view=True,
                        style_header=table_styles['header'],
                        style_cell=table_styles['cell']['problem'],
                        style_filter=table_styles['filter'],
                        style_cell_conditional=table_styles['cell']['conditional'],
                    ),
                    html.Div(id='problem_user_table'),
                    ],
                ),
                style=styles['table_page'],
            )
            ]
        )

    return tab_problems


def get_problem_table(overview, pr_dropdown_values, category_values, status):
    mask = overview.index.notna()
    for el in pr_dropdown_values:
        if el == 'Geen constateringen':
            mask &= ((overview['Projectstructuur constateringen'] == '') |
                     (overview['Projectstructuur constateringen'].isna()))
        else:
            mask &= (overview['Projectstructuur constateringen'].str.contains(el, regex=False))
    mask &= (overview['categorie'].isin(category_values))

    temp = overview[mask].fillna('')
    temp.at[:, 'key'] = temp.loc[:, 'ln_id'] + '|' + \
        temp.loc[:, 'bpnr'] + '|' + temp.loc[:, 'con_opdrachtid']

    temp = status.merge(temp, on='key', how='right').drop('key', axis=1).fillna('')

    return temp


# Functies tabblad problems
def update_problem_table(pr_dropdown_values, category_values, status):
    with Connection('r', 'read overview: update_problem_table') as session:
        overview = read(session, 'projectstructure')

    col_order = ['ln_id', 'bpnr', 'con_opdrachtid', 'categorie',
                 'Projectstructuur constateringen', 'koppeling']
    overview = overview[col_order]
    dataframe = get_problem_table(
        overview, pr_dropdown_values, category_values, status)

    table_data = {
        'columns':
            [{"name": i, "id": i} for i in dataframe.columns],
        'data':
            dataframe.to_dict("rows"),
        }

    table_user = get_table_explain(dataframe),

    counter = [
        dbc.Row(
            [
             dbc.Col(
                html.Div(
                    html.P('''
                        {}/{} combinaties |
                        {}/{} afgerond |
                        {}/{} LNnummers |
                        {}/{} CPnummers |
                        {}/{} Connect Opdrachten
                    '''.format(
                        len(dataframe), len(overview),
                        len(dataframe[dataframe['bewerkingsstatus'].str.startswith('Afgerond')]), len(dataframe),
                        len(set(dataframe['ln_id']) - set([''])), len(set(overview['ln_id']) - set([''])),
                        len(set(dataframe['bpnr']) - set([''])), len(set(overview['bpnr']) - set([''])),
                        len(set(dataframe['con_opdrachtid']) - set([''])),
                        len(set(overview['con_opdrachtid']) - set([''])),
                        ),
                        # className="lead",
                    ),
                    style={
                        'textAlign': 'center',
                        'margin-top': '1.5%',
                    },
                ),
                width={'size': 10, 'order': 1}
             ),
             dbc.Col(
                html.Div(
                    html.A(
                        button("Download", backgroundcolor=site_colors['indigo']),
                        href='/download?type=problem&category={}&problems={}&'.format(
                            '|'.join(category_values),
                            '|'.join(pr_dropdown_values),
                        ),
                    ),
                    style={
                        'textAlign': 'center',
                        },
                ),
                width={'size': 2, 'order': 10}
                ),
            ]
        ),
    ]

    return table_data, table_user, counter


def get_table_explain(dataframe):
    explain = set()
    for el in set(dataframe['Projectstructuur constateringen'].fillna('').unique())-set(['']):
        for ell in el.split('; '):
            explain |= set([ell])
    explain -= set(['', ' '])

    df_explain = utils.download_as_dataframe(config.tmp_bucket, config.files['actions'])
    dataframe = df_explain[df_explain['flag'].isin(explain)]
    table_explain = None
    if len(dataframe) > 0:
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

        return html.Div(
            children=[
                html.Br(),
                html.H4(
                    "Uitleg verschillende acties",
                    className="lead"),
                html.P(""),
                table_explain,
            ],
        )
    else:
        return None


@app.callback(
    [
        Output('datatable_problem', 'data'),
        Output('datatable_problem', 'columns'),
        Output('problem_count', 'children'),
        Output('problem_table', 'sort_by'),
        Output('problem_table', 'pagination_settings'),
    ],
    [
        Input('projectstructuur_fout_dropdown', 'value'),
        Input('category_dropdown', 'value'),
    ],
    [
        State('datatable_problem', 'sort_by'),
        State('datatable_problem', 'pagination_settings'),
    ]
)
def update_ln_table(
    # Input dropdowns
    pr_dropdown_values,
    category_values,

    # # Table settings
    problem_table_sort_by,
    problem_table_pagination,
):
    # Get data from czCleaning
    q_select = sa.select([czCleaning.key, czCleaning.status, czCleaning.updated]).\
        where(czCleaning.versionEnd.is_(None)).\
        where(czCleaning.kind == 'ln|cp|con')

    with Connection('r', 'read complete czCleaning') as session:
        status = pd.read_sql(q_select, session.bind).\
            fillna("").\
            rename(columns={'status': 'bewerkingsstatus'})

    problem = update_problem_table(
        pr_dropdown_values, category_values, status.drop('updated', axis=1))

    return [
            problem[0]['data'],
            problem[0]['columns'],
            problem[2],
            problem_table_sort_by,
            problem_table_pagination,
    ]
