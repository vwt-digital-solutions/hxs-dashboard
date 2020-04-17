import dash_table
import pandas as pd

import dash_html_components as html

from apps.elements import styles, table_styles
from app import get_user
from db.models import czCleaning
from db.queries import read
from connection import Connection
import sqlalchemy as sa


def get_body():
    tab_user = html.Div(
        children=update_user_table(),
        id='user_table',
    )
    return tab_user


# Callback in datamanagement_lncpcon
def update_user_table():
    q_select = sa.select([czCleaning.key, czCleaning.status, czCleaning.updated]).\
        where(czCleaning.versionEnd.is_(None)).\
        where(czCleaning.kind == 'ln|cp|con')

    with Connection('r', 'read complete czCleaning') as session:
        status = pd.read_sql(q_select, session.bind).\
            fillna("").\
            rename(columns={'status': 'bewerkingsstatus'})

    user_title = html.Div(
        html.Div(
            html.H4("Door gebruiker '{}' in verschillende statussen geplaatst".format(get_user()), className='lead'),
            style={
                'margin-left': '10%',
                'margin-right': '10%',
                'margin-top': '2%',
                'margin-bottom': '2%',
            },
        ),
        style=styles['box_header'],
    )

    df = status[status['bewerkingsstatus'].str.contains('Control')]
    df = df.append(
        status[status['bewerkingsstatus'].str.contains(get_user().split('@')[0])]
        )
    df['updated'] = df['updated'] + pd.DateOffset(hours=1)
    df = df.rename(columns={
                'updated': 'sinds',
                })

    user_in_control = [user_title]
    if len(df) > 0:
        keys = df['key'].str.split('|', expand=True).rename(columns={
            0: 'ln_id',
            1: 'bpnr',
            2: 'con_opdrachtid',
        })
        with Connection('r', 'read overview: update_user_table') as session:
            overview = read(session, 'projectstructure', key=df['key'].tolist())
        if len(overview) > 0:
            overview = overview[[
                'ln_id',
                'bpnr',
                'con_opdrachtid',
                'categorie',
                'Projectstructuur constateringen',
                'koppeling'
            ]]
            dataframe = keys.join(df.drop('key', axis=1))
            dataframe = dataframe.merge(overview, on=['ln_id', 'bpnr', 'con_opdrachtid'])

            for status in ['In behandeling', 'Afgerond', 'Niet af te ronden']:
                temp = dataframe[dataframe['bewerkingsstatus'].str.contains(status)].drop('bewerkingsstatus', axis=1)
                if len(temp) > 0:
                    user_in_control.append(
                        html.Div(
                            html.H4(
                                "Status: {} [{}]".format(status, len(temp)),
                                className="lead",
                            ),
                            style={
                                'margin-left': '10%',
                                'margin-right': '10%',
                                'margin-top': '1%',
                                'margin-bottom': '1%',
                                'textAlign': 'center',
                            })
                    )
                    user_in_control.append(html.P(''))
                    user_in_control.append(
                        html.Div(
                            children=dash_table.DataTable(
                                    columns=[{"name": i, "id": i} for i in temp.columns],
                                    data=temp.to_dict("rows"),
                                    navigation="page",
                                    sorting=True,
                                    sort_by=[dict(column_id='sinds', direction='desc')],
                                    filtering=True,
                                    css=[{
                                        'selector': '.dash-cell div.dash-cell-value',
                                        'rule': 'display: inline; white-space: inherit; \
                                            overflow: inherit; text-overflow: inherit;'
                                    }],
                                    style_data={'whiteSpace': 'normal'},
                                    style_table={'overflowX': 'scroll'},
                                    style_as_list_view=True,
                                    style_header=table_styles['header'],
                                    style_cell=table_styles['cell']['problem'],
                                    style_filter=table_styles['filter'],
                                    style_cell_conditional=table_styles['cell']['conditional'],
                                ),
                            style=styles['table_page'],
                        )
                    )
                    user_in_control.append(html.Br())

            control = dataframe[dataframe['bewerkingsstatus'].str.contains('Control')]
            if len(control) > 0:
                user_in_control.append(
                    html.Div(
                        html.Div(
                            html.H4(
                                "Status: Control [{}]".format(len(control)),
                                className="lead",
                            ),
                            style={
                                'margin-left': '10%',
                                'margin-right': '10%',
                                'margin-top': '2%',
                                'margin-bottom': '2%',
                            },
                        ),
                        style=styles['box_header'])
                )
                user_in_control.append(html.P(""))
                user_in_control.append(
                    html.Div(
                        children=dash_table.DataTable(
                                columns=[{"name": i, "id": i} for i in control.columns],
                                data=control.to_dict("rows"),
                                navigation="page",
                                sorting=True,
                                sort_by=[dict(column_id='sinds', direction='desc')],
                                filtering=True,
                                css=[{
                                    'selector': '.dash-cell div.dash-cell-value',
                                    'rule': 'display: inline; white-space: inherit; overflow: inherit; \
                                        text-overflow: inherit;'
                                }],
                                style_data={'whiteSpace': 'normal'},
                                style_table={'overflowX': 'scroll'},
                                style_as_list_view=True,
                                style_header=table_styles['header'],
                                style_cell=table_styles['cell']['problem'],
                                style_filter=table_styles['filter'],
                                style_cell_conditional=table_styles['cell']['conditional'],
                            ),
                        style=styles['table_page'],
                    )
                )
                user_in_control.append(html.Br())

    return user_in_control
