from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import config
import pandas as pd
import traceback
import sys
import dash_bootstrap_components as dbc
from app import app
import sqlalchemy as sa
from connection import Connection
from db.models import czLog
from db.queries import compare_and_insert
from apps.elements import button, site_colors
import plotly.graph_objs as go

# Imports for data update cleaning
from db.queries import update_czCleaning, update_dropdown_values
from analysis.projectstructuur import compute_projectstucture, get_lncpcon_data, xaris
from analysis.fases_to_DB import compute_fases


opdracht_id = 'con_opdrachtid'
sourcetag1 = 'projectstructure_ln|cp|con'
sourcetag2 = 'fases_ln|cp'
sourcetag3 = 'fases_ln|con'
sourcetag4 = 'fases_xaris|con'
importmeasurevalue1 = 'intake_cp|con'
text6 = 'Start update czCleaning'
dropdown1 = 'dropdown_ln'
dropdown2 = 'dropdown_cp'
dropdown3 = 'dropdown1_con'

observation_messages = [
    'C03',
    'C07',
    'C08',
    'C10',
    'C11',
    'C18',
]


def get_body():
    jumbotron = html.Div(
        [
            dbc.Row(
                id='alert_reload',
                style={
                    'margin-left': '3%',
                    'margin-right': '3%',
                },
            ),
            html.Div(
                html.Img(src=app.get_asset_url(config.image),
                         style={
                    'width': '320px',
                    'align': 'right',
                }),
                className='display-3'
            ),
            html.P(
                "Applicatie voor het onderhouden van de projectstructuur",
                style={
                    'color': site_colors['indigo'],
                },
                className="lead",
            ),
            html.Hr(className="my-2"),
            dbc.Row([
                dbc.Col([
                    html.P(""),
                    html.P(""),
                    html.P(""),
                    html.H5("Werkwijze"),
                    html.P(""),
                    html.P(""),
                    html.P(""),
                    html.P(
                        "Onder het tabblad 'Projectstructuur - problemen' kan worden gefilterd op problemen in de projectstructuur.",
                        className="lead"),
                    html.P(
                        """Na de selectie van een probleem komen orders, bouwplannumers en/of projecten naar voren die vervolgens opgezocht kunnen worden
                        onder het tabblad 'Projectstructuur op project'.""",
                        className="lead"),
                    html.P(
                        "Op deze pagina staat de uitleg van het probleem en de actie die ondernomen moet worden.",
                        className='lead'),
                    html.P(
                        "Omvat de actie een aanpassing van het bouwplannummer in Connect, dan kan dit worden aangepast op het tabblad 'Connect - Objectniveau'.",
                        className="lead"),
                    html.A(button(
                            "Klik hier om te beginnen",
                            backgroundcolor=site_colors['indigo'],
                        ),
                        href='/apps/datamanagement_problems/'
                    ),
                    html.P(""),
                    html.A(button(
                            "Download uitgebreide uitleg",
                            backgroundcolor=site_colors['indigo'],
                        ),
                        href='/download?type=pdf&value=explain'
                    ),
                    html.P(""),
                    html.A(button(
                        "Reload data dashboard",
                        _id='reload_button',
                        backgroundcolor=site_colors['indigo'],
                    ))
                ]),
                dbc.Col([
                    html.Div(
                        update_cleanheid_tabel()
                    )]),
                ]),
            ],
        style={
            'margin-left': '5%',
            'margin-right': '5%',
            'margin-top': '2%',
            'margin-bottom': '2%',
            'backgroundColor': site_colors['grey20'],
            'border-style': 'solid solid solid solid',
            'border-color': '#BEBEBE',
            'border-width': '1px',
            'padding': '20px',
        },
    )
    return jumbotron


def update_cleanheid_tabel():

    with Connection('r', 'get data cleaning graph') as session:
        q = sa.select([czLog.description, czLog.created]).\
            where(czLog.action == 'reload_end')
    df = pd.read_sql(q, session.bind, coerce_float=False)
    
    mask = ((df['description'] != 'failure') & (df['description'].str.count(':') ==3))
    df.at[mask, 'description'] = df['description'] + '| _notes:0'

    try:
        df[['status', '_0', '_1', '_2', '_notes']] = df['description'].str.split('|', expand=True)
        df['_0'] = df['_0'].str.split(':').str[1]
        df['_1'] = df['_1'].str.split(':').str[1]
        df['_2'] = df['_2'].str.split(':').str[1]
        df['_notes'] = df['_notes'].str.split(':').str[1]
        df = df.dropna()

        x = df['created']
        trace_0 = go.Scatter(
            x=x,
            y=df['_0'],
            stackgroup='one',
            text="Geen foutmeldingen geconstateerd",
            hoverinfo=['text + y'],
            line=dict(
                color=site_colors['indigo'],
                width=1,
            )
        )

        trace_notes = go.Scatter(
            x=x,
            y=df['_notes'],
            stackgroup='one',
            text="Alleen één of meer constateringen",
            hoverinfo=['text + y'],
            line=dict(
                color='#6467e4',
                width=1,
            )
        )

        trace_1 = go.Scatter(
            x=x,
            y=df['_1'],
            stackgroup='one',
            text="Een foutmelding geconstateerd",
            hoverinfo=['text'],
            line=dict(
                color=site_colors['silver'],
                width=1,
            )
        )

        trace_2 = go.Scatter(
            x=x,
            y=df['_2'],
            stackgroup='one',
            text="Meerdere foutmeldingen geconstateerd",
            hoverinfo=['text + y'],
            line=dict(
                color=site_colors['white'],
                width=1,
            )
        )

        data = [trace_0, trace_notes, trace_1, trace_2]
        layout = go.Layout(
                title="Progressie datacleaning",
                xaxis=dict(
                            showgrid=False,
                            showline=False,
                            showticklabels=True,
                            zeroline=False,
                        ),
                yaxis=dict(
                            showgrid=False,
                            showline=True,
                            showticklabels=True,
                            zeroline=False,
                        ),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
        )

        graph = dcc.Graph(figure=go.Figure(data=data, layout=layout))

        return [
            html.Div(graph)
        ]
    except Exception as e:
        traceback.print_exc()
        return None


def update_data_dashboard():
    q = sa.select([czLog._id]).\
        order_by(sa.desc(czLog.created)).\
        limit(1)
    q_start = q.where(czLog.action == 'reload_start')

    with Connection('r', 'check reload available') as session:
        start = [r for r, in session.execute(q_start)]
        stop = 'stop'
        if len(start) != 0:
            q_stop = q.where(czLog.action == 'reload_end').\
                        where(czLog._id > start[0])
            stop = [r for r, in session.execute(q_stop)]

    if len(stop) > 0:
        # Data can be refreshed
        with Connection('w', 'reload_start') as session:
            czLog.insert(
                {
                    'action': 'reload_start',
                    'created': dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                session)
        try:
            data = get_lncpcon_data()
            overview, intake, = compute_projectstucture(data)

            xar = xaris()
            status_ln_cp, status_ln_con, relevante_xaris = compute_fases(data, overview, xar)

            projectstructuur_fouten = set([])
            for el in set(overview['Projectstructuur constateringen'].fillna('').unique())-set(['', ' ']):
                for ell in el.split('; '):
                    if ell != '':
                        projectstructuur_fouten |= set([ell])

            with Connection('w', 'update projectstructure') as session:
                # Update projectstructure
                print('Insert projectstructuur')
                overview['sourceKey'] = overview["ln_id"].fillna('') + '|' + overview["bpnr"].fillna('') + '|' + overview[opdracht_id].fillna('')
                compare_and_insert(
                    session,
                    overview,
                    sourceTag=sourcetag1,
                    sourceKey='sourceKey',
                    ts=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    load_type='diff',
                )
                session.commit()
                status_ln_cp['sourceKey'] = status_ln_cp["ln_id"] + '|' + status_ln_cp["bpnr"]
                compare_and_insert(
                    session,
                    status_ln_cp, sourceTag=sourcetag2,
                    sourceKey='sourceKey',
                    ts=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    load_type='diff',
                )
                session.commit()
                compare_and_insert(
                    session,
                    status_ln_con, sourceTag=sourcetag3,
                    sourceKey='sourceKey',
                    ts=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    load_type='diff',
                )
                session.commit()
                relevante_xaris['sourceKey'] = relevante_xaris['juist_nummer']
                compare_and_insert(
                    session,
                    relevante_xaris, sourceTag=sourcetag4,
                    sourceKey='sourceKey',
                    ts=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    load_type='diff',
                )
                session.commit()

                overview = overview.drop('sourceKey', axis=1)
                intake['sourceKey'] = intake["bpnr"].fillna('') + '|' + intake[opdracht_id].fillna('')
                compare_and_insert(session, intake, importmeasurevalue1)

                # Update all dropdowns:
                update_dropdown_values(dropdown1, overview["ln_id"].tolist(), session=session)
                update_dropdown_values(dropdown2, overview["bpnr"].tolist(), session=session)
                update_dropdown_values(dropdown3, overview[opdracht_id].tolist(), session=session)
                update_dropdown_values('category_dropdown', overview['categorie'].tolist(), session=session)
                update_dropdown_values('projectstructuur_fout_dropdown', projectstructuur_fouten, session=session)
                update_dropdown_values('dropdown_intake', intake['categorie'].drop_duplicates().tolist(), session=session)

                # Update the cleaning table
                print(text6)
                update_czCleaning(overview.fillna(''), session=session)

            overview['observation_count'] = 0
            for obs in observation_messages:
                mask = (overview['Projectstructuur constateringen'].str.contains(obs).fillna(False))
                overview.at[mask, 'observation_count'] = overview[mask]['observation_count'] + 1

            message = 'Data has been succesfully reloaded'
            color = 'success'
            success = 'success|_0:{}| _1:{}| _>1:{}| _notes:{}'.format(
                len(overview[overview['Projectstructuur constateringen'].isna()]),
                len(overview[
                    (overview['Projectstructuur constateringen'].str.count(';') == overview['observation_count'])
                ]),
                len(overview[(
                    (overview['Projectstructuur constateringen'].str.count(';') - overview['observation_count'] >= 1)
                )]),
                len(overview[
                    ((overview['Projectstructuur constateringen'].str.count(';')) - (overview['observation_count']) == -1)
                ]),
            )
            overview.drop(columns=['observation_count'], inplace=True)

        except Exception as e:
            print('Error in data update')
            print(e)
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            message = 'Data has not been succesfully reloaded, error in code'
            color = 'danger'
            success = 'failure'

        with Connection('w', 'reload_end') as session:
            czLog.insert(
                {
                    'action': 'reload_end',
                    'description': success,
                    'created': dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                session)

    else:
        # Data cannot be refreshed
        message = 'Data is being reloaded at the moment'
        color = 'warning'

    return [
        dbc.Alert(
            message,
            is_open=True,
            color=color,
            dismissable=True
        ),
    ]


# Callback reload_button
@app.callback(
    [
        Output('alert_reload', 'children'),
    ],
    [
        Input('reload_button', 'n_clicks_timestamp'),
    ],
)
def reload_data(reload_button):
    reloaded = update_data_dashboard()
    return reloaded
