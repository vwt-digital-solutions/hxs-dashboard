import dash
import dash_table
import pandas as pd
import io
import base64
import sqlalchemy as sa
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from app import app
from connection import Connection
from datetime import datetime as dt
from dash.dependencies import Input, Output, State
from db.queries import read
from db.models import czHierarchy, czLog
from apps.elements import table_styles, button, site_colors, styles, alert, toggle


SOURCETAG = 'Connect'


def get_body():
    uitleg_upload = html.Div(
        html.A(
            [
                button('Uitleg upload', 'uitleg_upload_button', site_colors['cyan']),
                dbc.Collapse(
                    dbc.Card(
                        dbc.CardBody("""
                        In de tabel hiernaast is voor alle objecten binnen een \
                        Connect opdracht het Bouwplannummer weergegeven.
                        In de kolom 'con_opdrachtid' staat het opdrachtnummer aangegeven, \
                        in 'con_objectid' het objectnummer.
                        De kolom 'cpnr_extracted' geeft aan welk ChangePoint nummer er is \
                        bepaald uit het veld 'Bouwplannummer' van Connect.
                        De kolom 'cpnr_corrected' geeft aan welk ChangePoint nummer er op dit moment wordt gebruikt in \
                        het bepalen van de projectstructuur.

                        Het kan zijn dat er een onjuist bouwplannummer aan een Connect object is verbonden.
                        Deze kun je wijzigen door het juiste bouwplannummer up te loaden.
                        De Connect order kun je downloaden met de knop 'Download Connect opdracht xxx'.
                        De excel die wordt gedownload kun je aanpassen.
                        Alleen aanpassingen in de gele kolom ('cpnr_corrected') worden opgenomen in de database.
                        Als je je aanpassingen hebt gemaakt, \
                        sla je het bestand op en load je het up via de knop 'Upload correctie Connect'.
                        """),
                        style={'background-color': site_colors['grey20']},
                    ),
                    id="uitleg_upload",
                )
            ]
        )
    )

    # queries
    q_conids = sa.select([czHierarchy.parentKindKey]).\
        where(czHierarchy.parentKind == 'con_opdrachtid').\
        where(czHierarchy.versionEnd.is_(None)).distinct()
    with Connection('r', 'read_conids') as session:
        con_ids_dropdown = [r for r, in session.execute(q_conids)]

    # Objects
    button_upload = dcc.Upload(
        id='con_upload',
        multiple=False,
        children=html.Div([
                    html.A('Drag and drop or click here.')
                 ],
                 style={
                    'textAlign': 'center',
                 }
        ),
        style={
            'width': '90%',
            'padding': '10px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px',
            },
    )

    con_dropdown = dcc.Dropdown(
        id='con_dropdown',
        options=[{'label': i, 'value': i}
                 for i in con_ids_dropdown],
        value='',
        style={'width': '90%', 'margin': '8px'},
    )

    tab_con = html.Div([
        # html.Br(),
        html.Div(id='upload_alert_el'),
        dbc.Row([
            dbc.Col([
                html.Div(
                    [
                        html.P(""),
                        html.H4('Selecteer een {}opdrachtnummer:'.format(
                            SOURCETAG
                        ),
                            className='lead',
                            ),
                        con_dropdown,
                        html.A(id='con_downloadlink'),
                    ],
                    style=styles['box'],
                ),
                html.Div(
                    children=[
                        html.Br(),
                        button_upload,
                        html.Br(),
                        uitleg_upload,
                    ],
                    style=styles['box'],
                ),
            ],
                width={"size": 3, "order": 1}),


            dbc.Col([
                html.Div(id='con_table',
                         style=styles['table_page']),
            ],
                width={"size": 8, "order": 2},
            ),
        ]
        )
    ])

    return tab_con


def get_con_df(value, short=True):
    measures = []
    if short:
        measures = ['con_opdrachtid', 'con_objectid', 'build_plan_no']

    # Returns the dataframe with the current coupling of the objects
    with Connection('r', 'read con data') as session:
        q = sa.select([czHierarchy.kindKey]).\
            where(czHierarchy.parentKindKey == str(value)).\
            where(czHierarchy.parentKind == 'con_opdrachtid').\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.versionEnd.is_(None))
        keys = [r for r, in session.execute(q).fetchall()]

        q = sa.select([czHierarchy.kindKey, czHierarchy.parentKind, czHierarchy.parentKindKey]).\
            where(czHierarchy.kindKey.in_(keys)).\
            where(czHierarchy.parentKind.in_(['cpnr_corrected', 'cpnr_extracted'])).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.versionEnd.is_(None))
        dataframe = pd.read_sql(q, session.bind, coerce_float=False)

        con = read(session, sourceTag=SOURCETAG, key=keys, measure=measures)

    dataframe = dataframe.pivot(index='kindKey', columns='parentKind', values='parentKindKey').reset_index()
    dataframe = dataframe.rename(columns={'kindKey': 'con_objectid'}).fillna('')

    # Add missing columns (for missing data)
    cols = [
        'cpnr_extracted',
        'cpnr_corrected',
    ]

    # Merge dataframes
    if len(dataframe) > 0:
        dataframe = dataframe.merge(con, on='con_objectid', how='left')
    else:
        dataframe = con

    # Correct column order
    cols.insert(0, 'con_objectid')
    cols.insert(1, 'con_opdrachtid')
    cols.append('build_plan_no')
    for col in list(dataframe):
        if col not in cols:
            cols.append(col)

    for col in cols:
        if col not in list(dataframe):
            dataframe[col] = ''

    return dataframe[cols]


@app.callback(  # Create download link
    dash.dependencies.Output('con_downloadlink', 'children'),
    [dash.dependencies.Input('con_dropdown', 'value')])
def update_cp_fase(con_dropdown_value):
    if con_dropdown_value != '':
        return html.A(
            id='download',
            href='/download?type=hFwmotkaNo&value={}'.format(con_dropdown_value),
            children=html.A(
                button(
                    children='Download {}opdracht {}'.format(
                        SOURCETAG,
                        con_dropdown_value),
                    _id='con-download-button',
                    backgroundcolor=site_colors['indigo'],
                    )
            )
        )
    else:
        return html.P("")


def parse_con_upload(contents, filename, session):
    content_string = contents.split(',')[1]
    # content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)

    try:
        # Lees het bestand
        df = pd.read_excel(
            io.BytesIO(decoded),
            converters={
                'con_opdrachtid': str,
                'con_objectid': str,
                'cpnr_corrected': str,
            }).fillna("")

        # Check of er data in zit:
        if len(df) == 0:
            return 'De geüploadde tabel is leeg.'

        # Check of de juiste kolommen erin zitten
        for col in ['con_opdrachtid', 'cpnr_corrected', 'con_objectid']:
            if col not in df.columns.tolist():
                return 'De kolom {} ontbreekt in de upload.'.format(col)

        # Check of er maar een opdracht_id in zit
        value = df['con_opdrachtid'].unique()
        if len(value) != 1:
            return "De kolom 'con_opdrachtid' is niet juist gevuld, er zijn meerdere opdracht ID's gegeven"
        value = value[0]

        # Check of er maar een cpnr_corrected in zit
        cpnr = df['cpnr_corrected'].unique()
        if len(cpnr) != 1:
            return "De kolom 'cpnr_corrected' is niet juist gevuld, er zijn meerdere bouwplannummers gegeven"

        # Download overzicht van de objecten zoals die nu geregisteerd staan in de database
        q_check = sa.select([czHierarchy.kindKey, czHierarchy.kind]).\
            where(czHierarchy.parentKind == 'con_opdrachtid').\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKindKey == str(value)).\
            where(czHierarchy.versionEnd.is_(None))

        # Controleer op aantallen
        db_check = pd.read_sql(q_check, session.bind)

        if len(db_check) != len(df):
            return "Het aantal gegeven {}objecten binnen de {}opdracht komt niet overeen met de huidige situatie". \
                format(
                    SOURCETAG,
                    SOURCETAG
                )

        # Controleer of de objecten gelijk zijn
        if len(set(db_check['kindKey']) & set(df['con_objectid'])) != len(db_check):
            return "De {}objecten in de upload komen niet overeen met de objecten in de database. \
                Download {}opdracht {} opnieuw en vul de juiste waarden in.". \
                    format(
                        SOURCETAG,
                        SOURCETAG,
                        value
                    )

        return df

    except Exception as e:
        print(e)
        return 'Bestand kan niet als excel-bestand worden gelezen.'


@app.callback(
    [
        Output('upload_alert_el', 'children'),
        Output('con_table', 'children'),
    ],
    [
        Input('con_dropdown', 'value'),
        Input('con_upload', 'contents'),
    ],
    state=[
        State('con_upload', 'filename'),
    ],
)
def upload_con(con_dropdown_value, contents, filename):
    # First process upload
    upload = None
    if contents is not None:
        with Connection('r', 'check {} upload'.format(SOURCETAG)) as session:
            df = parse_con_upload(contents, filename, session)
        if isinstance(df, pd.DataFrame):
            try:
                with Connection('w', '{} upload'.format(SOURCETAG)) as session:
                    value = update_con_cpnr(df, session)
                message = "Upload of {} succesfull. The data for {} Order ID {} has been updated.\t".format(
                    SOURCETAG,
                    filename, value)
                color = 'success'
            except Exception as e:
                message = "Upload of {} unsuccesfull: Connection with database failed\t".format(
                    filename)
                color = 'danger'
                print("Error in datamanagement_connect.py, fuction upload_con()")
                print(e)
        else:
            message = "Upload of {} unsuccesfull: {}\t".format(filename, df)
            color = 'danger'

        upload = [alert(message, color)]

    # Determine datatable (with refreshed data)
    table = None
    if con_dropdown_value != '':
        dataframe = get_con_df(con_dropdown_value)
        dataframe['cpnr_corrected'].replace('', '<empty>', inplace=True)
        dataframe['cpnr_corrected'].fillna('deleted', inplace=True)

        table = dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in dataframe.columns],
            data=dataframe.to_dict("rows"),
            sorting=True,
            style_table={'overflowX': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_cell_conditional=table_styles['cell']['conditional'],
        )

    # Return upload and table in the same order as the output
    return upload, table


def update_con_cpnr(df, session):
    value = str(df['con_opdrachtid'].values[0])
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    # upload new values to czHierarchy
    # set to version end
    setVersionEnd = df['con_objectid'].tolist()
    print('set to versionEnd {}: {}'.format(ts, ', '.join(setVersionEnd)))
    q_update = sa.update(czHierarchy).\
        where(czHierarchy.versionEnd.is_(None)).\
        where(czHierarchy.kind == 'con_objectid').\
        where(czHierarchy.kindKey.in_(setVersionEnd)).\
        where(czHierarchy.parentKind == 'cpnr_corrected').\
        values(versionEnd=ts)
    session.execute(q_update)
    session.flush()
    print('Update con_cpnr flushed')

    # upload new values in czHierarchy
    df = df[['cpnr_corrected', 'con_objectid']].rename(
        columns={'cpnr_corrected': 'parentKindKey', 'con_objectid': 'kindKey'})
    df['kind'] = 'con_objectid'
    df['parentKind'] = 'cpnr_corrected'
    czHierarchy.insert(df, session, created=ts)

    # Log upload in czLog
    czLog.insert(
        {
            'action': 'upload',
            'description': 'conobjid-cpnr',
            'parameter': value,
            'created': ts,
        },
        session)

    return value


@app.callback(  # Toggle explaination upload
    Output("uitleg_upload", "is_open"),
    [Input("uitleg_upload_button", "n_clicks")],
    [State("uitleg_upload", "is_open")],
)
def toggle_collapse_con(n, is_open):
    return toggle(n, is_open)
