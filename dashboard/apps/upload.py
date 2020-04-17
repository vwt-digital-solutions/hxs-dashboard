from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from datetime import datetime as dt
import pandas as pd
import io
import time
import traceback
import base64
import dash_bootstrap_components as dbc
from apps.elements import button, site_colors, styles, alert, toggle
from app import app
from connection import Connection
from db.models import czLog
from google.cloud import storage
from iap import make_iap_request
import config

SOURCETAG = "ChangePoint"

cardbody_cp = """
Verwachte kolommen ChangePoint:

User-defined project ID,
Project name,
Project Classification,
HoofdProjectNummer,
Location,
Delivery Track,
Platform,
Related Program,
ERP Project manager,
ERP Projectleider,
Build Manager (BM),
Delivery Manager (DM),
Build Supervisor (BS),
Network Planner (NP),
Netwerk Expert (NE),
Technician Build (TB),
Partner,
Fase,
Verwacht Technisch Gereed,
Last Approved Budget,
Aantal aansluitingen,
Step name,
Current assignee,
Date assigned,
aantal,
Datum_1e_Oplevering,
Opmerking ziggo,
Opmerking aan,
Vermoedelijke Start Aanleg,
Start Aanleg,
Aanleg Hoofdnet gereed,
Revisie Verwerkt,
Status Bouwplan,
Date Created,
Regio,
Status Opdracht
"""


def get_body():
    uitleg_cp = [
        button(SOURCETAG, 'uitleg_upload_st1_button', site_colors['cyan']),
        dbc.Collapse(
            dbc.Card(
                dbc.CardBody(cardbody_cp),
                style={
                    'background-color': site_colors['grey20']},
            ),
            id='uitleg_upload_st1',
        ),
    ]

    uitleg_upload_gcloud = html.Div(
        html.A(
            [
                button('Uitleg upload',
                       'uitleg_upload_gcloud_button', site_colors['cyan']),
                dbc.Collapse(
                    dbc.Card(
                        dbc.CardBody([dbc.Row(html.Div(
                            """
                            Op deze pagina kunnen bronbestanden voor ChangePoint worden geüpload.
                            Deze bestanden worden opgenomen in het dashboard.
                            De data wordt verwerkt door de Operational Datahub en daardoor \
                                zal de dataverwerking een paar minuutjes duren.
                            Na een bevestiging dat de upload is geslaagd kun je deze pagina verlaten.

                            Bij het uploaden worden een aantal kolommen verwacht.
                            Als deze kolommen niet voorkomen in de upload, wordt het bestand niet geaccepteerd.
                            """
                            )),
                            dbc.Row([
                                dbc.Col(html.Div(uitleg_cp)),
                            ])]
                        ),
                        style={'background-color': site_colors['grey20']},
                    ),
                    id='uitleg_upload_gcloud',
                )
            ]
        ),
    )

    gcloud_upload = dcc.Upload(
        id='gcloud_upload',
        multiple=True,
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

    upload = html.Div(
        children=[
            html.Div(id='upload_alert_gloud'),
            html.Div(
                children=[
                    html.Br(),
                    gcloud_upload,
                    html.Br(),
                    uitleg_upload_gcloud,
                ],
                style=styles['box'],
            ),
        ],
    )

    return upload


def blob_post_stream_to_file_gcloud(stream, filename, source):
    try:
        client = storage.Client()
        bucket = client.get_bucket(source['bucket_name'])
        blob = storage.Blob(filename, bucket)
        blob.upload_from_string(
            stream.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

        print('File {} uploaded to {}.'.format(
            filename,
            source['bucket_name']))
        return 'File accepted, placed in bucket'
    except Exception as e:
        print(e)
        return 'File has not been placed, please try again.'


def http_post_stream_to_file_gcloud(stream, filename, source):
    # Source:
    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/endpoints/getting-started/clients/service_to_service_non_default/main.py
    try:
        response = make_iap_request(
            source['url'], source['client_id'], method='POST', files={
                'file': (filename, stream.getvalue())})
        if response == 'OK':
            return 'File accepted, post accepted'
        else:
            return 'File has not been accepted, please try again. Response: {}'.format(response)

    except Exception as e:
        print('Response: {}'.format(str(e)))
        traceback.print_exc()
        if str(e).startswith('Bad response from application: '):
            error = str(e).split('/')[-1]
            return 'File has not been accepted, please try again. Response: {}'.format(error)
        else:
            return 'File cannot be posted to server.'


def fix_columns_connect(df, filename):
    return df.drop_duplicates(subset='Uniek ID', keep='first')


def checkSource(filename, sources):
    for source, data in sources.items():
        for name in data['filename_id']:
            if name.lower() in filename.lower():
                return sources[source]
    return None


def parse_upload(contents, filename):
    sources = config.sources
    # Add fix_columns
    # Connect cannot be uploaded, thus commented out
    # sources['Connect']['fix_columns'] = fix_columns_connect

    # Find sourceTag based on filename:
    source = None
    for fname in filename:
        if source is None:
            source = checkSource(fname, sources)
        else:
            if source['sourceTag'] != checkSource(fname, sources)['sourceTag']:
                return "danger|Files don't appear to belong to the same source", None, None, None

    if source is None:
        return "danger|Source could not be determined", None, None, None

    if source['no_of_files'] != len(filename):
        return "danger|For source {}: expected {} {}, received {}".format(
            source['sourceTag'],
            source['no_of_files'],
            'file' if source['no_of_files'] == 1 else 'files',
            len(filename),
        ), None, None, None

    # Create one dataframe from multiple sourceTags
    df = pd.DataFrame([])
    for i in range(len(filename)):
        content = contents[i]
        fname = filename[i]

        content_string = content.split(',')[1]
        decoded = base64.b64decode(content_string)
        df_temp = pd.read_excel(io.BytesIO(decoded), dtype=str)

        # Add extra columns based on source
        add_columns = source.get('add_columns', None)
        if add_columns is not None:
            df_temp = add_columns(df_temp, fname)

        fix_columns = source.get('fix_columns', None)
        if fix_columns is not None:
            df_temp = fix_columns(df_temp, fname)

        # Append
        df = df.append(df_temp)
    # Drop duplicates
    df = df.drop_duplicates()

    filename = '_'.join([
        str(int(time.time())),
        source['sourceTag'],
        '__'.join([i.split('.')[0] for i in filename]),
    ]) + '.xlsx'

    # Create stream to return:
    stream = io.BytesIO()
    excel_writer = pd.ExcelWriter(stream, engine="xlsxwriter")
    df.to_excel(excel_writer, sheet_name="data", index=False)
    excel_writer.save()

    return None, stream, filename, source


@app.callback(
    [
        Output('upload_alert_gloud', 'children'),
    ],
    [
        Input('gcloud_upload', 'contents'),
    ],
    [
        State('gcloud_upload', 'filename'),
    ],
)
def upload_gcloud(contents, filenames):
    # First process upload
    message, color = 'Unknown problem occured, please contact the administator', 'danger'
    try:
        message, stream, filename, source = parse_upload(contents, filenames)

        if message is None:
            post_message = blob_post_stream_to_file_gcloud(
                stream, filename, source)

            if post_message.startswith('File accepted'):
                message = 'Upload of "{}" succesfull: {}.'.format(
                    '" ,"'.join(filenames), post_message)
                color = 'success'
            else:
                message = 'Upload of "{}" unsuccesfull: {}'.format(
                    '", "'.join(filenames), post_message)
                color = 'danger'
        else:
            color, message = message.split('|')

        with Connection('w', 'upload_sourcefile') as session:
            czLog.insert(
                {
                    'action': 'upload_sourcefile_' + color,
                    'created': dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'description': message,
                },
                session)
    except Exception:
        traceback.print_exc()

    # Return upload
    return [alert(message, color)]


@app.callback(  # Toggle explaination upload
    Output('uitleg_upload_gcloud', 'is_open'),
    [Input('uitleg_upload_gcloud_button', 'n_clicks')],
    [State('uitleg_upload_gcloud', 'is_open')],
)
def toggle_collapse_gloud(n, is_open):
    return toggle(n, is_open)


@app.callback(  # Toggle explaination upload InforLN
    Output('uitleg_upload_st1', 'is_open'),
    [Input('uitleg_upload_st1_button', 'n_clicks')],
    [State('uitleg_upload_st1', 'is_open')],
)
def toggle_collapse_cp(n, is_open):
    return toggle(n, is_open)
