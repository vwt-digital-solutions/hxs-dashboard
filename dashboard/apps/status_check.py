from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
from apps.elements import button, site_colors, styles, table_styles
from app import app
from connection import Connection
from db.models import czHierarchy
from db.queries import read
import sqlalchemy as sa
from analysis.fase_check import fase_check
import plotly.graph_objs as go
from ast import literal_eval
from analysis.projectstructuur import xaris

cardbody1 = '''
    In de tabbladen die vallen onder de pagina ‘statussen check’ wordt er een vergelijking
    gemaakt tussen de statussen van een project in de verschillende deelsystemen. Er komen
    verschillende combinaties van statussen voor en de projecten binnen een bepaalde combinatie
    kunnen via deze pagina worden opgevraagd. Er zijn 4 verschillende vergelijkingen te maken:
'''
cardbody2 = "1.	InforLN versus Changepoint Nieuwbouw: de status van alle nieuwbouwprojecten in LN worden vergeleken met de status van de gekoppelde Changepoint nummers"
cardbody3 = "2.	InforLN versus Changepoint Vooraanleg: de status van alle vooraanleg projecten in LN worden vergeleken met de status van de gekoppelde Changepoint nummers."
cardbody4 = "3.	InforLN versus Connect: de statussen van alle in LN worden vergeleken met de status van de gekoppelde Connect objecten."
cardbody5 = "4.	Xaris versus Connect: de statussen van alle projecten die voorkomen in Xaris worden vergeleken met de statussen van alle gekoppelde Connect opdrachten."
cardbody6 = '''
    Het aantal verschijningen van een bepaalde combinatie wordt weergegeven in de tabellen op de pagina.
    Door op een bepaalde combinatie te klikken, volgt onderaan een uitgebreide tabel met de desbetreffende
    opdrachten. Met kleuren codes is aangegeven of deze combinatie van statussen in de deelsystemen voor
    mag komen of niet. Hierbij is de volgende mapping toegepast:
'''
cardbody7 = 'Groen --> Deze combinatie is correct'
cardbody8 = 'Geel --> Deze combinatie van statussen is twijfelachtig. Per case kan het verschillen of dit correct of incorrect is'
cardbody9 = 'Rood --> Deze combinatie van statussen is incorrect'
cardbody10 = 'Blauw --> Deze combinatie kan alleen voorkomen als er een fout is gemaakt bij Vodafone Ziggo'
cardbodyheader = 'Bepalen van de statussen:'
cardbody11 = '''
    De status van een project die voorkomt in de tabel op deze tabbladen komt niet
    één op één overeen met de status van de Connect, LN, Changepoint en Xaris opdracht zoals
    deze in de systemen zelf voorkomen. Een LN-opdracht doorloopt in InforLN bijvoorbeeld 27
    fases (van 100-algemeen opbrengsten tot 740-gereed) die in de weergegeven tabellen
    verdeeld zijn over 7 statussen. Ook de fases in deelsystemen Connect, Changepoint en
    Xaris zijn gemapt naar een kleinere set van statussen. De Connect status wordt bepaald
    aan de hand van een combinatie tussen ‘Connect aanvraagstatus’, ‘Connect object status’ en de ‘Connect
    afrekenstatus’. Bij de vergelijking van Xaris en Connect wordt de status in het dashboard
    bepaald aan de hand van een aantal statements met betrekking tot de Xaris werkstromen en
    de Connect objecten. De uitgebreide mapping is weergegevenin de download.
'''

radio_item_options = [
    {'label': 'Xaris zonder connect: vanaf 01-01-2018', 'value': True},
    {'label': 'Xaris zonder connect: gehele historie', 'value': False},
]

intake_options = ['InforLN_vs_ChangePoint_Nieuwbouw', 'InforLNn_vs_ChangePoint_Vooraanleg', 'InforLN_vs_Connect', 'Xaris_vs_Connect']
option1 = 'InforLN_vs_ChangePoint_Nieuwbouw'
option2 = 'InforLNn_vs_ChangePoint_Vooraanleg'
option3 = 'InforLN_vs_Connect'
option4 = 'Xaris_vs_Connect'

df_out1 = ['ln_id', 'bpnr', 'categorie']
df_out2 = ['ln_id', 'con_opdrachtid', 'aantal_conobj_status', 'verhouding', 'aantal_conobj_totaal']
df_out3 = [
    'Aanvraagdatum', 'Aanvraagnummer', 'Aanvraagstatus', 'Adres', 'Datum gereed',
    'Hoofdleidingenprojectnummer', 'Kabel', 'Objecten', 'Plaats', 'Postcode',
    'Projectnummer', 'Startdatum', 'nr_werkstromen', 'Status Xaris'
]

sourcetag1 = 'InforLN'
id = 'ln_id'
measures1 = ['ln_fase', 'ln_fase_description', 'project_description']


sourcetag2 = 'ChangePoint'
measures2 = ['project_id', 'cp_fase', 'cp_status', 'project_name']

df_out4 = ['ln_id', 'bpnr', 'categorie', 'ln_fase', 'ln_fase_description', 'cp_fase', 'cp_status', 'project_name']
opdracht_id = 'con_opdrachtid'

measures3 = ['con_opdrachtid', 'status_request', 'status_object', 'status_payment', 'status_order', 'order_type']
sourcetag3 = 'Connect'
df_out5 = ['ln_id', 'con_opdrachtid', 'con_objectid', 'ln_fase', 'ln_fase_description',
           'status_request', 'status_object', 'status_payment', 'status_order',
           'aantal_conobj_status', 'verhouding', 'aantal_conobj_totaal', 'order_type']

df_out6 = {
    'Con_status': 'Con. Opdr. status',
    'Con_uitvoering': 'Con. uitvoertype',
    'juist_nummer': 'Connect OpdrachtID',
    'Status Xaris': 'Xaris status',
}

df_out7 = [
    'Connect OpdrachtID',
    'Con. uitvoertype',
    'Con. Opdr. status',
    'Xaris status'
]


def get_body():
    uitleg_upload = html.Div(
        html.A(
            [
                button('Uitleg', 'uitleg_upload_button', site_colors['cyan']),
                dbc.Collapse(
                    dbc.Card(
                        dbc.CardBody([
                            html.P(
                                cardbody1
                            ),
                            html.P(
                                cardbody2
                            ),
                            html.P(
                                cardbody3
                            ),
                            html.P(
                                cardbody4
                            ),
                            html.P(
                                cardbody5
                            ),
                            html.P(''),
                            html.P(
                                cardbody6
                            ),
                            html.P(cardbody7),
                            html.P(cardbody8),
                            html.P(cardbody9),
                            html.P(cardbody10),

                            html.Header(
                                cardbodyheader
                            ),
                            html.P(
                                cardbody11
                            ),
                        ]),
                        style={'background-color': site_colors['grey20']},
                        ),
                    id="uitleg_upload",
                )
            ]
        )
    )

    download_uitleg = html.A(
        button(
            "Download uitgebreide uitleg",
            backgroundcolor=site_colors['indigo'],
        ),
        href='/download?type=pdf_status&value=explain',
    )

    choose_cpnr_radio = dbc.FormGroup(
        [
            dbc.RadioItems(
                options=radio_item_options,
                value=True,
                id='historie',
                style={'display': 'none'},
            ),
        ],
    )

    dropdown_status_vs = dcc.Dropdown(
        id='dropdown_status_vs',
        options=[
            {'label': i.replace('_', ' '), 'value': i} for i in intake_options],
        multi=False,
        value=intake_options[0],
        style={'width': '90%', 'margin': '8px'},
    )

    # Returned tab_connect
    intake_page = html.Div([

        html.Div(
            uitleg_upload,
        ),

        html.Div(
            download_uitleg,
        ),

        html.Div(
            choose_cpnr_radio,
        ),

        html.Div(
            [
                html.P(""),
                html.H4(
                    'Selecteer een status check optie:',
                    className='lead'),
                dropdown_status_vs,
            ],
            style=styles['box'],
        ),

        html.Div(dbc.Row([
            dbc.Col(
                html.Div(
                    dash_table.DataTable(
                        id='dash_datatable',
                        style_table={'overflowX': 'auto'},
                        style_header=table_styles['header'],
                        style_cell=table_styles['cell']['action'],
                        selected_cells=[{'row': 1000, 'column': 1000}],
                        active_cell=None,
                    ),
                    # style=styles['table_page'],
                    className='six columns',
                ),
            ),
            dbc.Col(
                html.Div(
                    dcc.Graph(
                        id='status_graph',
                        style=styles['graph_page']
                    ),
                    className='six columns',
                ),
            ),
            ]),
            style=styles['page']
        ),


        html.Div(
            id='status_table_ext',
            style=styles['table_page'],
        ),

        html.Div(
            id='status_download_button',
        ),

        dcc.Store(id='memory-output1'),

        dcc.Store(id='memory-output2'),
    ])

    return intake_page


# function to update filtered data from cell
def get_status_df(intake_value, selected_cells, fm, xaris_df=None):
    df_out = pd.DataFrame([])
    if intake_value == option1:
        df_ = pd.DataFrame(fm['types_1_unique'])
        df2 = pd.DataFrame(fm['status_1'])
        df_out = df2[(df2['cpfase'] == df_['cpfase'][selected_cells[0]['row']]) &
                     (df2['categorie'] == '34_nieuwbouw') & (df2['lnfase'] == selected_cells[0]['column_id'])]
        df_out = df_out[df_out1]
        type_fout = '(lnfase = ' + selected_cells[0]['column_id'] + ', cpfase = ' + df_['cpfase'][selected_cells[0]['row']] + ')'
    elif intake_value == option2:
        df_ = pd.DataFrame(fm['types_2_unique'])
        df2 = pd.DataFrame(fm['status_1'])
        df_out = df2[(df2['cpfase'] == df_['cpfase'][selected_cells[0]['row']]) &
                     (df2['categorie'] == '34_vooraanleg') & (df2['lnfase'] == selected_cells[0]['column_id'])]
        df_out = df_out[df_out1]
        type_fout = '(lnfase = ' + selected_cells[0]['column_id'] + ', cpfase = ' + df_['cpfase'][selected_cells[0]['row']] + ')'
    elif intake_value == option3:
        df_ = pd.DataFrame(fm['types_3_unique'])
        df2 = pd.DataFrame(fm['status_2'])
        idx = selected_cells[0]['row']
        df_out = df2[(df2['con_request'] == df_['con_request'].iloc[idx]) & (df2['con_object'] == df_['con_object'].iloc[idx]) &
                     (df2['con_payment'] == df_['con_payment'].iloc[idx]) & (df2['lnfase'] == selected_cells[0]['column_id'])]
        type_fout = '(confase: ' + df_['con_request'].iloc[idx] + \
                    ' + ' + df_['con_object'].iloc[idx] + ' + ' + df_['con_payment'].iloc[idx] + \
                    ', lnfase: ' + selected_cells[0]['column_id'] + ')'
        df_out['aantal_conobj_totaal'] = df_out['aantal_conobj'].astype('float')/df_out['verhouding'].astype('float')
        df_out['aantal_conobj_totaal'] = df_out['aantal_conobj_totaal'].round(0)
        df_out = df_out.rename(columns={'aantal_conobj': 'aantal_conobj_status'})
        df_out = df_out[df_out2]
        type_fout = '(lnfase = ' + selected_cells[0]['column_id'] + ', con_request = ' + df_['con_request'].iloc[idx] + \
                    ', con_object = ' + df_['con_object'].iloc[idx] + ', con_payment = ' + df_['con_payment'].iloc[idx] + ')'
    elif intake_value == option4:
        df_ = pd.DataFrame(fm['types_4_unique'])
        df2 = pd.DataFrame(fm['status_4'])
        row_idx = selected_cells[0]['row']
        if isinstance(df_.index[0], str):
            row_idx = str(row_idx)
        type_ = df_.at[row_idx, 'Con_uitvoering']
        con = df_.at[row_idx, 'Con_status']
        xar = selected_cells[0]['column_id']
        df_out = df2[((df2['Status Xaris'] == xar) & (df2['Con_status'] == con) & (df2['Con_uitvoering'] == type_))]
        if xaris_df is not None:
            temp_list = list(df_out['juist_nummer'])
            xaris_df = pd.DataFrame(xaris_df)
            df_out = xaris_df[xaris_df['juist_nummer'].isin(temp_list)]

            df_out = df_out[df_out3]
        else:
            df_out = df_out.rename(columns=df_out6)[df_out7]

        type_fout = '(Xaris: ' + xar + ', Connect: ' + con + ', type uitvoering: ' + type_ + ')'

    else:
        raise ValueError("Not correct status table")

    return df_out, type_fout, df_


# only show 'radio item' when xaris tab is shown
@app.callback(
    Output('historie', 'style'),
    [
        Input('dropdown_status_vs', 'value'),
    ],
)
def show(value):
    if value == option4:
        return {'display': 'block'}
    else:
        return {'display': 'none'}


# Create datatable: when dropdown is refreshed or when a Connect order is uploaded
@app.callback(
        [
            Output('dash_datatable', 'columns'),
            Output('dash_datatable', 'data'),
            Output('dash_datatable', 'style_data_conditional'),
            Output('dash_datatable', 'selected_cells'),
            Output('dash_datatable', 'active_cell'),
            Output('memory-output1', 'data'),
            Output('memory-output2', 'data'),

        ],
        [
            Input('dropdown_status_vs', 'value'),
            Input('historie', 'value')
        ],
        [
            State('memory-output1', 'data'),
            State('memory-output2', 'data'),
        ]

)
def generate_status_table(value, hist, fm_all, fm_filter):

    if fm_all is None:
        fm_all, fm_filter = fase_check()

    if hist:
        fm = fm_filter
    else:
        fm = fm_all

    if value == option3:
        df_ = pd.DataFrame(fm['types_3_unique'])
        df_status = pd.DataFrame(fm['types_3_unique_status'])
    elif value == option2:
        df_ = pd.DataFrame(fm['types_2_unique'])
        df_status = pd.DataFrame(fm['types_2_unique_status'])
    elif value == option1:
        df_ = pd.DataFrame(fm['types_1_unique'])
        df_status = pd.DataFrame(fm['types_1_unique_status'])
    elif value == option4:
        df_ = pd.DataFrame(fm['types_4_unique'])
        df_status = pd.DataFrame(fm['types_4_unique_status'])
    else:
        raise ValueError("Not correct status table")

    ifstatement = []
    for i, row in df_status.iterrows():
        for j in row.index:
            color = None
            if df_[j][i] != 0:
                if row[j] == 0:
                    color = 'rgb(205,51,51)'
                elif row[j] == 1:
                    color = 'rgb(154,205,50)'
                elif row[j] == 0.5:
                    color = 'rgb(238,238,0)'
                elif row[j] == -1:
                    color = 'rgb(0,0,255)'
                if color is not None:
                    ifstatement.append(
                        {
                            'if': {'row_index': int(i), 'column_id': j},
                            'backgroundColor': color,
                        }
                    )

    return [
        # columns=
        [{"name": i, "id": i} for i in df_.columns],
        # data=
        df_.to_dict("rows"),
        # style_data_conditional =
        ifstatement,
        [{'row': 1000, 'column': 1000}],
        None,
        fm_all,
        fm_filter,
    ]


@app.callback(
        Output('status_graph', 'figure'),
        [
            Input('dropdown_status_vs', 'value'),
            Input('historie', 'value'),
            Input('memory-output1', 'data'),
            Input('memory-output2', 'data'),
        ],

)
def generate_status_graph(value, hist, fm_all, fm_filter):
    if fm_all is None:
        raise PreventUpdate
    if fm_filter is None:
        raise PreventUpdate

    if hist:
        fm = fm_filter
    else:
        fm = fm_all

    if value == option1:
        df = pd.DataFrame(fm['status_1'])
        y_v = df[df['categorie'] == '34_nieuwbouw']['status'].value_counts().values
        y_v = np.array([y_v[0], y_v[1]])
        x_v = df[df['categorie'] == '34_nieuwbouw']['status'].value_counts().index
        x_v = ['Status Goed', 'Status Fout']
        tot = len(df[df['categorie'] == '34_nieuwbouw'])
        colors = ['rgb(154,205,50)', 'rgb(205,51,51)']
    elif value == option2:
        df = pd.DataFrame(fm['status_1'])
        y_v = df[df['categorie'] == '34_vooraanleg']['status'].value_counts().values
        y_v = np.array([y_v[0], y_v[1]])
        x_v = df[df['categorie'] == '34_vooraanleg']['status'].value_counts().index
        x_v = ['Status Goed', 'Status Fout']
        tot = len(df[df['categorie'] == '34_vooraanleg'])
        colors = ['rgb(154,205,50)', 'rgb(205,51,51)']
    elif value == option3:
        df = pd.DataFrame(fm['status_2'])
        y_v = df['status'].value_counts().values
        y_v = np.array([y_v[1], y_v[0], y_v[2], y_v[3]])
        x_v = df['status'].value_counts().index
        x_v = ['Status Goed', 'Status Fout', 'Status Twijfel', 'Status Fout VodafoneZiggo']
        tot = len(df)
        colors = ['rgb(154,205,50)', 'rgb(205,51,51)', 'rgb(238,238,0)', 'rgb(0,0,255)']
    elif value == option4:
        df = pd.DataFrame(fm['types_4'])
        y_v = df.groupby('Kleuren').agg({'Totaal': 'sum'})
        y_v = pd.DataFrame(y_v)
        y_v = np.array([y_v.loc[0]['Totaal'], y_v.loc[0.5]['Totaal'], y_v.loc[1]['Totaal']])
        x_v = ['type ' + 'Status Fout', 'type ' + 'Status Twijfel', 'type ' + 'Status Goed']
        tot = sum(y_v)
        colors = ['rgb(205,51,51)', 'rgb(238,238,0)', 'rgb(154,205,50)']
    else:
        raise ValueError("Not correct status table")

    return {
        'data': [
            go.Pie(values=y_v/tot,
                   labels=x_v,
                   marker=dict(colors=colors))
        ],
        'layout': go.Layout(title='% type melding op totaal',
                            yaxis={'range': [0, 1]})
    }


@app.callback(
        Output('status_table_ext', 'children'),
        [
            Input('dash_datatable', 'selected_cells'),
            Input('historie', 'value'),
            Input('memory-output1', 'data'),
            Input('memory-output2', 'data'),
        ],
        [
            State('dropdown_status_vs', 'value')
        ]
)
def generate_status_table_ext(selected_cells, hist, fm_all, fm_filter, intake_value):
    if fm_all is None:
        raise PreventUpdate
    if fm_filter is None:
        raise PreventUpdate

    if hist:
        fm = fm_filter
    else:
        fm = fm_all

    if selected_cells[0]['row'] == 1000:
        return [html.P()]

    df_out, type_fout, df_ = get_status_df(intake_value, selected_cells, fm)

    if (df_.iloc[selected_cells[0]['row'], selected_cells[0]['column']]) == 0:
        return [html.P()]

    return [
            html.Div(
                [
                    html.P(""),
                    html.H4(
                        'Hieronder een gedetaileerd overzicht voor: ' + type_fout,
                        className='lead',
                    ),
                ],
                style=styles['box'],
            ),

            dash_table.DataTable(
                columns=[{"name": i, "id": i} for i in df_out.columns],
                data=df_out.to_dict("rows"),
                style_table={'overflowX': 'auto'},
                style_header=table_styles['header'],
                style_cell=table_styles['cell']['action'],
                style_filter=table_styles['filter'],
                )
    ]


@app.callback(
        Output('status_download_button', 'children'),
        [
            Input('dash_datatable', 'selected_cells'),
            Input('memory-output1', 'data'),
            Input('memory-output2', 'data')
        ],
        [
            State('dropdown_status_vs', 'value'),
            State('historie', 'value')
        ],
)
def status_download_button(selected_cells, fm_all, fm_filter, intake_value, hist):
    if fm_all is None:
        raise PreventUpdate
    if fm_filter is None:
        raise PreventUpdate

    if hist:
        fm = fm_filter
    else:
        fm = fm_all

    if selected_cells[0]['row'] == 1000:
        return html.P()

    _, _, df_ = get_status_df(intake_value, selected_cells, fm)

    if (df_.iloc[selected_cells[0]['row'], selected_cells[0]['column']]) == 0:
        return [html.P()]

    button_download = html.A(
        button("Download bovenstaande status check in Excel", backgroundcolor=site_colors['indigo']),
        href='/download?type=status&intake={}&cells={}&hist={}'.format(intake_value, selected_cells, hist)
    )

    return html.Div(
            [
                html.Div(
                    button_download
                ),
            ],
            style={
                'margin-left': '20%',
                'margin-right': '20%',
                'margin-top': '1%',
                'margin-bottom': '1%',
                'padding': '1%',
                'backgroundColor': site_colors['grey20'],
                'border-style': 'solid solid solid solid',
                'border-color': '#BEBEBE',
                'border-width': '1px',
                "text-align": "center",
            },
        )


def status_download_file(excel_writer, intake_value, selected_cells, hist):
    selected_cells = literal_eval(selected_cells)
    fm_all, fm_filter = fase_check()
    xaris_df = []
    if intake_value == 'Xaris_vs_Connect':
        xaris_df = xaris()
        xaris_df = xaris_df.df.to_dict()

    if hist:
        fm = fm_filter
    else:
        fm = fm_all

    df_out, _, _ = get_status_df(intake_value, selected_cells, fm, xaris_df)

    # ophalen extra data voor gedetaileerde tabel;
    with Connection('r', 'download_status_check') as session:
        if 'ln_id' in df_out.columns:
            ln = read(session,
                      sourceTag=sourcetag1,
                      key=df_out[id].drop_duplicates().tolist(),
                      measure=measures1).reset_index()
            df_out = df_out.merge(ln, left_on=id, right_on='sourceKey').drop('sourceKey', axis=1)

        if 'bpnr' in df_out.columns:
            cp_keys_q = sa.text('''
            SELECT kindKey, parentKindKey FROM czHierarchy
            WHERE parentKind = 'cpnr_extracted'
            AND kind = 'cp_id'
            AND versionEnd IS NULL
            AND parentKindKey IN :ids
            ''')
            cp_coupling = pd.read_sql(cp_keys_q, session.bind, params={'ids': df_out['bpnr'].fillna('').drop_duplicates().tolist()})
            cp = read(
                session,
                sourceTag=sourcetag2,
                key=cp_coupling['kindKey'].tolist(),
                measure=measures2
            )
            cp_coupling = cp_coupling.rename(
                columns={
                    'kindKey': 'sourceKey',
                    'parentKindKey': 'bpnr'})
            cp = cp.reset_index().merge(cp_coupling, on='sourceKey', how='left').drop('sourceKey', axis=1)
            df_out = df_out.merge(cp, on='bpnr')
            df_out = df_out[df_out4]

        if opdracht_id in df_out.columns:
            lijst = df_out[opdracht_id].fillna('').drop_duplicates().tolist()
            q = sa.select([czHierarchy.kindKey]).\
                where(czHierarchy.parentKindKey.in_(lijst)).\
                where(czHierarchy.parentKind == opdracht_id).\
                where(czHierarchy.kind == 'con_objectid').\
                where(czHierarchy.versionEnd.is_(None))
            keys = [r for r, in session.execute(q).fetchall()]
            connect = read(
                session,
                sourceTag=sourcetag3,
                key=keys,
                measure=measures3
            )
            connect = connect.reset_index().rename(columns={'sourceKey': 'con_objectid'})
            df_out = df_out.merge(connect, on=opdracht_id)
            df_out = df_out[df_out5]

    df_out.to_excel(excel_writer, sheet_name='status', index=False)
    ws = excel_writer.sheets['status']
    ws.freeze_panes(1, 0)
    ws.set_column('A:AH', 22)
    ws.autofilter(0, 0, df_out.shape[0], df_out.shape[1]-1)

    return excel_writer
