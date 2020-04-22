from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from connection import Connection
from db.models import czSubscriptions, czImportKeys
from db.queries import read
from app import app
from apps.elements import button, site_colors, styles, colors_graph
import pandas as pd
import numpy as np
from analysis.connectz import expand_column
from datetime import datetime as dt
import textwrap
import plotly.graph_objs as go
from plotly.subplots import make_subplots


text1 = 'Projectstructuur constateringen'
text2 = 'Totale aantal foutmeldingen per constatering over tijd'
text3 = 'Toename en afname aantal constateringen over de gehele periode'
text4 = 'Projectstructuur constateringen_voorheen'

merge_on = ['ln_id', 'bpnr', 'con_opdrachtid']
sort_by = ['ln_id', 'bpnr', 'con_opdrachtid']

id = 'ln_id'
nr = 'bpnr'
opdracht_id = 'con_opdrachtid'

write_data_sheet_cols = [
    'ln_id',
    'bpnr',
    'con_opdrachtid',
    'categorie',
    'Projectstructuur constateringen',
    'koppeling',
]

write_data_sheet_cols2 = [
    'ln_id',
    'bpnr',
    'con_opdrachtid',
    'categorie',
    'Projectstructuur constateringen',
    'koppeling',
    'categorie_voorheen',
    'Projectstructuur constateringen_voorheen',
    'koppeling_voorheen',
    'komt voor in',
    'bewerkingsstatus',
]


def get_body():
    with Connection('r') as session:
        q = session.query(czImportKeys.version).\
            join(czSubscriptions, czImportKeys.sourceTag == czSubscriptions.stagingSourceTag).\
            filter(czSubscriptions.sourceTag == 'projectstructure').\
            order_by(czImportKeys.version).distinct()
        versions = session.execute(q)
    versions = [str(r) for r, in versions]

    slider = dcc.RangeSlider(
        id='diff_slider',
        min=unix_time_sec(versions[0]),
        max=unix_time_sec(versions[-1]),
        marks={unix_time_sec(i): i.split(' ')[0] for i in versions},
        value=[unix_time_sec(i) for i in versions[-2:]],
        step=None
    )

    # Returned tab_content
    diff_page = html.Div([
        html.Div([
            html.P(''),
            html.H4('Selecteer een range om te vergelijken'),
            html.Div(
                slider,
                style={
                    'margin-left': '3%',
                    'margin-right': '3%',
                    'margin-top': '3%',
                    'margin-bottom': '5%',
                    "text-align": 'center',
                }),
            ],
            style=styles['box']
        ),
        html.Div(
                id='diff_graph_total',
                style=styles['graph_page'],
        ),
        html.Div([
            'Enkele klik op een melding in de legenda om hem te selecteren of te deselecteren, \
                dubbelklik op een melding in de legenda om deze als enige weer te geven.',
            html.Div(
                id='diff_download_button',
            )],
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
        ),
    ])

    return diff_page


# update the graph with two or more data points to compare (defined by the slider)
def diff_update_graph(dates):
    # read data from database the overview and the versions
    with Connection('r', 'read overview: update_problem_table') as session:
        overview = read(
            session, 'projectstructure', ts='all')

    with Connection('r') as session:
        q = session.query(czImportKeys.version).\
            join(czSubscriptions, czImportKeys.sourceTag == czSubscriptions.stagingSourceTag).\
            filter(czSubscriptions.sourceTag == 'projectstructure').\
            order_by(czImportKeys.version).distinct()
        versions = session.execute(q)
    versions = [str(r) for r, in versions]

    # number of updates inbetween the two dates
    start_idx = versions.index(dates[0])
    end_idx = versions.index(dates[1])
    total_dates = versions[start_idx:end_idx + 1]

    # find the number of error messages per moment in time
    # create empyt data frame with a column
    total_df = pd.DataFrame(columns=[text1])
    for date_itt in total_dates:
        # translate string to datetime
        date = pd.to_datetime(date_itt)
        # filter the number errors until a certain date
        df_most_recent = overview[
            (overview['version'].apply(pd.to_datetime) <= date) &
            (overview['versionEnd'].isna() | (overview['versionEnd'].apply(pd.to_datetime) > date)) &
            (overview[text1].notna()) &
            (overview[text1] != ' ') &
            (overview[text1] != '')
        ]
        # split the multiple errors into single lines
        df_most_recent = expand_column(
            df_most_recent, text1, splitter='; ')

        # remove the empty lines because some gave '' or ' '
        df_most_recent = df_most_recent[~df_most_recent[text1].isin([' ', ''])]
        # groupby 'constateringen'
        df_most_recent = df_most_recent.groupby([text1]).\
            size().reset_index().rename(columns={0: pd.to_datetime(date_itt)})
        # merge with existing table
        total_df = total_df.merge(df_most_recent, on=text1, how='outer').fillna(0)

    # total errors per update
    overall_errors = total_df.drop(text1, axis=1).sum(axis=0)
    overall_y = overall_errors.tolist()
    overall_x = overall_errors.index

    total_df = total_df.set_index(text1).to_dict(orient='split')

    # prepare data
    data = []
    count = 0
    for y in total_df['data']:
        data.append({
            'x': total_df['columns'],
            'y': y,
            'name': '<br>'.join(textwrap.wrap(total_df['index'][count], width=40)),
            'type': 'scatter'
        })
        count += 1

    # sorting the data before entering in the plot gives
    sort_temp = []
    for y in data:
        sort_temp.append(y.get('y')[-1] - y.get('y')[0])
    sort_temp = np.argsort(sort_temp)

    data_temp = []
    for i in range(len(sort_temp)):
        data_temp.append(data[sort_temp[i]])
    data = data_temp

    # Make the figure
    fig_total = make_subplots(
        rows=2, cols=1,
        subplot_titles=(text2, text3),
        )

    # First entry: sum of 'constateringen'
    count = 0
    fig_total.add_trace(
        go.Scatter(
            x=overall_x, y=overall_y,
            name='Totaal aantal constateringen',
            legendgroup='Totaal aantal constateringen',
            marker={'color': colors_graph[count]},
            hoverinfo=['name+y'],
        ),
        row=1, col=1
    )

    for el in data:
        count += 1
        # assign color to the specific error (max 20 colors, else start again with the same colors)
        if count < len(colors_graph):
            color_line = colors_graph[count]
        else:
            count = 0
            color_line = colors_graph[count]

        fig_total.add_trace(
            go.Scatter(
                el,
                legendgroup=el['name'],
                marker={'color': color_line},
                hoverinfo=['name+y']
            ),
            row=1, col=1
        )

        if (el.get('y')[-1] - el.get('y')[0]) != 0:
            fig_total.add_trace(
                go.Bar(
                    y=[(el.get('y')[-1] - el.get('y')[0])],
                    legendgroup=el['name'],
                    name=el['name'],
                    showlegend=False,
                    marker={'color': color_line},
                    hoverinfo=['name+y'],
                ),
                row=2, col=1,
            )

    fig_total.layout.update(
        height=800,
        title='Overzicht constateringen tussen {} en {}'.format(versions[0], versions[-1]))
    graph = dcc.Graph(
        figure=fig_total
    )
    return graph


def unix_time_sec(ts):
    epoch = dt.utcfromtimestamp(0)
    return int((dt.strptime(ts, '%Y-%m-%d %H:%M:%S') - epoch).total_seconds())


# because the slider gives value 'None' when not exactly on a mark,
# define the output when inbetween two marks
def find_group_key(marks, date):
    keys = list(marks.keys())
    nr_keys = len(keys)
    for i in range(nr_keys-1):
        if (int(date) >= int(keys[i])) & (int(date) < int(keys[i+1])):
            return marks.get(keys[i])
    return marks.get(keys[-1])


def get_marks_diff_dateselection():
    with Connection('r') as session:
        q = session.query(czImportKeys.version).\
            join(czSubscriptions, czImportKeys.sourceTag == czSubscriptions.stagingSourceTag).\
            filter(czSubscriptions.sourceTag == 'projectstructure').\
            order_by(czImportKeys.version).distinct()
        versions = session.execute(q)

    versions = [str(r) for r, in versions]
    marks = {unix_time_sec(i): i for i in versions}
    return marks


@app.callback(
    [
        Output('diff_graph_total', 'children'),
        Output('diff_download_button', 'children'),
    ],
    [
        Input('diff_slider', 'value'),
    ],
)
def diff_update_graph_dropdown(dates):
    marks = get_marks_diff_dateselection()

    dates = [find_group_key(marks, dates[0]), find_group_key(marks, dates[1])]
    fig_total = diff_update_graph(dates)

    button_download = html.A(
        button("Download de verschillen in Excel", backgroundcolor=site_colors['indigo']),
        href='/download?type=diff&old={}&new={}&'.format(
            dates[0],
            dates[1],
        ))

    return [fig_total, button_download]


def compare_download(now, hist, excel_writer, now_date, hist_date):
    diff = now.fillna('').merge(hist.fillna(''), on=merge_on, how='outer', indicator=True, suffixes=['', '_voorheen'])
    diff['_merge'] = diff['_merge'].str.replace('left_only', 'overzicht {}'.format(now_date))
    diff['_merge'] = diff['_merge'].str.replace('right_only', 'overzicht {}'.format(hist_date))
    diff.at[((diff['_merge'] == 'both') &
            ~((diff[text1] == diff[text4]) &
            (diff['categorie'] == diff['categorie_voorheen']) &
            (diff['koppeling'] == diff['koppeling_voorheen']))
            ), '_merge'] = 'in beide: verandering'
    diff['_merge'] = diff['_merge'].str.replace('both', 'in beide: geen verandering')

    with Connection('r', 'beetgehad') as session:
        q = 'SELECT `key`, `status` FROM czCleaning where `status` IS NOT NULL and `versionEnd` IS NOT NULL'
        beet = pd.read_sql(q, session.bind)

    diff['key'] = diff[id] + '|' + diff[nr] + '|' + diff[opdracht_id]
    diff = diff.merge(beet, on='key', how='left')
    diff = diff.sort_values(by=sort_by)
    diff = diff.rename(columns={'_merge': 'komt voor in', 'status': 'bewerkingsstatus'})
    diff.drop(['key'], axis=1, inplace=True)

    now_date = now_date.replace(' ', '').replace('-', '').replace(':', '')
    hist_date = hist_date.replace(' ', '').replace('-', '').replace(':', '')

    # Write data to sheets
    cols = write_data_sheet_cols
    now[cols].to_excel(excel_writer, sheet_name="overzicht {}".format(now_date), index=False)
    hist[cols].to_excel(excel_writer, sheet_name="overzicht {}".format(hist_date), index=False)

    cols = write_data_sheet_cols2

    diff[cols].to_excel(excel_writer, sheet_name='overzicht verschillen', index=False)

    ws = excel_writer.sheets["overzicht {}".format(now_date)]
    ws.freeze_panes(1, 0)
    ws.set_column('A:F', 22)
    ws.autofilter(0, 0, now.shape[0], now.shape[1]-1)

    ws = excel_writer.sheets["overzicht {}".format(hist_date)]
    ws.freeze_panes(1, 0)
    ws.set_column('A:F', 22)
    ws.autofilter(0, 0, hist.shape[0], hist.shape[1]-1)

    ws = excel_writer.sheets["overzicht verschillen"]
    ws.freeze_panes(1, 0)
    ws.set_column('A:K', 22)
    ws.autofilter(0, 0, diff.shape[0], diff.shape[1]-1)

    return excel_writer
