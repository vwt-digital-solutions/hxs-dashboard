from _plotly_future_ import v4_subplots

import os
import io
import flask
import utils
import pandas as pd
import sqlalchemy as sa
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from flask import redirect
from datetime import datetime as dt
from collections import OrderedDict

# Import app
from apps import error404
from dash.dependencies import Input, Output
from app import app, get_logout_url, get_user

# Import connection
from connection import Connection
from db.models import czLog, czCleaning
from db.queries import read

# Functions and pages from apps
from apps.datamanagement_problems import get_problem_table
from apps.elements import get_footer, site_colors
from apps.diffpage import compare_download
from apps.status_check import status_download_file
from apps.datamanagement_connect import get_con_df

from apps import startpagina
from apps import datamanagement_problems
from apps import datamanagement_lncpcon
from apps import datamanagement_user
from apps import datamanagement_connect
from apps import datamanagement_changepoint
from apps import upload
from apps import intake
from apps import status_check
from apps import diffpage

import config


download_config = {
    'hFwmotkaNo': {
        'filename': 'Connectopdracht',
        'sheetname': 'Connect',
    },
    'problem': {
        'col_order': ['ln_id', 'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen', 'koppeling'],
        'col_order2': [
            'ln_id', 'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen', 'koppeling', 'bewerkingsstatus'
        ],
    }
}

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


def get_navbar(navplace):
    navs = []
    for page in config_pages:
        if page in config.pages:
            navs = navs + [
                dbc.DropdownMenuItem(
                    config_pages[page]['name'],
                    href=config_pages[page]['link'][0]),
                dbc.DropdownMenuItem(divider=True),
            ]
    navs = navs[:-1]

    children = [
        dbc.NavItem(dbc.NavLink(navplace, href="#")),
        dbc.DropdownMenu(
            nav=True,
            in_navbar=True,
            label="Menu",
            children=navs,
        ),
    ]

    return dbc.NavbarSimple(
        children=children,
        brand=config.title,
        brand_href=config.website,
        sticky='top',
        color=site_colors['grey80'],
        dark=True
    )


def logout_route():
    logout_url = get_logout_url()
    return redirect(logout_url)


config_pages = OrderedDict([
    ("startpagina", {
        "name": 'Start',
        "link": ['/apps/start', '/apps/start/'],
        "body": startpagina
    }),
    ("datamanagement_problems", {
        "name": 'Datamanagement: overzicht',
        "link": ['/apps/datamanagement_problems', '/apps/datamanagement_problems/'],
        "body": datamanagement_problems
    }),
    ("datamanagement_lncpcon", {
        "name": 'Datamanagement: systemen',
        "link": ['/apps/datamanagement_lncpcon', '/apps/datamanagement_lncpcon/'],
        "body": datamanagement_lncpcon
    }),
    ("diffpage", {
        "name": 'Datamanagement: meldingen over tijd',
        "link": ['/apps/diffpage', '/apps/diffpage/'],
        "body": diffpage
     }),
    ("intake", {
        "name": 'Intake',
        "link": ['/apps/intake', '/apps/intake/'],
        "body": intake
    }),
    ("datamanagement_connect", {
        "name": 'Connect - bouwplan',
        "link": ['/apps/datamanagement_connect', '/apps/datamanagement_connect/'],
        "body": datamanagement_connect
     }),
    ("datamanagement_changepoint", {
        "name": 'ChangePoint contractor',
        "link": ['/apps/datamanagement_changepoint', '/apps/datamanagement_changepoint/'],
        "body": datamanagement_changepoint
    }),
    ("status_check", {
        "name": 'Statussen systemen',
        "link": ['/apps/status_check', '/apps/status_check/'],
        "body": status_check,
    }),
    ("datamanagement_user", {
        "name": 'Overzicht gebruiker',
        "link": ['/apps/datamanagement_user', '/apps/datamanagement_user/'],
        "body": datamanagement_user
    }),
    ("upload", {
        "name": 'Upload',
        "link": ['/apps/upload', '/apps/upload/'],
        "body": upload,
    }),
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    body = None
    navplace = ''

    footer = get_footer()

    if pathname == '/':
        pathname = "/apps/start"
    elif pathname == '/logout':
        logout_route()

    for page in config_pages:
        if page in config.pages and pathname in config_pages[page]['link']:
            body = config_pages[page]['body'].get_body()
            navplace = config_pages[page]['name']

    if body is None and pathname is not None:
        navplace = 'Pagina bestaat niet'
        body = error404.get_body(pathname)

    return html.Div([
        get_navbar(navplace),
        # Return the body in the overal style
        html.Div([body], style={
            'margin-left': '3%',
            'margin-right': '3%',
            'margin-top': '1%',
            'backgroundColor': '#FFFFFF',
            'border-color': '#BEBEBE',
            'border-width': '1px',
        }),
        html.Div(footer, style={
            'margin-left': '3%',
            'margin-right': '3%',
            'margin-top': '1%',
            'margin-bottom': '20px',
            'backgroundColor': '#FFFFFF',
            'border-style': 'hidden hidden hidden hidden',
            'border-color': '#BEBEBE',
            'border-width': '1px',
            'text-align': 'center',
        })
    ])


@app.server.route('/download')
def download():
    download_type = flask.request.args.get('type')

    strIO = io.BytesIO()

    if download_type == 'pdf':
        strIO = utils.download_as_buffer(
            config.tmp_bucket, config.files['explain_pdf'])
        send_info = dict(
            attachment_filename='Uitleg_{}.pdf'.format(
                str(dt.now())),
            mimetype='application/pdf',
        )

    elif download_type == 'pdf_status':
        strIO = utils.download_as_buffer(
            config.tmp_bucket, config.files['explain_status_check_pdf'])
        send_info = dict(
            attachment_filename='Uitleg_statuscheck_{}.pdf'.format(
                str(dt.now())),
            mimetype='application/pdf',
        )

    # Download connect orders
    elif download_type == 'hFwmotkaNo':
        value = flask.request.args.get('value')
        # create a dynamic csv or file here using `StringIO`
        excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
        yellow = excel_writer.book.add_format({'bg_color': '#F2DB09'})
        df = get_con_df(value, short=False)
        df.to_excel(excel_writer, sheet_name=download_config[download_type]['sheetname'], index=False)

        ws = excel_writer.sheets[download_config[download_type]['sheetname']]
        ws.freeze_panes(1, 0)
        ws.set_column('A:AH', 22)
        ws.set_column('D:D', 22, yellow)
        ws.autofilter(0, 0, df.shape[0], df.shape[1]-1)

        excel_writer.save()

        with Connection('w', 'log download') as session:
            czLog.insert([{
                'user': get_user(),
                'action': 'download',
                'description': 'conobjid-cpnr',
                'parameter': value,
            }], session)

        send_info = dict(
            attachment_filename='{}_{}_{}.xlsx'.format(
                download_config[download_type]['filename'],
                str(value),
                str(dt.now()),
            ),
        )

    # Download problem table
    elif download_type == 'problem':
        # Get values from url
        pr_dropdown_values = flask.request.args.get('problems').split('|')
        if pr_dropdown_values == ['']:
            pr_dropdown_values = []
        category_values = flask.request.args.get('category').split('|')
        if category_values == ['']:
            category_values = []

        with Connection('r', 'read overview: download_overview') as session:
            overview = read(session, 'projectstructure').fillna('')
        overview = overview[download_config[download_type]['col_order']]

        # Get status
        q_select = sa.select([czCleaning.key, czCleaning.status, czCleaning.updated]).\
            where(czCleaning.versionEnd.is_(None)).\
            where(czCleaning.kind == 'ln|cp|con')
        with Connection('r', 'read complete czCleaning') as session:
            status = pd.read_sql(q_select, session.bind).\
                fillna("").\
                rename(columns={'status': 'bewerkingsstatus'})
        df = get_problem_table(overview, pr_dropdown_values, category_values, status)[
            download_config[download_type]['col_order2']
        ]

        excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
        df.to_excel(excel_writer, sheet_name="overview", index=False)

        ws = excel_writer.sheets['overview']
        ws.freeze_panes(1, 0)
        ws.set_column('A:AH', 22)
        ws.autofilter(0, 0, df.shape[0], df.shape[1]-1)

        excel_writer.save()

        send_info = dict(
            attachment_filename='Overview_{}.xlsx'.format(
                str(dt.now())
            )
        )

    elif download_type == 'diff':
        old = flask.request.args.get('old')
        new = flask.request.args.get('new')

        with Connection('r', 'read_diff_download') as session:
            olddf = read(session, sourceTag='projectstructure', ts=old)
            newdf = read(session, sourceTag='projectstructure', ts=new)

        excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
        excel_writer = compare_download(newdf, olddf, excel_writer, new, old)
        excel_writer.save()

        send_info = dict(
            attachment_filename='Verschillen_vorig{}_nieuw{}_{}.xlsx'.format(
                old,
                new,
                str(dt.now()),
            ),
        )

    elif download_type == 'status':

        intake_value = flask.request.args.get('intake')
        selected_cells = flask.request.args.get('cells')
        hist = flask.request.args.get('hist')

        excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
        excel_writer = status_download_file(excel_writer, intake_value, selected_cells, hist)
        excel_writer.save()

        send_info = dict(
            attachment_filename='Status_{}.xlsx'.format(
                str(dt.now())
            )
        )

    strIO.seek(0)

    return flask.send_file(
        strIO,
        as_attachment=True,
        **send_info,
    )


if __name__ == '__main__':
    port = os.getenv('PORT', config.port)
    app.run_server(debug=config.debug, port=port)
