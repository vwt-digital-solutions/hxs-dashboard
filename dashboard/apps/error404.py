import dash_html_components as html
import dash_bootstrap_components as dbc
from connection import Connection
from db.models import czLog


def get_body(pathname):
    print('Niet bestaande pagina opgevraagd: "{}"'.format(pathname))

    with Connection() as session:
        czLog.insert([{
            'action': 'webpage',
            'description': '404',
            'parameter': pathname,
        }], session)

    body = dbc.Container([
        dbc.Row([
            html.A(pathname)
        ]),
        dbc.Row([
            html.H1('Pagina bestaat niet')
        ]),
        dbc.Row([
            html.H5('Klik op vorige of kies een pagina in het menu rechtsboven')
        ]),
    ])

    return body
