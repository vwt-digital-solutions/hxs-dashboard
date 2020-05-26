import dash
import flask
import config
import utils
import authentication
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import os
from flask_sslify import SSLify

server = flask.Flask(__name__)

if 'GAE_INSTANCE' in os.environ:
    SSLify(server, permanent=True)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    assets_folder='assets/',
    server=server
)
app.title = f"[{config.environment}] {config.title}"
app.config.supress_callback_exceptions = True
app.scripts.config.serve_locally = False


if config.authentication:
    auth = config.authentication
    secret_base64 = auth['encrypted_session_secret']
    auth['session_secret'] = utils.decrypt_secret(
        auth['kms_project'],
        auth['kms_region'],
        auth['kms_keyring'],
        auth['kms_key'],
        secret_base64
    )
    auth = authentication.AzureOAuth(
        app,
        auth['client_id'],
        auth['client_secret'],
        auth['expected_issuer'],
        auth['expected_audience'],
        auth['jwks_url'],
        auth['tenant'],
        auth['session_secret'],
        auth['required_scopes'],
        e2e_expected_audience=auth.get('e2e_expected_audience', None),
        e2e_client_id=auth.get('e2e_client_id', None)
    )


def get_user():
    if config.authentication:
        return auth.get_user()
    else:
        return 'no-authentication@testing.test'


def is_authenticated():
    return auth.is_authorized()


def get_logout_url():
    if config.authentication:
        return auth.get_logout_url()
    else:
        return "www.example.com"


def get_asset(name):
    return app.get_asset_url(name)


app.css.append_css(
    {"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"}
)

dcc._js_dist[0]['external_url'] = 'https://cdn.plot.ly/plotly-basic-latest.min.js'
