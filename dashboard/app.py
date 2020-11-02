import dash
import flask
import config
import utils
import authentication
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import os
from flask_sslify import SSLify
from flask_behind_proxy import FlaskBehindProxy

server = flask.Flask(__name__)
FlaskBehindProxy(server)

if 'GAE_INSTANCE' in os.environ:
    SSLify(server, permanent=True)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    assets_folder='assets/',
    server=server
)
app.title = f"[{config.environment}] {config.title}"
app.config.suppress_callback_exceptions = True
app.scripts.config.serve_locally = False


if config.authentication:
    auth = config.authentication
    auth = authentication.AzureOAuth(
        app,
        auth['client_id'],
        auth['client_secret'],
        auth['expected_issuer'],
        auth['expected_audience'],
        auth['jwks_url'],
        auth['tenant'],
        utils.get_secret(auth['project_id'], auth['secret_name']),
        auth['required_scopes'],
        auth.get('redirect_url', None),
        e2e_expected_audience=auth.get('e2e_expected_audience', None),
        e2e_client_id=auth.get('e2e_client_id', None),
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
