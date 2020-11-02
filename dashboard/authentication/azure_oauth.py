from flask import (
    redirect,
    url_for,
    Response,
    abort,
    request,
    session,
)
from flask_dance.contrib.azure import (
    make_azure_blueprint,
    azure,
)
from jwkaas import JWKaas
import logging
import flask_login

from .auth import Auth


class AzureOAuth(Auth):
    def __init__(self, app, client_id, client_secret, expected_issuer, expected_audience, jwks_url, tenant,
                 session_secret, scopes=None, redirect_url=None, e2e_expected_audience=None, e2e_client_id=None):
        super(AzureOAuth, self).__init__(app)
        azure_bp = make_azure_blueprint(
            client_id=client_id,
            client_secret=client_secret,
            scope=scopes,
            redirect_url=redirect_url,
            tenant=tenant,
        )
        app.server.register_blueprint(azure_bp, url_prefix="/login")
        app.server.secret_key = session_secret
        self._jwkaas = JWKaas(expected_audience, expected_issuer, jwks_url=jwks_url)
        self._e2e_jwkaas = JWKaas(e2e_expected_audience,
                                  expected_issuer,
                                  jwks_url=jwks_url)
        self.client_id = client_id
        self.e2e_client_id = e2e_client_id
        self.logout_url = None
        self.user = None

    def is_authorized(self):
        if request.args.get('access_token') or session.get('access_token'):
            if request.args.get('access_token'):
                session['access_token'] = request.args.get('access_token')
            token_info = self._e2e_jwkaas.get_token_info(
                (session['access_token'] if session.get('access_token')
                    else request.args.get('access_token')))
            if not token_info or token_info['appid'] != self.e2e_client_id:
                logging.warning('Invalid access token')
                return abort(401)
            self.user = 'opensource.e2e@vwtelecom.com'
            token_info['roles'] = ['czdashboard.user']
        else:
            if not azure.authorized or azure.token['expires_in'] < 10:
                # send to azure login
                return False
            token_info = self._jwkaas.get_token_info(azure.access_token)
            self.user = token_info['unique_name']
            self.logout_url = "https://login.microsoftonline.com/{}/oauth2/v2.0/logout".format(token_info['tid'])

        if token_info:
            if 'roles' in token_info and 'czdashboard.user' in token_info['roles']:
                return True
            else:
                logging.warning('Missing required role czdashboard.user')
                return abort(401)
        else:
            logging.warning('Invalid access token')
            return abort(401)

    def login_request(self):
        # send to azure auth page
        return redirect(url_for("azure.login"))

    def logout_user(self):
        flask_login.logout_user()

    def get_logout_url(self):
        # returns logout url
        return self.logout_url

    def get_user(self):
        # returns username
        return self.user

    def auth_wrapper(self, f):
        def wrap(*args, **kwargs):
            if not self.is_authorized():
                return Response(status=401)

            response = f(*args, **kwargs)
            logging.info(response)
            return response
        return wrap

    def index_auth_wrapper(self, original_index):
        def wrap(*args, **kwargs):
            if self.is_authorized():
                return original_index(*args, **kwargs)
            else:
                return self.login_request()
        return wrap
