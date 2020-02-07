import utils
import config
import sqlalchemy as sa

from datetime import datetime as dt


DB_PASSWORD = utils.decrypt_secret(
    config.database['project_id'],
    config.database['kms_region'],
    config.database['kms_keyring'],
    config.database['kms_key'],
    config.database['enc_password']
)

if 'db_ip' in config.database:
    SACN = 'mysql://{}:{}@{}:3306/{}?charset=utf8&ssl_ca={}&ssl_cert={}&ssl_key={}'.format(
        config.database['db_user'],
        DB_PASSWORD,
        config.database['db_ip'],
        config.database['db_name'],
        config.database['server_ca'],
        config.database['client_ca'],
        config.database['client_key']
    )
else:
    SACN = 'mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:europe-west1:{}'.format(
        config.database['db_user'],
        DB_PASSWORD,
        config.database['db_name'],
        config.database['project_id'],
        config.database['instance_id']
    )


class Connection:
    # Initialize as 'w' (write, default) or 'r' (read)
    # Contextmanager which returns a sqlalchemy session
    def __init__(self, intent='w', goal='', sacn=None):
        if intent == 'w':
            self.write = True
        elif intent == 'r':
            self.write = False
        else:
            raise ValueError(
                "No 'w' or 'r' given as intent in initialization of Class connection")

        self.goal = goal

        if sacn is None:
            self.sacn = SACN
        else:
            self.sacn = sacn

    def __enter__(self):
        self.engine = sa.create_engine(self.sacn)
        self.session = sa.orm.session.sessionmaker(
            self.engine, autoflush=True, autocommit=False)()
        self._id = dt.now().strftime('%Y%m%d%H%M%S%f')
        self.name = self._id if self.goal == '' else self._id + ' - ' + self.goal

        print("Session opened: {}".format(self.name))

        return self.session

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            if self.write:
                self.session.commit()
                print('Session {} committed'.format(self._id))

        elif exc_type == sa.exc.OperationalError:
            print('No database connection')

            self.session.close()
            print('Session {} closed'.format(self._id))

            raise ValueError("Database connection incorrect")

        else:
            if self.write:
                self.session.rollback()
                print('Session {} rolled back'.format(self._id))
                print(exc_type, exc_value)
            raise exc_type(exc_value)

        self.session.close()
        print('Session closed: {}'.format(self.name))
