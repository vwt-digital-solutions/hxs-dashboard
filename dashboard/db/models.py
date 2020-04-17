import sqlalchemy as sa
from app import get_user
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class czHierarchy(Base):
    __tablename__ = 'czHierarchy'
    _id = sa.Column('id', sa.types.INTEGER, nullable=False,
                    primary_key=True, autoincrement=True)
    kind = sa.Column('kind', sa.types.String(128), nullable=False)
    kindKey = sa.Column('kindKey', sa.types.String(128), nullable=True)
    parentKind = sa.Column('parentKind', sa.types.String(128), nullable=False)
    parentKindKey = sa.Column(
        'parentKindKey', sa.types.String(128), nullable=True)
    versionEnd = sa.Column('versionEnd', sa.types.DATETIME, nullable=True)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())
    updated = sa.Column('updated', sa.types.DATETIME,
                        nullable=True, default=None)

    def __init__(self, kind, kindKey, parentKind, parentKindKey, versionEnd=None, created=sa.func.now()):
        self.kind = kind
        self.kindKey = kindKey
        self.parentKind = parentKind
        self.parentKindKey = parentKindKey
        self.versionEnd = versionEnd
        self.created = created

    def insert(df, session, created=None):
        df = df[['kind', 'kindKey', 'parentKind', 'parentKindKey']]
        if created:
            df['created'] = created
        chunksize = 500
        i = 0
        while i*chunksize < len(df):
            insert = df.iloc[i*chunksize:(i+1) *
                             chunksize].to_dict(orient='records')
            q_insert = sa.sql.expression.insert(czHierarchy, values=insert)
            session.execute(q_insert)
            # session.add_all(
            #     [czHierarchy(r['kind'], r['kindKey'], r['parentKind'], r['parentKindKey']) for r in insert])
            session.flush()
            i += 1
        print('czHierarcy - flushed')


class czLog(Base):
    __tablename__ = 'czLog'
    _id = sa.Column('id', sa.types.INTEGER, nullable=False,
                    primary_key=True, autoincrement=True)
    action = sa.Column('action', sa.types.String(128), nullable=False)
    parameter = sa.Column('parameter', sa.types.String(128), nullable=True)
    description = sa.Column('description', sa.types.String(128), nullable=True)
    user = sa.Column('user', sa.types.String(128), nullable=False)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())

    def __init__(self, action, user, description=None, parameter=None):
        self.action = action
        self.parameter = parameter
        self.description = description
        self.user = user

    def insert(insert, session):
        insert = add_user(insert)
        q_insert = sa.sql.expression.insert(czLog, values=insert)
        session.execute(q_insert)
        session.flush()
        print('czLog - flush')


class czCleaning(Base):
    __tablename__ = 'czCleaning'
    _id = sa.Column('id', sa.types.INTEGER, nullable=False,
                    primary_key=True, autoincrement=True)
    kind = sa.Column('kind', sa.types.String(128), nullable=False)
    key = sa.Column('key', sa.types.String(128), nullable=True)
    status = sa.Column('status', sa.types.String(128), nullable=True)
    versionEnd = sa.Column('versionEnd', sa.types.DATETIME, nullable=True)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())
    updated = sa.Column('updated', sa.types.DATETIME,
                        nullable=True, default=None)

    def __init__(self, kind, key, status, updated=None, created=sa.func.now()):
        self.kind = kind
        self.key = key
        self.status = status
        self.updated = updated
        self.created = created

    def insert(df, session, created=None):
        df = df[['kind', 'key', 'status']]
        if created:
            df['created'] = created
        chunksize = 500
        i = 0
        while i*chunksize < len(df):
            insert = df.iloc[i*chunksize:(i+1) *
                             chunksize].to_dict(orient='records')
            q_insert = sa.sql.expression.insert(czCleaning, values=insert)
            session.execute(q_insert)
            session.flush()
            i += 1
        print('czCleaning - flushed')


class czComment(Base):
    __tablename__ = 'czComment'
    _id = sa.Column('id', sa.types.INTEGER, nullable=False,
                    primary_key=True, autoincrement=True)
    kind = sa.Column('kind', sa.types.String(128), nullable=False)
    kindKey = sa.Column('kindKey', sa.types.String(128), nullable=False)
    comment = sa.Column('comment', sa.types.TEXT, nullable=False)
    user = sa.Column('user', sa.types.String(128), nullable=False)
    versionEnd = sa.Column('versionEnd', sa.types.DATETIME, nullable=True)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())

    def __init__(self, kind, kindKey, comment):
        self.kind = kind
        self.kindKey = kindKey
        self.comment = comment
        # self.user = user

    def insert(insert, session):
        insert = add_user(insert)
        q_insert = sa.sql.expression.insert(czComment, values=insert)
        session.execute(q_insert)
        session.flush()
        print('czComment - flush')


def add_user(insert):
    if isinstance(insert, dict):
        if 'user' not in insert.keys():
            insert['user'] = get_user()
    elif isinstance(insert, list):
        for el in insert:
            el = add_user(el)
    return insert


class czFilterOptions(Base):
    __tablename__ = 'czFilterOptions'
    _id = sa.Column('id', sa.types.INTEGER, nullable=False,
                    primary_key=True, autoincrement=True)
    kind = sa.Column('kind', sa.types.String(128), nullable=False)
    value = sa.Column('value', sa.types.String(128), nullable=False)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())

    def __init__(self, kind, value):
        self.kind = kind
        self.value = value

    def insert(df, session, created=None):
        df = df[['kind', 'value']]
        q_delete = sa.delete(czFilterOptions).\
            where(czFilterOptions.kind.in_(df['kind'].drop_duplicates().tolist()))
        session.execute(q_delete)
        session.flush()

        if created:
            df['created'] = created

        chunksize = 500
        i = 0
        while i*chunksize < len(df):
            insert = df.iloc[i*chunksize:(i+1) *
                             chunksize].to_dict(orient='records')
            q_insert = sa.sql.expression.insert(czFilterOptions, values=insert)
            session.execute(q_insert)
            session.flush()
            i += 1


class czImportKeys(Base):
    __tablename__ = 'czImportKeys'
    importId = sa.Column('id', sa.types.INTEGER, nullable=False,
                         primary_key=True, autoincrement=True)
    sourceTag = sa.Column('sourceTag', sa.types.String(64), nullable=False)
    sourceKey = sa.Column('sourceKey', sa.types.String(128), nullable=False)
    delete = sa.Column('delete', sa.types.INTEGER, default=0)
    version = sa.Column('version', sa.types.DATETIME, nullable=False)
    versionEnd = sa.Column('versionEnd', sa.types.DATETIME, default=None, nullable=True)

    def __init__(self, sourceTag, sourceKey, delete, version):
        self.sourceTag = sourceTag
        self.sourceKey = sourceKey
        self.delete = delete
        self.version = version


class czImportMeasureValues(Base):
    __tablename__ = 'czImportMeasureValues'
    importId = sa.Column('importId', sa.types.INTEGER, nullable=False,
                         primary_key=True, autoincrement=True)
    sourceId = sa.Column('sourceId', sa.types.INTEGER, nullable=False)
    sourceKey = sa.Column('sourceKey', sa.types.String(128), nullable=False)
    measure = sa.Column('measure', sa.types.String(128), nullable=False)
    value = sa.Column('value', sa.types.TEXT, nullable=True)
    valueDate = sa.Column('valueDate', sa.types.DATETIME, nullable=False)

    def __init__(self, importId, sourceId, sourceKey, measure, value, valueDate):
        self.importId = importId
        self.sourceId = sourceId
        self.sourceKey = sourceKey
        self.delete = measure
        self.version = value
        self.valueDate = valueDate


class czSubscriptions(Base):
    __tablename__ = 'czSubscriptions'

    stagingSourceTag = sa.Column('stagingSourceTag', sa.types.String(64), nullable=False, primary_key=True)
    sourceTag = sa.Column('sourceTag', sa.types.String(32), nullable=False)
    name = sa.Column('name', sa.types.String(64), nullable=False)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())
