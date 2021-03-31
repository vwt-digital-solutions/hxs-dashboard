import pandas as pd
import numpy as np
import sqlalchemy as sa

from datetime import datetime as dt
from connection import Connection
from db.models import (czHierarchy, czLog, czCleaning, czFilterOptions,
                       czImportKeys, czImportMeasureValues, czSubscriptions)


def get_corrections_connect(con):
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    # %% Lees connect in Hierarchy: eerste update
    con = con.rename(columns={
        'bpnr': 'cpnr_extracted',
        'Uniek ID': 'con_objectid',
        'Code extern': 'con_opdrachtid',
    })
    overview_con = con.copy()
    overview_con = overview_con[['con_objectid',
                                 'cpnr_extracted']].drop_duplicates()
    overview_con = overview_con.rename(
        columns={'cpnr_extracted': 'parentKindKey', 'con_objectid': 'kindKey'})
    overview_con['kind'] = 'con_objectid'
    overview_con['parentKind'] = 'cpnr'

    q_1 = '''SELECT kindkey AS 'con_objectid', parentKindKey AS 'cpnr'
        FROM czHierarchy
        WHERE versionEnd IS NULL
        AND kind = 'con_objectid'
        AND parentKind = 'cpnr'
        '''
    q_2 = '''SELECT kindkey AS 'con_objectid', parentKindKey AS 'con_opdrachtid'
        FROM czHierarchy
        WHERE versionEnd IS NULL
        AND kind = 'con_objectid'
        AND parentKind = 'con_opdrachtid'
        '''

    with Connection('r', 'dataupdate con_objectid - cpnr') as session:

        a = pd.read_sql(q_1, session.bind)
        b = pd.read_sql(q_2, session.bind)
        df = a.merge(b, on='con_objectid')

    tomany = df.fillna('').groupby('con_opdrachtid').nunique().drop('con_opdrachtid', axis=1).reset_index()
    tomany = tomany[tomany['cpnr'] > 1]['con_opdrachtid'].tolist()

    connect = con[con['con_opdrachtid'].isin(tomany)].\
        fillna('')[['con_opdrachtid', 'Bouwplan nummer']]

    def remove_(row):
        return row - set([''])

    tocorrect = connect.fillna('').groupby('con_opdrachtid')['Bouwplan nummer'].apply(set)
    tocorrect = tocorrect.apply(remove_).apply(len).reset_index()
    tocorrect = tocorrect[tocorrect['Bouwplan nummer'] == 1]['con_opdrachtid'].tolist()

    upload = con[con['con_opdrachtid'].isin(tocorrect)].fillna('')

    def get_correct(row):
        final = row - set([''])
        if len(final) == 1:
            return list(final)[0]
        else:
            return ''
    upload = upload.groupby('con_opdrachtid')['cpnr_extracted'].apply(set).apply(get_correct).reset_index()

    upload = df.merge(upload, on='con_opdrachtid')
    upload = upload[upload['cpnr_extracted'].notna()]
    upload = upload[['con_objectid', 'cpnr_extracted']].rename(
        columns={'con_objectid': 'kindKey',
                 'cpnr_extracted': 'parentKindKey'}
    )
    upload['kind'] = 'con_objectid'
    upload['parentKind'] = 'cpnr'

    with Connection('w', 'dataupdate con_objectid - cpnr (correctDoubles)') as session:
        q_update = sa.update(czHierarchy).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKind == 'cpnr').\
            where(czHierarchy.kindKey.in_(upload['kindKey'].tolist())).\
            values(versionEnd=ts)
        session.execute(q_update)
        session.flush()

        czHierarchy.insert(upload.fillna(''), session, ts)

        czLog.insert([{
            'action': 'data-update',
            'description': 'conobjid-cpnr-correctDoubles',
            'created': ts,
            }], session)


def update_connect_couples(con):
    print('Update connect couples: con_objectid - cpnr')
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    # %% Lees connect in Hierarchy: eerste update
    con = con.rename(columns={
        'bpnr': 'cpnr_extracted',
        'Uniek ID': 'con_objectid',
        'Code extern': 'con_opdrachtid',
    })
    overview_con = con.copy()
    overview_con = overview_con[['con_objectid',
                                 'cpnr_extracted']].drop_duplicates()
    overview_con = overview_con.rename(
        columns={'cpnr_extracted': 'parentKindKey', 'con_objectid': 'kindKey'})
    overview_con['kind'] = 'con_objectid'
    overview_con['parentKind'] = 'cpnr'
    # %% obj-bpnr Set to version end: all values that are not in the new delivery
    print("Update connect_objectid and cpnr")
    with Connection('w', 'dataupdate con_objectid - cpnr') as session:
        # Haal op wat er nog beschikbaar is
        q = '''SELECT kindKey FROM czHierarchy
            WHERE versionEnd is NULL
            AND kind = 'con_objectid'
            AND parentKind = 'cpnr'
            '''

        new_bpnr = pd.read_sql(q, session.bind)

        # update statement: set to versionEnd which are not in the new
        q_update = sa.update(czHierarchy).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKind == 'cpnr').\
            where(~czHierarchy.kindKey.in_(overview_con['kindKey'].tolist())).\
            values(versionEnd=ts)
        session.execute(q_update)
        session.flush()

        # insert all new couples
        overview_con = overview_con[~overview_con['kindKey'].isin(
            new_bpnr['kindKey'].tolist())]
        czHierarchy.insert(overview_con.fillna(""), session, ts)

        czLog.insert([{
            'action': 'data-update',
            'description': 'conobjid-cpnr',
            'created': ts,
        }], session)

    # %% obj-opdr Update con_opdrachtid's
    print('Update connect couples: con_objectid - con_opdrachtid')
    overview_con1 = con.copy()
    overview_con1 = overview_con1[[
        'con_opdrachtid', 'con_objectid']].drop_duplicates()
    overview_con1 = overview_con1.rename(
        columns={'con_objectid': 'kindKey', 'con_opdrachtid': 'parentKindKey'})
    overview_con1['kind'] = 'con_objectid'
    overview_con1['parentKind'] = 'con_opdrachtid'

    print("Update connect_objectid and con_opdrachtid")
    # Haal op wat er nog beschikbaar is
    with Connection('w', 'dataupdate con_objectid - con_opdrachtid') as session:
        q = '''Select kindKey from czHierarchy
                        where versionEnd is NULL
                        and kind = 'con_objectid'
                        and parentKind = 'con_opdrachtid'
                        '''

        new_opdrachtid = pd.read_sql(q, session.bind)

        # Set to version end: all values that are not in the new delivery
        q_update = sa.update(czHierarchy).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKind == 'con_opdrachtid').\
            where(~czHierarchy.kindKey.in_(overview_con1['kindKey'].tolist())).\
            values(versionEnd=ts)
        session.execute(q_update)
        session.flush()

        # insert all new couples
        overview_con1 = overview_con1[~overview_con1['kindKey'].isin(
            new_opdrachtid['kindKey'].tolist())]
        czHierarchy.insert(overview_con1, session, ts)

        czLog.insert([{
            'action': 'data-update',
            'description': 'conobjid-conopdrid',
            'created': ts,
        }], session)


def update_cp_couples(cp, kind, add_new=False):
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    # %% Lees connect in Hierarchy: eerste update
    cp = cp.rename(columns={kind: 'parentKindKey', 'bpnr': 'kindKey'})
    cp['kind'] = 'cpnr'
    cp['parentKind'] = kind
    print("Update cpnr and {}".format(kind))

    if add_new:
        # Only import the ones that are not already in the database
        with Connection('w', 'dataupdate cpnr - contractor') as session:
            # Haal op wat er nog beschikbaar is
            q = sa.select(czHierarchy.kindKey).\
                where(czHierarchy.versionEnd.is_(None)).\
                where(czHierarchy.kind == 'cpnr').\
                where(czHierarchy.parentKind == kind)

            new_bpnr = pd.read_sql(q, session.bind)

            # update statement: set to versionEnd which are not in the new
            q_update = sa.update(czHierarchy).\
                where(czHierarchy.versionEnd.is_(None)).\
                where(czHierarchy.kind == 'cpnr').\
                where(czHierarchy.parentKind == kind).\
                where(~czHierarchy.kindKey.in_(cp['kindKey'].tolist())).\
                values(versionEnd=ts)
            session.execute(q_update)
            session.flush()

            # insert all new couples
            cp = cp[~cp['kindKey'].isin(new_bpnr['kindKey'].tolist())]
            czHierarchy.insert(cp.fillna(''), session, ts)

            czLog.insert([{
                'action': 'data-update',
                'description': 'conobjid-cpnr',
                'created': ts,
            }], session)
    else:
        # 'Overwrite' current values
        with Connection('w', 'dataupdate cpnr - {}'.format(kind)) as session:
            # update statement: set to versionEnd for the new values
            q_update = sa.update(czHierarchy).\
                where(czHierarchy.versionEnd.is_(None)).\
                where(czHierarchy.kind == 'cpnr').\
                where(czHierarchy.parentKind == kind).\
                where(czHierarchy.kindKey.in_(cp['kindKey'].tolist())).\
                values(versionEnd=ts)
            session.execute(q_update)
            session.flush()

            # insert all new couples
            czHierarchy.insert(cp.fillna(''), session, ts)

            czLog.insert([{
                'action': 'data-update',
                'description': '{}-cpnr'.format(kind),
                'created': ts,
            }], session)


def update_czCleaning(overview_clean, session=None):
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    overview_clean['key'] = overview_clean['ln_id'] + '|' + \
        overview_clean['bpnr'] + '|' + overview_clean['con_opdrachtid']
    overview_clean['kind'] = 'ln|cp|con'
    overview_clean = overview_clean[['key', 'kind']]
    overview_clean['status'] = None

    print("Update cleaning")
    q = '''Select `key` from czCleaning
            where versionEnd is NULL
            and kind = 'ln|cp|con'
            '''
    q_update = sa.update(czCleaning).\
        where(czCleaning.versionEnd.is_(None)).\
        where(czCleaning.kind == 'ln|cp|con').\
        where(~czCleaning.key.in_(overview_clean['key'].tolist())).\
        values(versionEnd=ts)

    if session is None:
        with Connection('w', 'dataupdate czCleaning') as session:
            cleaned = pd.read_sql(q, session.bind)

            # Set to version end: all values that are not in the new delivery
            session.execute(q_update)
            session.flush()

            # insert all new couples
            overview_clean = overview_clean[~overview_clean['key'].isin(
                cleaned['key'].tolist())]
            czCleaning.insert(overview_clean, session, ts)

            czLog.insert([{
                'action': 'data-update',
                'description': 'cleaning ln|cp|con',
                'created': ts,
            }], session)
    else:
        cleaned = pd.read_sql(q, session.bind)

        # Set to version end: all values that are not in the new delivery
        session.execute(q_update)
        session.flush()

        # insert all new couples
        overview_clean = overview_clean[~overview_clean['key'].isin(
            cleaned['key'].tolist())]
        czCleaning.insert(overview_clean, session, ts)

        czLog.insert([{
            'action': 'data-update',
            'description': 'cleaning ln|cp|con',
            'created': ts,
        }], session)


def init_tables():
    fd = open('db/create_procedures.sql', 'r')
    sqlFile = fd.read()
    fd.close()
    with Connection() as session:
        session.execute(sqlFile)
        session.commit()


def read_dump(filename):
    insert = open(filename, 'r')
    with Connection('w', 'restore database') as session:
        for line in insert:
            if line.startswith('INSERT INTO'):
                session.execute(line)


def update_dropdown_values(dropdown_id, values, session=None):
    print('Start update dropdown "{}"'.format(dropdown_id))
    values = list(set(values) - set(['', np.nan, ' ', None]))
    df = pd.DataFrame({'kind': dropdown_id, 'value': values})
    if session is None:
        with Connection('w', 'update dropdownvalues "{}"'.format(dropdown_id)) as session:
            czFilterOptions.insert(df, session)
    else:
        czFilterOptions.insert(df, session)
    print('End update dropdown "{}"'.format(dropdown_id))


def compare_and_insert(session, df, sourceTag, sourceKey='sourceKey', valueDate=None, ts=None, load_type='diff'):
    print('Insert for sourceTag {}'.format(sourceTag))
    if ts is None:
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    orig = read(session, stagingSourceTag=sourceTag).reset_index(drop=False)

    if load_type == 'diff':
        add, remove = calculate_diff(orig.fillna(''), df.fillna(''))
    elif load_type == 'full':
        add = df.fillna('')
        remove = orig.fillna('')

    if len(remove) > 0:
        # Update statement on czImportKeys
        q_update = sa.sql.expression.update(czImportKeys).\
            where(czImportKeys.sourceTag == sourceTag).\
            where(czImportKeys.sourceKey.in_(remove['sourceKey'].tolist())).\
            where(czImportKeys.versionEnd.is_(None)).\
            values(versionEnd=ts)

        session.execute(q_update)
        session.flush()

    if len(add) > 0:
        add = add.reset_index(drop=True)
        add['sourceId'] = add.index + 1
        for i, row in add.iterrows():
            insert(
                session,
                row.to_dict(),
                sourceTag=sourceTag,
                sourceKey=sourceKey,
                valueDate=valueDate,
                ts=ts,
                versionManagement=False,
            )


def insert(session, data_dict, sourceTag, sourceKey='sourceKey', valueDate=None, ts=None, versionManagement=True):
    if ts is None:
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    sourceKey = data_dict.pop(sourceKey)
    sourceId = data_dict.pop('sourceId', 0)

    if valueDate is None:
        valueDate = ts

    if versionManagement:
        # Update statement on czImportKeys
        q_update = sa.sql.expression.update(czImportKeys).\
            where(czImportKeys.sourceKey == sourceKey).\
            where(czImportKeys.sourceTag == sourceTag).\
            where(czImportKeys.versionEnd is None).\
            values(versionEnd=ts)

        session.execute(q_update)
        session.flush()

    # Insert statement into czImportKeys
    keys_insert = czImportKeys(
        sourceKey=sourceKey,
        sourceTag=sourceTag,
        delete=0,
        version=ts,
    )

    session.add(keys_insert)
    session.flush()

    # Create dict containing the data for import
    data = dict(
        importId=keys_insert.importId,
        sourceKey=sourceKey,
        sourceId=sourceId,
        valueDate=valueDate,
        )

    insert_values = []
    for measure, value in data_dict.items():
        if (value != '') and (value is not None) and (value is not np.nan):
            insert_values.append(
                {**data, **dict(
                    measure=measure,
                    value=value,
                )
                }
            )

    # Insert data into czImportMeasureValues
    q_insert = sa.sql.expression.insert(czImportMeasureValues, values=insert_values)
    session.execute(q_insert)
    session.flush()


def read(session, sourceTag=None, key=[], measure=[], ts=None, stagingSourceTag=None):  # noqa: C901
    # Keys in list
    if isinstance(key, str):
        key = [key]
    elif not isinstance(key, list):
        raise ValueError('Wrong type of key: {} instead of string or list'.format(type(key)))
    if len(key) > 0:
        key = list(
            set(key) -
            set([np.nan])
        )

    # Measures in list
    if isinstance(measure, str):
        measure = [measure]
    elif not isinstance(measure, list):
        raise ValueError('Wrong type of measure: {} instead of string or list'.format(type(measure)))
    if len(measure) > 0:
        measure = list(
            set(measure) -
            set([np.nan])
        )

    if ts == 'all':
        q = session.query(
            czImportMeasureValues.sourceKey,
            czImportMeasureValues.measure,
            czImportMeasureValues.value,
            czImportKeys.version,
            czImportKeys.versionEnd,
        )
        set_index = [0, 3, 4, 1]
    else:
        q = session.query(
            czImportMeasureValues.sourceKey,
            czImportMeasureValues.measure,
            czImportMeasureValues.value
        )
        set_index = [0, 1]

    q = q.\
        join(czImportKeys, czImportMeasureValues.importId == czImportKeys.importId).\
        join(czSubscriptions, czImportKeys.sourceTag == czSubscriptions.stagingSourceTag).\
        filter(czImportKeys.delete == 0)

    if ts is None:
        q = q.filter(czImportKeys.versionEnd.is_(None))
    elif ts != 'all':
        q = q.filter(czImportKeys.version <= ts).\
            filter(sa.or_(czImportKeys.versionEnd.is_(None),
                   czImportKeys.versionEnd > ts))

    if stagingSourceTag is not None:
        q = q.filter(czSubscriptions.stagingSourceTag == stagingSourceTag)
    else:
        q = q.filter(czSubscriptions.sourceTag == sourceTag)

    if len(key) > 0:
        q = q.filter(czImportKeys.sourceKey.in_(key))

    if len(measure) > 0:
        q = q.filter(czImportMeasureValues.measure.in_(measure))

    df = session.execute(q)
    df = pd.DataFrame(df)

    if len(df) == 0:
        return pd.DataFrame([])
    else:
        df = df.set_index(set_index).unstack()
        df.columns = df.columns.levels[1].tolist()
        # return df
        df.reset_index(inplace=True)
        df = df.rename(columns={
            0: 'sourceKey',
            3: 'version',
            4: 'versionEnd',
        })
        df.columns.name = ''
        df.set_index('sourceKey', inplace=True)

        # sourceTag specific additions:
        cols = {
            'projectstructure': [
                'ln_id',
                'bpnr',
                'con_opdrachtid',
                'categorie',
                'Projectstructuur constateringen',
                'koppeling'
            ],
        }
        to_add = cols.get(sourceTag, None)
        if to_add is not None:
            for col in to_add:
                if col not in list(df):
                    df[col] = ''

        return df


def calculate_diff(df_old, df_new):
    if (len(df_old) == 0) | (len(df_new) == 0):
        return df_new, df_old
    joined = df_old.astype(str).fillna('').drop_duplicates().merge(
        df_new.astype(str).drop_duplicates().fillna(''), how='outer', indicator=True)
    remove = joined.query("_merge == 'left_only'").drop('_merge', axis=1)
    add = joined.query("_merge == 'right_only'").drop('_merge', axis=1)
    return add, remove
