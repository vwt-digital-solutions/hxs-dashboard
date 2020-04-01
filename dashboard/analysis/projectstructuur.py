# import os
import utils
import config
import numpy as np
import pandas as pd
import sqlalchemy as sa
import re

from analysis.connectz import expand_column
from datetime import datetime as dt
from db.queries import read
from connection import Connection
from db.models import czHierarchy, czLog


def merge_columns_text(df, columnnames, columnname_new=None, connector='; '):
    if columnname_new is None:
        columnname_new = columnnames[0]

    for i in range(len(columnnames)-1):
        place = (
            (df[columnnames[i]].notna()) & (df[columnnames[i]].notna()) &
            (df[columnnames[i+1]].notna()) & (df[columnnames[i+1]].notna())
        )
        df.at[place, columnname_new] = df[place][columnnames[i]] + \
            connector + df[place][columnnames[i+1]]
        df.at[~place, columnname_new] = df[~place][columnnames[i]].fillna(
            '') + df[~place][columnnames[i+1]].fillna('')
        df.at[(df[columnname_new].isna()), columnname_new] = np.nan
        columnnames[i+1] = columnname_new
    return df


class cp():
    def __init__(self):
        self.sourceTag = 'ChangePoint'
        self.measures = [
            'project_id',
            'project_name',
            'project_class',
            'cp_fase',
            'date_created',
            'cp_capacity',
            'cp_status'
        ]

    def _get_data(self, session):
        self.df = read(
            session, self.sourceTag, measure=self.measures)
        # Add bpnr
        q_select = sa.select([czHierarchy.kindKey, czHierarchy.parentKindKey]).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'cp_id').\
            where(czHierarchy.parentKind == 'cpnr_extracted')
        df = pd.read_sql(q_select, session.bind).rename(columns={
            'kindKey': 'sourceKey',
            'parentKindKey': 'bpnr',
        })
        self.df = self.df.reset_index().merge(df, on='sourceKey', how='left')

        # Add contractor
        q_select = sa.select([czHierarchy.kindKey, czHierarchy.parentKindKey]).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'cpnr').\
            where(czHierarchy.parentKind == 'contractor')
        df = pd.read_sql(q_select, session.bind).rename(columns={
            'kindKey': 'bpnr',
            'parentKindKey': 'contractor',
        })
        self.add_contractor(df)

    def add_contractor(self, contractor=None):
        if contractor is None:
            self.df['contractor'] = np.nan
        else:
            self.df = self.df.merge(contractor, on=['bpnr'], how='left')

    # def reduceConnectZ(self):
    #     self.nb = self.nb[self.nb['contractor'] == 'Connect-Z Utrecht']

    def splitFrames(self):
        cp = self.df.copy()
        self.nb = cp[cp['project_class'].str.lower().str.contains('nieuwbouw')]
        # self.reduceConnectZ()
        self.rec = cp[(cp['project_class'].str.contains('Reconstructies')) &
                      ~(cp['project_class'].str.contains('Kleine'))]
        self.krec = cp[(cp['project_class'].str.contains('Kleine'))]


class inforln():
    def __init__(self):
        self.sourceTag = 'InforLN'
        self.measures = [
            'ln_id',
            'ln_project_status',
            'ln_fase',
            'ln_fase_description',
            'project_description',
            'search_argument',
            'search_argument2',
            'type_hierarchy',
            'main_project'
        ]

    def _get_data(self, session):
        self.df_proj = read(
            session, self.sourceTag, measure=self.measures)

        # Get extracted information
        q_select = sa.select([czHierarchy.kindKey, czHierarchy.parentKind, czHierarchy.parentKindKey]).\
            where(czHierarchy.kind == 'ln_id').\
            where(czHierarchy.parentKind.in_(['con_opdrachtid_extracted', 'cpnr_extracted'])).\
            where(czHierarchy.versionEnd.is_(None))

        df = pd.read_sql(q_select, session.bind).pivot(
            index='kindKey', columns='parentKind', values='parentKindKey')
        df = df.reset_index().rename(columns={
            'kindKey': 'ln_id',
            'con_opdrachtid_extracted': 'con_opdrachtid',
            'cpnr_extracted': 'bpnr',
        })

        # merge dataframes
        self.df_proj = self.df_proj.merge(df, on='ln_id', how='left')

        # Fillna('')
        self.df_proj['con_opdrachtid'] = self.df_proj['con_opdrachtid'].fillna(
            '')
        self.df_proj['bpnr'] = self.df_proj['bpnr'].fillna('')

    def get_active(self):
        self.filterProjectNumbers()
        nummer34 = self.df_proj['ln_id'].str.startswith('34')
        nummer35 = self.df_proj['ln_id'].str.startswith('35')
        nummer45 = self.df_proj['ln_id'].str.startswith('45')
        nummer40 = self.df_proj['ln_id'].str.startswith('40')
        nummer31 = self.df_proj['ln_id'].str.startswith('31')
        self.df_proj['active_ln'] = self.df_proj['ln_project_status'].str.lower().isin([
            'actief', 'gereed', 'vrij'])
        actief = self.df_proj['active_ln']
        # LN actieve nieuwbouw projecten (34, 31 en 35 nummers)
        self.df = self.df_proj[(nummer34 | nummer35 |
                                nummer45 | nummer40 |
                                nummer31) & actief]
        # Maak set met overige codes (niet 34 of 35 of 31)
        self.df_non3435 = self.df_proj[~(
            nummer34 | nummer35 | nummer45 | nummer40 | nummer31) & actief]
        # Maak set met inactieve LN nieuwbouw projecten
        self.df_nonActive = self.df_proj.loc[(
            nummer34 | nummer35 | nummer45 | nummer40 | nummer31) & ~actief]

    def filterProjectNumbers(self):
        """Examination of data shows that 31 out of 21169 project numbers have length less than nine, so we filter those out
        as they seem to be indicative of messy entries.

        For example, the length-8 project numbers all start with either `SJAB` or `TEMP`.
        All length-9 project numbers (in our initial data dumps) are fully numeric and can be retained."""

        # Filter only lenth-9 and alphanumeric project codes. This check can be extended with self.df_proj['Project'].str.isnumeric()
        mask = self.df_proj['ln_id'].str.len() == 9
        self.df_proj = self.df_proj.loc[mask]


class connect_vz():
    def __init__(self):
        self.sourceTag = 'Connect'
        self.measures = [
            'con_objectid',
            'con_opdrachtid',
            'object_pc',
            'order_type',
            'status_request',
            'status_object',
            'cost_characteristic',
            'reason_cancelation',
            'status_payment',
            'date_request',
            'date_request',
            'execution_type',
            'contractor_area',
        ]

        pc = utils.download_as_dataframe(config.tmp_bucket, config.files['connect_pc4'])
        self.pc4codes = pc[pc['Partner'].str.contains(
            'Connect-Z Utrecht')]['PC4CODE'].astype(str).unique()

    def _update_cpnr_corrected(self, session):
        # Get cpnr_extracted, cpnr_corrected, con_opdrachtid per con_objectid
        q = sa.select([czHierarchy.kindKey, czHierarchy.parentKind, czHierarchy.parentKindKey]).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKind.in_(['cpnr_extracted', 'cpnr_corrected', 'con_opdrachtid']))
        df_cpnr = pd.read_sql(q, session.bind)

        # Pivot
        df_cpnr = df_cpnr.pivot(
            index='kindKey',
            columns='parentKind',
            values='parentKindKey').\
            reset_index().\
            rename(columns={'kindKey': 'con_objectid'})

        # If missing: add cpnr_corrected
        if 'cpnr_corrected' not in list(df_cpnr):
            df_cpnr = df_cpnr.assign(cpnr_corrected=np.nan)

        # Empty cpnr_extracted: explicit to np.nan
        df_cpnr['cpnr_extracted'] = df_cpnr['cpnr_extracted'].replace('', np.nan)

        # Use group by to find the 'opdrachten' with no 'cpnr_corrected' and exactly 1 'cpnr_extracted'
        to_find = df_cpnr.groupby('con_opdrachtid').nunique()
        to_find = to_find[
            (to_find['cpnr_corrected'] == 0) &
            (to_find['cpnr_extracted'] == 1)
        ]

        # Create 'upload', to update the cpnr_corrected in the database
        upload = df_cpnr[df_cpnr['con_opdrachtid'].isin(to_find.index)]

        upload = upload.assign(cpnr_corrected=upload.
                               sort_values(by=['con_opdrachtid', 'cpnr_extracted'])['cpnr_extracted'].
                               fillna(method='ffill'))

        upload = upload.rename(
            columns={
                'con_objectid': 'kindKey',
                'cpnr_corrected': 'parentKindKey',
                }
        )[['kindKey', 'parentKindKey']]
        upload = upload.\
            assign(kind='con_objectid').\
            assign(parentKind='cpnr_corrected')

        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

        # Update statement
        q_update = sa.update(czHierarchy).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKind == 'cpnr_corrected').\
            where(czHierarchy.kindKey.in_(upload['kindKey'].tolist())).\
            values(versionEnd=ts)
        session.execute(q_update)
        session.flush()

        # Insert statement
        czHierarchy.insert(upload, session, ts)

        czLog.insert([{
            'action': 'data-update',
            'description': 'conobjid-cpnr-update',
            'created': ts,
        }], session)

    def _get_data(self, session):
        self.df_orig = read(
            session, self.sourceTag, measure=self.measures)
        self.df_orig['date_request'] = pd.to_datetime(
            self.df_orig['date_request'])

        q_select = sa.select([czHierarchy.kindKey, czHierarchy.parentKindKey]).\
            where(czHierarchy.versionEnd.is_(None)).\
            where(czHierarchy.kind == 'con_objectid').\
            where(czHierarchy.parentKind == 'cpnr_corrected')

        df = pd.read_sql(q_select, session.bind).rename(columns={
            'kindKey': 'con_objectid',
            'parentKindKey': 'bpnr',
        })

        self.df_orig = self.df_orig.merge(df, on='con_objectid', how='left')
        self.df_orig['bpnr'] = self.df_orig['bpnr'].replace('', np.nan)

        self.df = self.df_orig.copy()

    def _split_data(self):
        self.reduceNieuwbouw(self.pc4codes)

    def reduceNieuwbouw(self, pc4codes, vervallen=True):
        """
        Reduceer de originele set door alleen nieuwbouw-aanvragen over te houden
        pc4codes: list of postcodes (first four numbers) that define the working ares
        vervallen: if False: include orders with 'Object Status' == Vervallen and that do not have a 'Afzegreden',
        if True (default) exclude them from the set
        """
        # Pas filters toe op originele set
        former_cz_area = self.df['object_pc'].str[:4].isin(pc4codes) & self.df['contractor_area'].str.contains('Connect-Z')
        current_hxs_area = self.df['contractor_area'].str.contains('VolkerWessels')
        area = former_cz_area | current_hxs_area
        aanleg = self.df['order_type'].isin(
            ['Aanleg', 'Sloop', 'Verplaatsing'])
        opdracht = self.df['status_request'] != 'Geen opdracht'
        ookopdracht = self.df['status_object'] != 'Geen opdracht'
        residentieel = self.df['cost_characteristic'] == 'Residentieel'

        self.df['active_con'] = self.extract_active_orders(self.df)
        self.df = self.df[aanleg & area & opdracht &
                          ookopdracht & residentieel].reset_index(drop=True)

        niet_afgezegd = self.df['reason_cancelation'].isna()
        nietvervallen = self.df['status_object'] != 'Vervallen'

        if vervallen:
            self.df = self.df[niet_afgezegd & nietvervallen]

    def extract_active_orders(self, df):
        # Vind alle con opdracht ids waarvan alle objecten de status_object
        # Gereed | Vervallen, status_request Gereed en status_payment Afgerekend hebben
        check = df.copy()
        check.at[(check['status_object'] == 'Vervallen') |
                 check['reason_cancelation'].notna(),
                 'status_payment'] = 'Afgerekend'
        check = check.groupby(['con_opdrachtid', 'status_payment'])['con_objectid'].count(
        ).reset_index().rename(columns={'con_objectid': 'afgerekend'})
        check = check.merge(df.groupby(['con_opdrachtid'])['con_objectid'].count().reset_index(),
                            on='con_opdrachtid',
                            how='left')
        check = check.merge(df[['con_opdrachtid', 'status_request']].drop_duplicates(),
                            on='con_opdrachtid',
                            how='left')

        gereed = (check['status_request'] == 'Gereed') & \
            (check['status_payment'] == 'Afgerekend') & \
            (check['con_objectid'] == check['afgerekend'])

        gereed = check[gereed]['con_opdrachtid'].tolist()
        gereed = df['con_opdrachtid'].isin(gereed)

        return np.logical_not(gereed)


class xaris():

    def __init__(self):
        self.sourceTag = 'Xaris'
        self.measures = ['Aanvraagnummer']

        # inlezen van de Excel files vanuit json:
        self.df = pd.DataFrame([])
        for fname in config.files['xaris']:
            df = utils.download_as_dataframe(config.tmp_bucket, fname)
            self.df = self.df.append(df)

        pc = utils.download_as_dataframe(config.tmp_bucket, config.files['connect_pc4'])

        # samenvoegen van de excels
        self.df.drop(['Datum gereed', 'Datum revisie water',
                      'Warmte'], axis='columns')

        # juist_nummer is het aanvraagnummer zonder _ of andere toevoegingen
        self.df['Aanvraagnummer'] = self.df['Aanvraagnummer'].astype(str)
        self.df['juist_nummer'] = self.df['Aanvraagnummer'].str.extract(
            r'(\d{10}|nhl\d{2}\.\d{5}|NHL\d{2}\.\d{5})')

        # als de kolom genaamd 'Kabel' de string 'Ziggo verv' bevat, en er is niet nog een andere Ziggo aanwezig. Dan is dit item vervallen
        self.df['vervallen'] = ((self.df['Kabel'].str.count('Ziggo verv') > 0) & (
            self.df['Kabel'].str.count('Ziggo') <= 1))
        # alleen als alle werkstromen een 'Datum gereed' hebben, dan is de Xaris gereed.
        self.df['gereed'] = self.df['Datum gereed'].notna()

        temp = self.df.groupby('juist_nummer').agg(
            {'Aanvraagnummer': 'count', 'vervallen': 'all', 'gereed': 'all'}
            ).reset_index().rename(columns={'Aanvraagnummer': 'nr_werkstromen'})
        self.df = self.df.drop(['vervallen', 'gereed'], axis='columns').merge(temp, on='juist_nummer', how='left')

        # Status van Xaris is nu Vervallen, Gereed of actief
        self.df.at[self.df['vervallen'], 'Status Xaris'] = 'Vervallen'
        self.df.at[(self.df['gereed'] & self.df['Status Xaris'].isna()), 'Status Xaris'] = 'Gereed'
        self.df.at[self.df['Status Xaris'].isna(), 'Status Xaris'] = 'Actief'
        self.df.drop(['vervallen', 'gereed'], axis='columns', inplace=True)

        # soms is het hoofdleidingprojectnummer slechts
        # bij 1 van de werkstromen ingevuld. Om er zeker van te zijn dat ze bij iedere werkstroom
        # zijn ingevuld (mits er uberhaupt een aanwezig is):
        self.df['Hoofdleidingenprojectnummer'] = self.df.groupby(
            'juist_nummer')['Hoofdleidingenprojectnummer'].fillna(method='ffill')
        self.df['Hoofdleidingenprojectnummer'] = self.df.groupby(
            'juist_nummer')['Hoofdleidingenprojectnummer'].fillna(method='bfill')

        # filter op postcode
        pc4codes = pc[pc['Partner'].str.contains(
            'Connect-Z Utrecht')]['PC4CODE'].astype(str).unique()
        self.df = self.df[self.df['Postcode'].str[:4].isin(pc4codes)]

        # verwijder de werkstromen zodat er slechts 1 'werkstroom' (en dus 1 connect nummer, overblijft)
        mask = self.df['juist_nummer'].duplicated(keep='first')
        self.df = self.df[~mask]


def xaris_status_check(df_xaris, connect):

    df_orig_connect = connect.copy()

    # Als status van het object 'vervallen' of 'geen opdracht' is, dan is de afrekening NVT
    mask = ((df_orig_connect['status_payment'].isna()) & (
        df_orig_connect['status_object'] == 'Vervallen'))
    df_orig_connect.at[mask, 'status_payment'] = 'NVT'
    mask = ((df_orig_connect['status_payment'].isna()) & (
        df_orig_connect['status_object'] == 'Geen opdracht'))
    df_orig_connect.at[mask, 'status_payment'] = 'NVT'

    # Vervallen (troep)
    aanleg = df_orig_connect['order_type'].isin(
        ['Aanleg', 'Sloop', 'Verplaatsing'])
    opdracht = df_orig_connect['status_request'] != 'Geen opdracht'
    ookopdracht = df_orig_connect['status_object'] != 'Geen opdracht'
    residentieel = df_orig_connect['cost_characteristic'] == 'Residentieel'
    vervallen = ((df_orig_connect['reason_cancelation'].notna()) | (
        df_orig_connect['status_object'] == 'Vervallen'))

    df_orig_connect['Vervallen'] = (
        (~(aanleg & opdracht & ookopdracht & residentieel)) | (vervallen))

    temp = df_orig_connect.groupby('con_opdrachtid')[
        'Vervallen'].all()
    df_orig_connect = df_orig_connect.drop('Vervallen', axis='columns').merge(
        temp, on='con_opdrachtid', how='left')

    mask = df_orig_connect['Vervallen']
    list_vervallen_connects = df_orig_connect[mask]['con_opdrachtid'].unique()
    df_orig_connect = df_orig_connect[~mask]

    # Afgerekend
    df_orig_connect['Afgerekend'] = ((df_orig_connect['status_payment'] == 'Afgerekend') | (
        df_orig_connect['status_payment'] == 'NVT'))

    temp = df_orig_connect.groupby(
        'con_opdrachtid')['Afgerekend'].all()
    df_orig_connect = df_orig_connect.drop('Afgerekend', axis='columns').merge(
        temp, on='con_opdrachtid', how='left')

    mask = df_orig_connect['Afgerekend']
    list_afgerekende_connects = df_orig_connect[mask]['con_opdrachtid'].unique()
    df_orig_connect = df_orig_connect[~mask]

    # Overig
    list_overige_connects = df_orig_connect['con_opdrachtid'].unique()

    # kopie van het dataframe zodat we hierachter de foutmeldingen kunnen plakken
    relevante_xaris = df_xaris.copy()

    relevante_xaris['gecanceled'] = relevante_xaris['juist_nummer'].isin(
        list_vervallen_connects)
    relevante_xaris['afgerekend'] = relevante_xaris['juist_nummer'].isin(
        list_afgerekende_connects)
    relevante_xaris['overig'] = relevante_xaris['juist_nummer'].isin(
        list_overige_connects)

    relevante_xaris.at[relevante_xaris['gecanceled'], 'Con_status'] = 'Gecanceled'
    relevante_xaris.at[relevante_xaris['afgerekend'], 'Con_status'] = 'Afgerekend'
    relevante_xaris.at[relevante_xaris['overig'], 'Con_status'] = 'Actief'
    relevante_xaris.at[relevante_xaris['Con_status'].isna(), 'Con_status'] = 'Geen connect'
    relevante_xaris.drop(['gecanceled', 'afgerekend', 'overig'], axis='columns', inplace=True)
    temp = connect.groupby('con_opdrachtid').agg({'execution_type': 'first'}).reset_index()
    temp_2 = relevante_xaris.merge(temp, left_on='juist_nummer', right_on='con_opdrachtid', how='left')
    relevante_xaris = temp_2
    relevante_xaris['execution_type'] = relevante_xaris['execution_type'].fillna('geen')
    relevante_xaris.rename(columns={'execution_type': 'Con_uitvoering'}, inplace=True)

    return relevante_xaris[['juist_nummer', 'Con_status', 'Con_uitvoering', 'Status Xaris', 'Aanvraagdatum']]


class get_lncpcon_data():
    def __init__(self):
        if config.environment != 'p':
            with Connection('w', 'remove test') as session:
                q = "update czImportKeys set `delete` = 1 where sourceKey = 'TEST' and `delete` = 0;"
                session.execute(q)

        self.cp = cp()
        self.connect = connect_vz()
        self.ln = inforln()

        with Connection('w', 'add_cpnr_corrected_if_needed') as session:
            self.connect._update_cpnr_corrected(session)

        with Connection('r', 'read data') as session:
            # Get data ChangePoint
            self.cp._get_data(session)
            print('CP data collected')
            # Get data InforLN:
            self.ln._get_data(session)
            print('LN data collected')
            # Get data Connect:
            self.connect._get_data(session)
            print('Connect data collected')

        self.ln.get_active()
        self.connect._split_data()
        self.cp.splitFrames()


def compute_projectstucture(lncpcon_data=None, check_sets=False):
    if lncpcon_data is None:
        lncpcon_data = get_lncpcon_data()
    ln = lncpcon_data.ln
    cp = lncpcon_data.cp
    connect = lncpcon_data.connect

    # Create sets containing all number for checking at the end
    if check_sets:
        sets = {}
        sets['bpnr'] = (set(expand_column(ln.df.fillna(''), 'bpnr')['bpnr'].tolist()) |
                        set(cp.nb[
                            ~(
                                (cp.nb['cp_fase'].str.startswith('46.')) |
                                (cp.nb['cp_fase'].str.startswith('99.')) |
                                (cp.nb['cp_fase'].str.startswith('301.'))
                            )
                        ]['bpnr'].fillna('').tolist())) - \
            set(['', np.nan])

        sets['ln_id'] = set(ln.df['ln_id']) - set(['', np.nan])

        sets['con_opdrachtid'] = (set(expand_column(ln.df, 'con_opdrachtid')['con_opdrachtid'].tolist()) |
                                  set(connect.df[
                                      (connect.df['con_opdrachtid'].str.startswith('100')) &
                                      (connect.df['active_con'])
                                  ]['con_opdrachtid'])) - \
            set(['', np.nan])

    ###################################################################################################################
    # Voorbereiding DF
    ###################################################################################################################
    # vul alle nan values in ln
    ln.df.replace('', np.nan, inplace=True)
    connect.df.replace('', np.nan, inplace=True)
    connect.df.fillna(value=np.nan, inplace=True)
    cp.df.replace('', np.nan, inplace=True)

    ###################################################################################################################
    # Combinaties binnen LN projectstructuur
    ###################################################################################################################
    # Bepaal alle combinaties voor 34, 35 en 45 nummers
    mask = ((ln.df['ln_id'].str.startswith('34')) |
            (ln.df['ln_id'].str.startswith('35')) |
            (ln.df['ln_id'].str.startswith('40')) |
            (ln.df['ln_id'].str.startswith('45')))
    nummer34 = ln.df[mask][['ln_id', 'search_argument', 'search_argument2']].rename(
        columns={'search_argument': 'con_opdrachtid', 'search_argument2': 'bpnr'})

    mask = ((nummer34['con_opdrachtid'].isna()) & (nummer34['bpnr'].notna()))
    overview = nummer34[mask][['ln_id', 'bpnr']]
    overview = expand_column(overview, 'bpnr')
    overview = overview.merge(
        connect.df[['bpnr', 'con_opdrachtid']], on='bpnr', how='left').drop_duplicates()
    overview['koppeling'] = 'LN > Con (BPNR)'
    overview.at[overview['con_opdrachtid'].isna(), 'koppeling'] = 'LN'

    mask = ((nummer34['con_opdrachtid'].notna()) & (nummer34['bpnr'].isna()))
    temp = nummer34[mask][['ln_id', 'con_opdrachtid']]
    temp = temp.merge(
        connect.df[['bpnr', 'con_opdrachtid']], on='con_opdrachtid', how='left').drop_duplicates()
    temp['koppeling'] = 'LN > Con (opdrachtid)'
    temp.at[temp['bpnr'].isna(), 'koppeling'] = 'LN'
    overview = overview.append(temp, sort=False)

    mask = ((nummer34['con_opdrachtid'].notna()) & (nummer34['bpnr'].notna()))
    temp = nummer34[mask][['ln_id', 'bpnr', 'con_opdrachtid']]
    temp['koppeling'] = 'LN'
    overview = overview.append(temp, sort=False)

    mask = ((nummer34['con_opdrachtid'].isna()) & (nummer34['bpnr'].isna()))
    temp = nummer34[mask][['ln_id']]
    temp['koppeling'] = 'LN'
    overview = overview.append(temp, sort=False)

    # Bepaal alle combinaties in categorie 31_hoofdnet
    mask = ln.df['ln_id'].str.startswith('31')
    temp = ln.df[mask][['ln_id', 'search_argument', 'search_argument2']].rename(
        columns={'search_argument': 'con_opdrachtid', 'search_argument2': 'bpnr'})
    temp['koppeling'] = 'LN'
    overview = overview.append(temp, sort=False)

    # Alles wat overblijft klopt niet omdat het search argument niet klopt
    temp = list(set(ln.df['ln_id']) - set(overview['ln_id']))
    mask = (
        (ln.df['ln_id'].isin(temp)) &
        ((ln.df['ln_id'].str.startswith('34')) |
         (ln.df['ln_id'].str.startswith('35')) |
         (ln.df['ln_id'].str.startswith('45')) |
         (ln.df['ln_id'].str.startswith('40')) |
         (ln.df['ln_id'].str.startswith('31')))
    )
    temp = ln.df[mask][['ln_id', 'search_argument', 'search_argument2']].rename(
        columns={'search_argument': 'con_opdrachtid', 'search_argument2': 'bpnr'}
    )
    temp['koppeling'] = 'LN'
    overview = overview.append(temp, sort=False)
    overview = overview.drop_duplicates(subset=['ln_id', 'bpnr', 'con_opdrachtid'])

    ###################################################################################################################
    # Combinaties binnen Intake
    ###################################################################################################################
    # Intake (vanuit Connect)
    con_totaal = connect.df[['bpnr', 'con_opdrachtid', 'active_con']]
    con_totaal = con_totaal[con_totaal['active_con']].drop_duplicates()

    # de connect intakes met BPNR, check hier of de complete combinatie niet al voorkomt.
    mask = (con_totaal['bpnr'].notna() & con_totaal['con_opdrachtid'].notna())
    temp = con_totaal[mask].drop_duplicates()
    mask2 = (overview['bpnr'].notna() & overview['con_opdrachtid'].notna())
    temp2 = overview[mask2][['bpnr', 'con_opdrachtid']].drop_duplicates()

    temp = temp2.append(temp, sort=False)
    temp['duplicated'] = temp.duplicated(subset=['bpnr', 'con_opdrachtid'], keep=False)
    mask = ((~temp['duplicated']) & temp['active_con'].notna())
    temp = temp[mask][['bpnr', 'con_opdrachtid']]
    temp['koppeling'] = 'Con'
    intake = temp

    # de connect intakes zonder BPNR
    mask = ((con_totaal['bpnr'].isna()) &
            (~con_totaal['con_opdrachtid'].isin(overview['con_opdrachtid'].tolist())) &
            (~con_totaal['con_opdrachtid'].isin(intake['con_opdrachtid'])))
    temp = con_totaal[mask][['bpnr', 'con_opdrachtid']]
    temp.drop_duplicates(inplace=True)
    temp['koppeling'] = 'Con'
    intake = intake.append(temp, sort=False)

    # de CP intakes
    cp_totaal = cp.nb[['cp_fase', 'bpnr']]
    mask = ((cp_totaal['cp_fase'].str.startswith('46.')) |
            (cp_totaal['cp_fase'].str.startswith('99.')) |
            (cp_totaal['cp_fase'].str.startswith('301.')))
    cp_totaal = cp_totaal[~mask]
    mask = ((cp_totaal['bpnr'].notna()) &
            (~cp_totaal['bpnr'].isin(intake['bpnr'].tolist())) &
            (~cp_totaal['bpnr'].isin(overview['bpnr'].tolist())))
    temp = cp_totaal[mask]
    temp['koppeling'] = 'CP'
    temp = temp[['bpnr', 'koppeling']]
    intake = intake.append(temp, sort=False)
    intake.drop_duplicates(inplace=True)

    ###################################################################################################################
    # Intake koppelen met inactieve of non3435 nummers
    ###################################################################################################################
    # NonActive en non3435 df voorbereiden
    # regex voor bpnr en con_opdrachtid
    regex_bpnr = re.compile(r'(20\d{7})|(71\d{7})')
    regex_connect = re.compile(r'(100\d{7}|H\d{8})')

    mask = ln.df_nonActive['search_argument'].fillna('').str.match(regex_connect)
    ln.df_nonActive.at[~mask, 'search_argument'] = np.nan
    mask = ln.df_non3435['search_argument'].fillna('').str.match(regex_connect)
    ln.df_non3435.at[~mask, 'search_argument'] = np.nan

    # PROBEER TE KOPPELEN MET EEN INACTIEF LN PROJECT
    # intake koppelen a.d.v. bpnr
    temp = intake[intake['bpnr'].notna()]
    intake_bpnr = temp.merge(ln.df_nonActive[['bpnr', 'ln_id']].drop_duplicates(), on='bpnr', how='left')
    # intake koppelen a.d.v. connect
    temp = intake[intake['con_opdrachtid'].notna()]
    intake_con = temp.merge(
        ln.df_nonActive[['ln_id', 'con_opdrachtid']].drop_duplicates(),
        on='con_opdrachtid',
        how='left')
    intake = intake_bpnr.append(intake_con, sort=False)

    # PROBEER TE KOPPELEN MET EEN NON 35 nummer
    temp = intake[intake['ln_id'].isna()][['bpnr', 'con_opdrachtid', 'koppeling']]
    # intake koppelen a.d.v. bpnr
    temp_bpnr = temp[temp['bpnr'].notna()]
    intake_bpnr = temp_bpnr.merge(ln.df_non3435[['bpnr', 'ln_id']].drop_duplicates(), on='bpnr', how='left')
    # intake koppelen a.d.v. connect
    temp_con = temp[temp['con_opdrachtid'].notna()]
    intake_con = temp_con.merge(
        ln.df_non3435[['ln_id', 'con_opdrachtid']].drop_duplicates(),
        on='con_opdrachtid',
        how='left')

    # samenvoegen
    intake = intake[intake['ln_id'].notna()]
    intake = intake.append([intake_bpnr, intake_con], sort=False)
    intake.drop_duplicates(inplace=True)

    # samenvoegen met het totaal: overview
    overview = overview.append(intake, sort=False)

    ###################################################################################################################
    # De Combinaties aanvullen met de rest van de gegevens uit de deel systemen
    ###################################################################################################################

    # overview aanvullen met projectinformatie uit LN
    df_ln = ln.df[['ln_id', 'main_project', 'type_hierarchy', 'active_ln']]
    df_ln = df_ln.append(ln.df_non3435[['ln_id', 'main_project', 'type_hierarchy', 'active_ln']], sort=False)
    df_ln = df_ln.append(ln.df_nonActive[['ln_id', 'main_project', 'type_hierarchy', 'active_ln']], sort=False)
    overview = overview.merge(df_ln, on='ln_id', how='left')

    # Voorbereiding Connect
    temp = connect.df.fillna('').groupby(['con_opdrachtid', 'status_request', 'status_object', 'status_payment']).agg(
        {'con_objectid': 'nunique', 'date_request': 'first', 'bpnr': 'first', 'active_con': 'first'}).reset_index()
    temp_2 = connect.df.groupby('con_opdrachtid')['con_objectid'].count()
    temp = temp.merge(temp_2, on='con_opdrachtid', how='left')
    temp['verhouding'] = temp.con_objectid_x / temp.con_objectid_y
    temp.drop(columns=['con_objectid_x'], inplace=True)
    temp.rename(columns={'con_objectid_y': 'totaal'}, inplace=True)

    # afgerekende connect opdrachten maar de aanvraag is nog niet gereed.
    mask = ((temp['verhouding'] == 1) &
            (temp['status_payment'] == 'Afgerekend') &
            (temp['status_request'] != 'Gereed'))
    temp.at[mask, 'afgerekend'] = True
    temp.at[~mask, 'afgerekend'] = False

    # Vervallen opdrachten
    mask = ((temp['verhouding'] == 1) &
            (temp['status_object'] == 'Vervallen'))
    temp.at[mask, 'vervallen'] = True
    temp.at[~mask, 'vervallen'] = False
    df_con = temp[['con_opdrachtid', 'afgerekend', 'vervallen', 'active_con']]
    df_con.drop_duplicates(inplace=True)

    # Overview aanvullen met Connect
    overview = overview.merge(df_con, on='con_opdrachtid', how='left')

    # # Overview aanvullen met CP
    df_cp = cp.nb[['bpnr', 'cp_fase']]
    df_cp['dup'] = df_cp['bpnr'].duplicated(keep=False)
    df_cp['start_fase'] = df_cp['cp_fase'].str.startswith('10')
    # verwijder de CP aansluitingen die dubbel in de db zitten waarvan er een afgesloten is en de ander open
    mask = df_cp['dup'] & ~df_cp['start_fase']
    df_cp = df_cp[~mask][['bpnr', 'cp_fase']]
    df_cp.drop_duplicates(inplace=True)
    overview = overview.merge(df_cp, on='bpnr', how='left')

    # Duplicates eruit halen (duplicate combinations)
    overview = overview.drop_duplicates(subset=['ln_id', 'bpnr', 'con_opdrachtid'], keep='first')

    overview['dup_ln_bpnr'] = overview.duplicated(subset=['ln_id', 'bpnr'], keep=False)
    mask = ((overview['dup_ln_bpnr']) & (overview['con_opdrachtid'].isna()))
    overview = overview[~mask]

    overview['dup_ln_con'] = overview.duplicated(subset=['ln_id', 'con_opdrachtid'], keep=False)
    mask = ((overview['dup_ln_con']) & (overview['bpnr'].isna()))
    overview = overview[~mask]

    overview['dup_bpnr_con'] = overview.duplicated(subset=['bpnr', 'con_opdrachtid'], keep=False)
    mask = ((overview['dup_bpnr_con']) & (overview['ln_id'].isna()))
    overview = overview[~mask]

    overview['d_bpnr'] = overview['bpnr'].duplicated(keep=False)
    overview['d_ln'] = overview['ln_id'].duplicated(keep=False)
    overview['d_con'] = overview['con_opdrachtid'].duplicated(keep=False)

    mask = ((overview['d_bpnr'] & overview['ln_id'].isna() & overview['con_opdrachtid'].isna()))
    overview = overview[~mask]
    mask = ((overview['d_ln'] & overview['bpnr'].isna() & overview['con_opdrachtid'].isna()))
    overview = overview[~mask]
    mask = ((overview['d_con'] & overview['ln_id'].isna() & overview['bpnr'].isna()))
    overview = overview[~mask]
    overview = overview.drop(columns=['dup_ln_bpnr', 'dup_ln_con', 'dup_bpnr_con', 'd_bpnr', 'd_ln', 'd_con'])

    ###################################################################################################################
    # Categorisering van de combinaties
    ###################################################################################################################
    # 31_hoofdnet
    mask = overview['ln_id'].fillna('').str.startswith('31')
    overview.at[mask, 'categorie'] = '31_hoofdnet'

    # 35_enkelvoudig
    mask = (((overview['ln_id'].fillna('').str.startswith('35')) |
            (overview['ln_id'].fillna('').str.startswith('45'))) &
            (overview['type_hierarchy'] == 'Enkelvoudig project'))
    overview.at[mask, 'categorie'] = '35_enkelvoudig'

    # 35_deel
    mask = ((overview['ln_id'].fillna('').str.startswith('35')) &
            (overview['type_hierarchy'] == 'Deelproject'))
    overview.at[mask, 'categorie'] = '35_deel'

    # 34_vooraanleg
    mask = ((overview['ln_id'].fillna('').str.startswith('34')) &
            (overview['con_opdrachtid'].isna()))
    overview.at[mask, 'categorie'] = '34_vooraanleg'

    # 34_nieuwbouw
    mask = ((overview['ln_id'].fillna('').str.startswith('34')) &
            (overview['con_opdrachtid'].notna()))
    overview.at[mask, 'categorie'] = '34_nieuwbouw'

    # VZ_ontbreekt
    mask = ((overview['ln_id'].notna()) &
            (~overview['bpnr'].fillna('').str.match(regex_bpnr)) &
            (~overview['con_opdrachtid'].fillna('').str.match(regex_connect)))
    overview.at[mask, 'categorie'] = 'VZ_ontbreekt'

    # 31_intake
    mask = ((overview['ln_id'].isna()) &
            (overview['con_opdrachtid'].isna()) &
            (overview['bpnr'].notna()))
    overview.at[mask, 'categorie'] = '31_intake'

    # 31_35_intake
    mask = ((overview['ln_id'].isna()) &
            (overview['con_opdrachtid'].notna()) &
            (overview['bpnr'].notna()))
    overview.at[mask, 'categorie'] = '31_35_intake'

    # 35_intake
    mask = ((overview['ln_id'].isna()) &
            (overview['con_opdrachtid'].notna()) &
            (overview['bpnr'].isna()))
    overview.at[mask, 'categorie'] = '35_intake'

    # 40_expenses
    mask = ((overview['ln_id'].fillna('').str.startswith('40')))
    overview.at[mask, 'categorie'] = '40_expenses'

    # de projecten die niet met een juist nummer beginnen worden onder 35_enkelvoudig ingedeeld
    mask = overview['categorie'].isna()
    overview.at[mask, 'categorie'] = '35_enkelvoudig'

    ###################################################################################################################
    # FOUTMELDINGEN
    ###################################################################################################################

    # Deze Connect opdracht valt over meerdere bouwplannummers
    temp = overview.groupby('con_opdrachtid').agg({'bpnr': 'nunique'})
    temp = list(temp[temp['bpnr'] > 1].index.unique())
    mask = overview['con_opdrachtid'].isin(temp)
    overview.at[mask, 'F01'] = 'F01_Deze Connect opdracht valt over meerdere bouwplannummers'

    # Dit connectid bevindt zich in meerdere LN projecten
    temp = overview.groupby('con_opdrachtid').agg({'ln_id': 'nunique'})
    temp = list(temp[temp['ln_id'] > 1].index.unique())
    mask = overview['con_opdrachtid'].isin(temp)
    overview.at[mask, 'F02'] = 'F02_Dit connectid bevindt zich in meerdere LN projecten'

    # Alle Connect objecten zijn afgerekend maar de Connect aanvraag is niet gereed
    mask = (overview['afgerekend'].fillna(False))
    overview.at[mask, 'C03'] = 'C03_Alle Connect objecten zijn afgerekend maar de Connect aanvraag is niet gereed'

    # in Connect is deze order gekoppeld aan een ChangePoint, maar staat geregisteerd als 35-nummer
    mask = ((overview['ln_id'].fillna('').str.startswith('35')) &
            (overview['bpnr'].notna()) &
            (overview['con_opdrachtid'].notna()) &
            (overview['type_hierarchy'] == 'Enkelvoudig project'))
    overview.at[mask, 'F04'] = \
        'F04_In Connect is deze order gekoppeld aan een ChangePoint, maar staat geregisteerd als een enkelvoudig project'

    # LNnr is niet actief
    mask = ((~overview['active_ln'].fillna(True)) | (overview['ln_id'].isin(list(ln.df_nonActive['ln_id'].unique()))))
    overview.at[mask, 'F05'] = 'F05_LNnr is niet actief'

    # CP nummer is niet als nieuwbouwproject geregistreerd in CP
    cp_nb_nummers = list(cp.nb['bpnr'].unique())
    mask = ((overview['bpnr'].notna()) & (~overview['bpnr'].isin(cp_nb_nummers)) & (overview['bpnr'] != ''))
    overview.at[mask, 'F06'] = 'F06_CP nummer is niet als nieuwbouwproject geregistreerd in CP'

    # Connect opdracht is niet als actief aanleg project geregistreerd in Connect
    connect_nummers = list(connect.df['con_opdrachtid'].unique())
    mask = ((overview['con_opdrachtid'].notna()) &
            (~overview['con_opdrachtid'].isin(connect_nummers)) &
            (overview['con_opdrachtid'] != ''))
    overview.at[mask, 'C07'] = 'C07_Connect opdracht is niet als actief aanleg project geregistreerd in Connect'

    # Connect order is niet meer actief
    mask = (~(overview['active_con'].fillna(True)))
    overview.at[mask, 'C08'] = 'C08_Connect opdracht is niet actief'

    # Dit CPnummer bevindt zich in meerdere LN projecten (alleen voor 31 nummers of 34 nummers)
    mask = ((overview['ln_id'].fillna('').str.startswith('31')) | overview['ln_id'].fillna('').str.startswith('34'))
    temp = overview[mask]
    temp = temp.groupby('bpnr').agg({'ln_id': 'nunique'})
    temp = list(temp[temp['ln_id'] > 1].index.unique())
    mask = overview['bpnr'].isin(temp)
    overview.at[mask, 'F09'] = 'F09_Dit CPnummer bevindt zich in meerdere LN (hoofd)projecten'

    # Dit CP nummer is afgesloten of gecanceld
    afgesloten = cp.df[cp.df['cp_fase'].str.lower(
    ).str.contains('project completed')]
    overview.at[overview['bpnr'].isin(afgesloten['bpnr'].tolist()) &
                overview['bpnr'].notna(), 'C10'] = "C10_Dit CPnummer is afgesloten"

    # Dit LN nummer is geen 31, 35, 45 of 34 nummer
    mask = (~(overview['ln_id'].fillna('').str.startswith('34') |
            overview['ln_id'].fillna('').str.startswith('31') |
            overview['ln_id'].fillna('').str.startswith('35') |
            overview['ln_id'].fillna('').str.startswith('45')) &
            overview['ln_id'].notna())
    overview.at[mask, 'C11'] = 'C11_LNnr is geen 31-, 34-, 35- of 45-nummer'

    # LN: hoofd/deelproject zonder zoekreferentie II (bouwplan)
    mask = (((ln.df['type_hierarchy'] == 'Hoofdproject') | (ln.df['type_hierarchy'] == 'Deelproject')) &
            (ln.df['search_argument2'].isna()) | (ln.df['search_argument2'] == ''))
    missing_searchargument = list(ln.df[mask]['ln_id'].unique())
    mask = overview['ln_id'].isin(missing_searchargument)
    overview.at[mask, 'F12'] = 'F12_LN: hoofd/deelproject zonder zoekreferentie II (bouwplan)'

    # LN: enkel/deelproject zonder zoekreferentie I (connect)
    mask = (((ln.df['type_hierarchy'] == 'Enkelvoudig project') | (ln.df['type_hierarchy'] == 'Deelproject')) &
            (ln.df['search_argument'].isna()) | (ln.df['search_argument'] == ''))
    missing_searchargument = list(ln.df[mask]['ln_id'].unique())
    mask = overview['ln_id'].isin(missing_searchargument)
    overview.at[mask, 'F13'] = 'F13_LN: enkel/deelproject zonder zoekreferentie I (connect)'

    # LN: enkelvoudig project met zoekreferentie II (bouwplan)
    mask = ((ln.df['type_hierarchy'] == 'Enkelvoudig project') &
            ((ln.df['search_argument2'].notna()) & (ln.df['search_argument2'] != '')))
    missing_searchargument = list(ln.df[mask]['ln_id'].unique())
    mask = overview['ln_id'].isin(missing_searchargument)
    overview.at[mask, 'F14'] = 'F14_LN: enkelvoudig project met zoekreferentie II (bpnr)'

    # Hoofdproject en deelproject hebben een ander bouwplannummer
    mask = overview['ln_id'].fillna('').str.startswith('31')
    temp = overview[mask][['ln_id', 'bpnr']].rename(columns={'ln_id': 'ln_id_hoofdproject', 'bpnr': 'bpnr_hoofdproject'})
    temp = overview[overview['main_project'].notna()].merge(temp, left_on='main_project', right_on='ln_id_hoofdproject', how='left')
    mask = temp['bpnr'] != temp['bpnr_hoofdproject']
    temp = list(temp[mask]['ln_id'].unique())
    mask = overview['ln_id'].isin(temp)
    overview.at[mask, 'F15'] = 'F15_Hoofdproject en deelproject hebben een ander bouwplannummer'
    # zelfde foutmelding toevoegen aan de 31 projecten waar dit voor geld.
    mask = overview['F15'].notna()
    nummbers31 = overview[mask]['main_project'].unique()
    mask = (overview['ln_id'].notna() & overview['ln_id'].isin(nummbers31))
    overview.at[mask, 'F15'] = 'F15_Hoofdproject en deelproject hebben een ander bouwplannummer'

    # Connect opdracht bestaat niet in Connect
    connect_nummers = list(connect.df_orig['con_opdrachtid'].unique())
    mask = ((overview['con_opdrachtid'].notna()) &
            (~overview['con_opdrachtid'].isin(connect_nummers)) &
            (overview['con_opdrachtid'] != ''))
    overview.at[mask, 'F16'] = 'F16_Connect opdracht bestaat niet in Connect'
    # op de plek waar de connect niet bestaat, is deze ook geen nieuwbouw project. Dus op die plaatsen weghalen
    mask = overview['F16'].notna()
    overview.at[mask, 'C07'] = np.nan

    # Dit CP nummer bestaat niet in CP
    cp_nummers = list(cp.df['bpnr'].unique())
    mask = ((overview['bpnr'].notna()) & (~overview['bpnr'].isin(cp_nummers)) & (overview['bpnr'] != ''))
    overview.at[mask, 'F17'] = 'F17_CP nummer bestaat niet in CP'
    # op de plek waar de bpnr niet bestaat, is deze ook geen nieuwbou project. Dus op die plaatsen weghalen
    mask = overview['F17'].notna()
    overview.at[mask, 'F06'] = np.nan

    # Dit CP nummer is afgesloten of gecanceld
    cancel = cp.df[cp.df['cp_fase'].str.lower().str.contains('cancel')]
    overview.at[overview['bpnr'].isin(cancel['bpnr'].tolist()) &
                overview['bpnr'].notna(), 'C18'] = "C18_Dit CPnummer is gecanceled"

    ###################################################################################################################
    # Verwijder afgesloten projecten
    ###################################################################################################################
    # LN, CP en CON zijn afgesloten
    mask = (overview['bpnr'].notna() & overview['con_opdrachtid'].notna() & overview['ln_id'].notna() &
            (overview['C10'].notna() | overview['C18'].notna()) & overview['C08'].notna() & overview['F05'].notna())
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # Geen LN, maar CP en CON zijn afgesloten
    mask = (overview['bpnr'].notna() & overview['con_opdrachtid'].notna() & overview['ln_id'].isna() &
            (overview['C10'].notna() | overview['C18'].notna()) & overview['C08'].notna())
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # Geen CP, maar LN en CON zijn afgesloten
    mask = (overview['bpnr'].isna() & overview['con_opdrachtid'].notna() & overview['ln_id'].notna() &
            overview['C08'].notna() & overview['F05'].notna())
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # Geen CON, maar LN en CP zijn afgesloten
    mask = (overview['bpnr'].notna() & overview['con_opdrachtid'].isna() & overview['ln_id'].notna() &
            (overview['C10'].notna() | overview['C18'].notna()) & overview['F05'].notna())
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # Alleen CP maar afgesloten
    mask = (overview['bpnr'].notna() & overview['con_opdrachtid'].isna() & overview['ln_id'].isna() &
            (overview['C10'].notna() | overview['C18'].notna()))
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # Alleen Connect maar afgesloten
    mask = (overview['bpnr'].isna() & overview['con_opdrachtid'].notna() & overview['ln_id'].isna() &
            overview['C08'].notna())
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # Alleen LN maar afgesloten
    mask = (overview['bpnr'].isna() & overview['con_opdrachtid'].isna() & overview['ln_id'].notna() &
            overview['F05'].notna())
    overview.at[mask, 'afgesloten'] = 'afgesloten'

    # Als de combinatie in VZ_ontbreekt beland, en het zoekargument (Connect) bevat 'ALG', 'Alg', 'alg', dan afgesloten
    mask = ((overview['categorie'] == 'VZ_ontbreekt') &
            ((overview['con_opdrachtid'].fillna('').str.contains('Alg')) |
             (overview['con_opdrachtid'].fillna('').str.contains('ALG')) |
             (overview['con_opdrachtid'].fillna('').str.contains('alg'))))
    overview.at[mask, 'afgesloten'] = 'afgesloten'

    # Connect opdrachten die over meerdere bouwplannen of LN vallen, maar waarvan er een LN project al is afgerond en
    # er een nieuwe is opgestart. Dit LN project mag eruitgehaald worden
    # Voor dubble bouwplan en LN project
    mask = ((overview['F01'].notna()) & (overview['F02'].notna()))
    temp = overview[mask]
    temp = temp.groupby('con_opdrachtid').agg({'ln_id': 'count', 'active_ln': lambda x: sum(x)})
    temp = temp[temp['active_ln'] != temp['ln_id']]
    temp = list(temp.index)
    # verwijder afgesloten projecten
    mask = ((overview['con_opdrachtid'].isin(temp)) & (~overview['active_ln'].fillna(True)))
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # verwijder de foutmelding bij het andere project
    mask = ((overview['con_opdrachtid'].isin(temp)) & (overview['active_ln'].fillna(True)))
    overview.at[mask, 'F01'] = np.nan
    overview.at[mask, 'F02'] = np.nan

    # Voor dubbele bouwplannummers
    temp = overview[overview['F01'].notna()]
    temp = temp.groupby('con_opdrachtid').agg({'ln_id': 'count', 'active_ln': lambda x: sum(x)})
    temp = temp[temp['active_ln'] != temp['ln_id']]
    temp = list(temp.index)
    # verwijder afgesloten projecten
    mask = ((overview['con_opdrachtid'].isin(temp)) & (~overview['active_ln'].fillna(True)))
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # verwijder de foutmelding bij het andere project
    mask = ((overview['con_opdrachtid'].isin(temp)) & (overview['active_ln'].fillna(True)))
    overview.at[mask, 'F01'] = np.nan

    # Voor dubbele bouwplannummers
    temp = overview[overview['F02'].notna()]
    temp = temp.groupby('con_opdrachtid').agg({'ln_id': 'count', 'active_ln': lambda x: sum(x)})
    temp = temp[temp['active_ln'] != temp['ln_id']]
    temp = list(temp.index)
    # verwijder afgesloten projecten
    mask = ((overview['con_opdrachtid'].isin(temp)) & (~overview['active_ln'].fillna(True)))
    overview.at[mask, 'afgesloten'] = 'afgesloten'
    # verwijder de foutmelding bij het andere project
    mask = ((overview['con_opdrachtid'].isin(temp)) & (overview['active_ln'].fillna(True)))
    overview.at[mask, 'F02'] = np.nan

    ###################################################################################################################
    # verwijder afgesloten projecten
    mask = overview['afgesloten'].isna()
    overview.at[mask, 'afgesloten'] = ''
    overview = overview[overview['afgesloten'] == '']

    ###################################################################################################################
    # Verwijder foutmeldingen die bij een bepaalde categorie niet voor hoeven komen.
    ###################################################################################################################

    # 'F01' = 'F01_Deze Connect opdracht valt over meerdere bouwplannummers'
    # 'F02' = 'F02_Dit connectid bevindt zich in meerdere LN projecten'
    # 'C03' = 'C03_Alle Connect objecten zijn afgerekend maar de Connect aanvraag is niet gereed'
    # 'F04' = 'F04_In Connect is deze order gekoppeld aan een ChangePoint, maar staat geregisteerd als een enkelvoudig project'
    # 'F05' = 'F05_LNnr is niet actief'
    # 'F06' = 'F06_CP nummer is niet als nieuwbouwproject geregistreerd in CP'
    # 'C07' = 'C07_Connect opdracht is niet als actief aanleg project geregistreerd in Connect'
    # 'C08' = 'C08_Connect opdracht is niet actief'
    # 'F09' = 'F09_Dit CPnummer bevindt zich in meerdere LN (hoofd)projecten'
    # 'C10' = 'C10_Dit CPnummer is afgesloten'
    # 'C11' = 'C11_LNnr is geen 31-, 34-, 35- of 45-nummer'
    # 'F12' = 'F12_LN: hoofd/deelproject zonder zoekreferentie II (bouwplan)'
    # 'F13' = 'F13_LN: enkel/deelproject zonder zoekreferentie I (connect)'
    # 'F14' = 'F14_LN: enkelvoudig project met zoekreferentie II (bpnr)'
    # 'F15' = 'F15_Hoofdproject en deelproject hebben een ander bouwplannummer'
    # 'F16' = 'F16_Connect opdracht bestaat niet in Connect'
    # 'F17' = 'F17_CP nummer bestaat niet in CP'
    # 'C18' = 'C18_Dit CPnummer is gecanceled'

    # FOUTEN
    mapping_fouten_categorie = {
        '34_nieuwbouw': ['F01', 'F02', 'C03', 'F05', 'F06', 'C08', 'F09', 'C10', 'F17', 'C18'],
        '34_vooraanleg': ['F05', 'F06', 'F09', 'C10', 'F17', 'C18'],
        '31_hoofdnet': ['F05', 'F06', 'F09', 'C10', 'F12', 'F15', 'F17', 'C18'],
        '35_deel': ['F01', 'F02', 'C03', 'F05', 'F06', 'C07', 'C08',
                    'C10', 'C11', 'F12', 'F13',  'F15', 'F16', 'F17', 'C18'],
        '35_enkelvoudig': ['F01', 'F02', 'C03', 'F04', 'F05', 'C07',
                           'C08', 'C11', 'F13', 'F14', 'F16'],
        '31_35_intake': ['F01', 'F02', 'C03', 'F06', 'C08', 'C10', 'F17', 'C18'],
        '35_intake': ['F01', 'C03', 'C08'],
        '31_intake': [],
        'VZ_ontbreekt': [],
        '40_expenses': []
    }

    foutmeldingen = pd.DataFrame([])
    for cat in mapping_fouten_categorie.keys():
        temp = overview[overview['categorie'] == cat]
        temp = merge_columns_text(temp, mapping_fouten_categorie[cat], 'Projectstructuur constateringen')
        foutmeldingen = foutmeldingen.append(temp, sort=False)
    overview = foutmeldingen
    overview.at[overview['Projectstructuur constateringen'] == '', 'Projectstructuur constateringen'] = np.nan

    ###################################################################################################################
    # Merge foutmeldingen en bereid het df voor
    ###################################################################################################################
    overview = overview[['ln_id', 'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen', 'koppeling']]
    mask = overview['Projectstructuur constateringen'].fillna('').str.startswith(';')
    overview.at[mask, 'Projectstructuur constateringen'] = overview[mask]['Projectstructuur constateringen'].str[2:]

    # intake aanvullen met aanvraagdatum cp en connect
    intake = overview[overview['categorie'].str.contains('intake')][[
        'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen']]
    intake = intake.merge(connect.df_orig[['con_opdrachtid', 'date_request']].drop_duplicates(
        subset='con_opdrachtid'), on='con_opdrachtid', how='left')
    intake = intake.merge(cp.df[['bpnr', 'date_created']].drop_duplicates(
        subset='bpnr'), on='bpnr', how='left')
    intake = intake.rename(columns={
        'date_request': 'Aanvraagdatum Connect',
        'date_created': 'Aanvraagdatum CP',
    })
    intake['Aanvraagdatum Connect'] = intake['Aanvraagdatum Connect'].fillna(
        '').astype(str).str.split(' ', expand=True)[0]
    intake['Aanvraagdatum CP'] = intake['Aanvraagdatum CP'].fillna(
        '').astype(str).str.split(' ', expand=True)[0]

    # %% Save list
    to_export = ['ln_id', 'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen', 'koppeling']

    if check_sets:
        print('LN: too many in list; set(list) - set(ln) = {}\nLN: missing in set; set(ln) - set(list) = {}'.format(
            set(overview['ln_id'].tolist()) -
            sets['ln_id'], sets['ln_id'] - set(overview['ln_id'].tolist())
        ))

        print('CP: too many in list; set(list) - set(cp) = {}\nCP: missing in set; set(cp) - set(list) = {}'.format(
            set(overview['bpnr'].tolist()) -
            sets['bpnr'], sets['bpnr'] - set(overview['bpnr'].tolist())
        ))

        print('Connect: too many in list; set(list) - set(con) = {}\nConnect: missing in set; set(con) - set(list) = {}'.format(
            set(overview['con_opdrachtid'].tolist(
            )) - sets['con_opdrachtid'], sets['con_opdrachtid'] - set(overview['con_opdrachtid'].tolist())
        ))

    return overview[to_export], intake
