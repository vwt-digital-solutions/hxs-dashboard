import os
import utils
import config
import numpy as np
import pandas as pd
import sqlalchemy as sa

from analysis.connectz import expand_column
from datetime import datetime as dt
from db.queries import read
from connection import Connection
from db.models import czHierarchy, czImportKeys, czSubscriptions, czLog


def merge_columns_text(df, columnnames, columnname_new=None, connector='; '):
    if columnname_new is None:
        columnname_new = columnnames[0]

    for i in range(len(columnnames)-1):
        place = (
            (df[columnnames[i]].notna()) & (df[columnnames[i]] != '') &
            (df[columnnames[i+1]].notna()) & (df[columnnames[i+1]] != '')
        )
        df.at[place, columnname_new] = df[place][columnnames[i]] + \
            connector + df[place][columnnames[i+1]]
        df.at[~place, columnname_new] = df[~place][columnnames[i]].fillna(
            '') + df[~place][columnnames[i+1]].fillna('')
        df.at[(df[columnname_new] == ''), columnname_new] = np.nan
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

    def reduceConnectZ(self):
        self.nb = self.nb[self.nb['contractor'] == 'Connect-Z Utrecht']

    def splitFrames(self):
        cp = self.df.copy()
        self.nb = cp[cp['project_class'].str.lower().str.contains('nieuwbouw')]
        self.reduceConnectZ()
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
            'project_description'
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
        self.df_proj['active_ln'] = self.df_proj['ln_project_status'].str.lower().isin([
            'actief', 'gereed', 'vrij'])
        actief = self.df_proj['active_ln']
        # LN actieve nieuwbouw projecten (34 en 35 nummers)
        self.df = self.df_proj[(nummer34 | nummer35 |
                                nummer45 | nummer40) & actief]
        # Maak set met overige codes (niet 34 of 35)
        self.df_non3435 = self.df_proj[~(
            nummer34 | nummer35 | nummer45 | nummer40) & actief]
        # Maak set met inactieve LN nieuwbouw projecten
        self.df_nonActive = self.df_proj.loc[(
            nummer34 | nummer35 | nummer45 | nummer40) & ~actief]

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
            'status_order',
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
        # Vind alle con opdracht ids waarvan alle objecten de status_order Gereed, status_object Gereed | Vervallen, status_request Gereed en status_payment Afgerekend hebben
        check = df.copy()
        check.at[(check['status_object'] == 'Vervallen') |
                 check['reason_cancelation'].notna(),
                 'status_payment'] = 'Afgerekend'
        check = check.groupby(['con_opdrachtid', 'status_payment'])['con_objectid'].count(
        ).reset_index().rename(columns={'con_objectid': 'afgerekend'})
        check = check.merge(df.groupby(['con_opdrachtid'])['con_objectid'].count().reset_index(),
                            on='con_opdrachtid',
                            how='left')
        check = check.merge(df[['con_opdrachtid', 'status_order', 'status_request']].drop_duplicates(),
                            on='con_opdrachtid',
                            how='left')

        gereed = (check['status_request'] == 'Gereed') & \
            (check['status_payment'] == 'Afgerekend') & \
            (check['con_objectid'] == check['afgerekend']) & \
            (check['status_order'] == 'Gereed')

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

        # soms is het hoofdleidingprojectnummer slechts bij 1 van de werkstromen ingevuld. Om er zeker van te zijn dat ze bij iedere werkstroom
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
    # %% Bepaal alles in categorie 34_nieuwbouw en in de categorie 34_vooraanleg
    overview = ln.df[(ln.df['bpnr'] != '') &
                     (ln.df['con_opdrachtid'] == '')][['ln_id', 'bpnr']]
    overview = expand_column(overview, 'bpnr')
    overview.at[overview.duplicated(subset='ln_id', keep=False),
                'let_op'] = "foute structuur: LN heeft referentie naar twee ChangePointorders"
    overview = overview.merge(
        connect.df[['bpnr', 'con_opdrachtid']], on='bpnr', how='left').drop_duplicates()
    overview.at[:, 'categorie'] = "34_nieuwbouw"
    overview.at[overview['con_opdrachtid'].isna(),
                'categorie'] = "34_vooraanleg"
    overview['koppeling'] = "LN > Connect (BPNR)"

    # %% Bepaal alles in categorie 35_aanleg
    temp = ln.df[(ln.df['bpnr'] == '') &
                 (ln.df['con_opdrachtid'] != '')][['ln_id', 'con_opdrachtid']].drop_duplicates()
    temp = expand_column(temp, 'con_opdrachtid')
    temp.at[temp.duplicated(subset='ln_id', keep=False),
            'let_op2'] = "foute structuur: LN heeft referentie naar twee ChangePointorders"
    temp = temp.merge(connect.df[['con_opdrachtid', 'bpnr']],
                      on='con_opdrachtid', how='left').drop_duplicates()
    temp.at[:, 'categorie'] = "35_aanleg"
    temp.at[temp['bpnr'].notna(
    ), 'let_op'] = "in Connect is deze order gekoppeld aan een ChangePoint, maar staat geregisteerd als 35-nummer"
    temp['koppeling'] = "LN > Connect (ConnID)"
    # koppeling komt uit Connect, niet uit LN
    overview = overview.append(temp, sort=False)

    # %% overige LN opdrachten: VZ_ontbreekt
    temp = ln.df[(ln.df['con_opdrachtid'] == '') &
                 (ln.df['bpnr'] == '')]['ln_id']
    temp = pd.DataFrame(temp)
    if len(temp) > 0:
        temp.at[:, 'categorie'] = "VZ_ontbreekt"
        temp['koppeling'] = "LN"
        overview = overview.append(temp, sort=False)

    # %% intake 34 nieuwbouw
    temp = cp.nb[~(cp.nb['bpnr'].isin(expand_column(ln.df, 'bpnr')['bpnr'].tolist())) &  # bpnr not in ln
                 # bpnr in Connect
                 (cp.nb['bpnr'].isin(connect.df['bpnr'].tolist())) &
                 # bpnr komt niet al voor in de structuur
                 (~cp.nb['bpnr'].isin(overview['bpnr'])) &
                 (cp.nb['bpnr'].notna())
                 ][['bpnr', 'cp_fase']].drop_duplicates()
    temp = temp.merge(connect.df, how='left', on='bpnr').drop_duplicates()
    int34 = temp.\
        fillna('').\
        groupby(['bpnr', 'con_opdrachtid', 'status_request', 'status_object', 'status_payment']).\
        agg({
            'con_objectid': 'nunique',
            'cp_fase': 'first',
        }).\
        reset_index()
    if len(int34) > 0:
        int34 = int34.merge(temp.groupby('con_opdrachtid')['con_objectid'].nunique().reset_index(
        ).rename(columns={'con_objectid': 'Totaal'}), on='con_opdrachtid', how='left')
        int34['verhouding'] = int34['con_objectid'] / int34['Totaal']
        temp = int34.copy()

        temp['categorie'] = "34_nieuwbouw_intake"
        temp['koppeling'] = "CP > Connect (BPNR)"

        # Afgerekende objecten
        temp.at[
            (temp['cp_fase'].str.startswith('104')) &
            (temp['verhouding'] == 1) &
            (temp['status_payment'] == 'Afgerekend'),
            'status'] = 'alle Connect objecten zijn afgerekend, maar bijbehorend bouwplannummer is nog niet aangenomen'

        # Vervallen objecten
        temp['status'] = ''
        temp.at[
            (temp['verhouding'] == 1) &
            (temp['status_object'] == 'Vervallen'), 'status'] = 'alle Connect objecten zijn vervallen'

        # Te koppelen met inactieve nieuwbouw LN
        temp_2 = temp.merge(
            ln.df_nonActive[['bpnr', 'ln_id', 'ln_project_status']], how='inner', on='bpnr')
        temp_2['koppeling'] = "CP > Connect > LN (BPNR, BPNR)"
        temp_2['let_op'] = ''
        temp_2.at[~(temp_2['ln_project_status'] == 'Actief'),
                  'let_op'] = 'LNnr is niet actief'
        temp_2.at[~(
            temp_2['ln_id'].str.startswith('34') |
            temp_2['ln_id'].str.startswith('35') |
            temp_2['ln_id'].str.startswith('45')), 'let_op2'] = 'LNnr is geen 34-, 35- of 45-nummer'

        # Wijs LN nummer toe obv Conn ID
        temp_3 = temp.merge(ln.df_nonActive[[
                            'con_opdrachtid', 'ln_id', 'ln_project_status']], how='inner', on='con_opdrachtid')
        temp_3.at[~(temp_3['ln_project_status'] == 'Actief'),
                  'let_op'] = 'LNnr is niet actief'
        temp_3['koppeling'] = "CP > Connect > LN (BPNR, ConnID)"
        temp_3.at[~(
            temp_3['ln_id'].str.startswith('34') |
            temp_3['ln_id'].str.startswith('35') |
            temp_3['ln_id'].str.startswith('45')), 'let_op2'] = 'LNnr is geen 34-, 35- of 45-nummer'

        # Er zijn cases die zowel een BPNR als Conn ID hebben wat gevonden kan worden in de set met inactieve LN nummers: hiervan moeten we alleen de eerste behouden
        # bepaal de duplicated records en vervang op die plekken de koppeling kolom
        temp_4 = pd.concat([temp_2, temp_3], sort=False)
        temp_4.at[temp_4[['bpnr', 'ln_id', 'con_opdrachtid']].duplicated(
        ), 'koppeling'] = "CP > Connect > LN (BPNR, BPNR en ConnID)"
        temp_4.drop_duplicates(
            subset=['bpnr', 'con_opdrachtid', 'ln_id'], inplace=True)
        temp_4['categorie'] = '34_nieuwbouw'

        # Filter cases die in temp zitten, maar ook in temp_2 of in temp_3 (i.e. cases hebben geen LN nummer maar kunnen dat vinden via ofwel BPNR ofwel Connect)
        temp = temp[~(temp['con_opdrachtid'].isin(temp_3['con_opdrachtid'].tolist()) | temp['bpnr'].isin(temp_2['bpnr'].tolist()))][[
            'bpnr', 'con_opdrachtid', 'categorie', 'koppeling']].drop_duplicates()

        overview = overview.append([temp, temp_4], sort=False)

    # %% intake 34 vooraanleg
    temp = cp.nb[~(cp.nb['bpnr'].isin(overview['bpnr'])) &
                 (cp.nb['bpnr'].notna()) &
                 ~(
        (cp.nb['cp_fase'].str.startswith('46.')) |
        (cp.nb['cp_fase'].str.startswith('99.')) |
        (cp.nb['cp_fase'].str.startswith('301.'))
    )
    ]
    if len(temp) > 0:
        temp.at[:, 'categorie'] = "34_vooraanleg_intake"
        temp.at[~temp['cp_fase'].str.startswith(
            '100'), 'let_op'] = "Dit nieuwbouwproject is al bezig volgens CP, maar is niet aan een juist LNnummer gekoppeld."
        # "Capaciteit in ChangePoint ontbreekt"
        temp.at[temp['cp_capacity'] == 0, 'status'] = ""

        # Te koppelen met inactieve nieuwbouw LN
        temp_2 = temp.merge(
            ln.df_nonActive[['bpnr', 'ln_id', 'ln_project_status']], how='inner', on='bpnr')
        temp_2['koppeling'] = "CP > LN (ConnID)"
        temp_2['let_op'] = ''
        temp_2.at[~(temp_2['ln_project_status'] == 'Actief'),
                  'let_op'] = 'LNnr is niet actief'
        temp_2.at[~(
            temp_2['ln_id'].str.startswith('34') |
            temp_2['ln_id'].str.startswith('35') |
            temp_2['ln_id'].str.startswith('45')), 'let_op2'] = 'LNnr is geen 34-, 35- of 45-nummer'
        temp = temp[~temp['bpnr'].isin(temp_2['bpnr'].tolist())]
        temp = temp[['bpnr', 'categorie', 'status']].drop_duplicates()
        temp['koppeling'] = "CP"

        overview = overview.append([temp, temp_2], sort=False)

    # %% LN projecten die niet een 34 of 35 nummer hebben, maar wel gekoppeld zijn aan een bpnr
    temp = ln.df_non3435[ln.df_non3435['bpnr'] != ''][['ln_id', 'bpnr']]
    temp = temp[temp['bpnr'].isin(cp.nb['bpnr'].tolist())]
    if len(temp) > 0:
        temp.at[:, 'categorie'] = "34_vooraanleg"
        temp['koppeling'] = "LN"
        temp = temp.merge(connect.df, on='bpnr', how='left').drop_duplicates(
            subset=['ln_id', 'bpnr', 'con_opdrachtid'])
        temp.at[temp['con_opdrachtid'].notna(), 'categorie'] = '34_nieuwbouw'
        temp.at[temp['con_opdrachtid'].notna(
        ), 'koppeling'] = 'LN > Connect (BPNR)'
        temp.at[:, 'let_op'] = 'Dit LN nummer bevat een referentie naar ChangePoint, maar is niet als 34 geregisteerd in LN'
        overview = overview.append(temp, sort=False)

    # %% LN projecten die niet een 34 of 35 nummer hebben, maar wel gekoppeld zijn aan een connectnummer
    temp = ln.df_non3435[ln.df_non3435['con_opdrachtid'] !=
                         ''][['ln_id', 'con_opdrachtid', 'ln_project_status']]
    temp = expand_column(temp, 'con_opdrachtid')
    temp = temp.merge(connect.df, on='con_opdrachtid', how='inner')[
        ['ln_id', 'con_opdrachtid', 'ln_project_status', 'order_type']].drop_duplicates()
    if len(temp) > 0:
        temp.at[:, 'categorie'] = "35_aanleg"
        temp.at[~(temp['ln_project_status'] == 'Actief'),
                'let_op'] = 'LNnr is niet actief'
        temp.at[~(
            temp['ln_id'].str.startswith('34') |
            temp['ln_id'].str.startswith('35') |
            temp['ln_id'].str.startswith('45')), 'let_op2'] = 'LNnr is geen 34-, 35- of 45-nummer'
        # Een Connect Opdracht type 'Verplaatsing' mag een 30-nummer hebben.
        temp.at[
            temp['ln_id'].str.startswith('30') & (
                temp['order_type'] == 'Verplaatsing'),
            'let_op2'] = ''
        temp['koppeling'] = "LN > Connect (ConnID)"
        overview = overview.append(temp, sort=False)

    # %% intake 35 aanleg: geen actief LN gevonden; geen BPNR; wel een connect opdracht
    int35 = connect.df[(connect.df['bpnr'].isna()) &
                       ~(connect.df['con_opdrachtid'].isin(
                           overview['con_opdrachtid'].tolist()))
                       & connect.df['active_con']
                       ]
    int35.at[:, 'categorie'] = "35_aanleg_intake"

    int34 = connect.df[(connect.df['bpnr'].notna()) &
                       ~(connect.df['con_opdrachtid'].isin(
                           overview['con_opdrachtid'].tolist()))
                       & connect.df['active_con']
                       & ~(connect.df['bpnr'].isin(set(overview['bpnr'])))
                       ]
    int34.at[:, 'categorie'] = "34_nieuwbouw_intake"
    int35 = int35.append(int34, sort=False).fillna('')
    int35_v = int35.groupby(['con_opdrachtid', 'status_request', 'status_object', 'status_payment']).agg(
        {'con_objectid': 'nunique', 'date_request': 'first'}).reset_index()
    int35_v = int35_v.merge(
        int35.groupby('con_opdrachtid')['con_objectid'].nunique(
        ).reset_index().rename(columns={'con_objectid': 'Totaal'}),
        on='con_opdrachtid',
        how='left')
    int35_v['verhouding'] = int35_v['con_objectid']/int35_v['Totaal']
    temp = int35[['con_opdrachtid', 'bpnr', 'categorie']].drop_duplicates()
    temp['koppeling'] = "Connect"

    # Afgerekende opdrachten
    temp.at[temp['con_opdrachtid'].isin(
        int35_v[
            (int35_v['verhouding'] == 1) &
            (int35_v['status_payment'] == 'Afgerekend') &
            (int35_v['status_request'] == 'Gereed')
        ]['con_opdrachtid'].to_list()),
        'status'] = 'alle Connect objecten zijn afgerekend en de aanvraag is gereed, maar er is geen LN project bekend'

    # Niet afgesloten opdracht
    temp.at[temp['con_opdrachtid'].isin(
        int35_v[
            (int35_v['verhouding'] == 1) &
            (int35_v['status_payment'] == 'Afgerekend') &
            (int35_v['status_request'] != 'Gereed')
        ]['con_opdrachtid'].to_list()),
        'status'] = 'alle Connect objecten zijn afgerekend maar de Connect aanvraag is niet gereed'

    # Vervallen opdracht
    temp.at[temp['con_opdrachtid'].isin(
        int35_v[
            (int35_v['verhouding'] == 1) &
            (int35_v['status_object'] == 'Vervallen')
        ]['con_opdrachtid'].to_list()),
        'status'] = 'alle Connect objecten zijn vervallen'

    # Hoge vervaloptie voor single opdrachten met een datum lang geleden
    temp.at[temp['con_opdrachtid'].isin(
        int35_v[
            (int35_v['Totaal'] == 1) &
            (int35_v['status_object'] != 'Vervallen') &
            (int35_v['status_payment'] != 'Afgerekend') &
            (int35_v['status_request'] != 'Gereed') &
            (int35_v['date_request'] < pd.to_datetime(
                'today') - pd.DateOffset(days=400))
        ]['con_opdrachtid'].to_list()),
        'status'] = 'Dit betreft een enkele Connect opdracht die meer dan 400 dagen geleden is aangemeld en nog niet gereed is'

    # Te koppelen met inactieve nieuwbouw LN
    temp_2 = temp.merge(ln.df_nonActive[[
                        'con_opdrachtid', 'ln_id', 'ln_project_status']], how='inner', on='con_opdrachtid')
    temp_2['koppeling'] = "Connect > LN (ConnID)"
    temp_2['let_op'] = ''
    temp_2.at[~(temp_2['ln_project_status'] == 'Actief'),
              'let_op'] = 'LNnr is niet actief'
    temp_2.at[~(
        temp_2['ln_id'].str.startswith('34') |
        temp_2['ln_id'].str.startswith('35') |
        temp_2['ln_id'].str.startswith('40') |
        temp_2['ln_id'].str.startswith('45')), 'let_op2'] = \
        'LNnr is geen 34-, 35-, 40- of 45-nummer'
    temp = temp[~temp['con_opdrachtid'].isin(
        temp_2['con_opdrachtid'].tolist())]
    overview = overview.append([temp, temp_2], sort=False)

    # %% Intake 34_nieuwbouw:    con_opdracht heeft bpnr in Connect
    #                           con_opdracht nog niet gekoppeld en is actief
    #                           bpnr komt niet voor in LN
    temp = connect.df[
        connect.df['bpnr'].notna() &
        connect.df['active_con'] &
        ~(connect.df['bpnr'].isin(ln.df_proj['bpnr'])) &
        ~(connect.df['con_opdrachtid'].isin(
            overview['con_opdrachtid'].unique()))
    ][['con_opdrachtid', 'bpnr']].drop_duplicates()
    temp['categorie'] = '34_nieuwbouw_intake'
    temp['koppeling'] = 'Connect'
    overview = overview.append(temp)

    # %% Bepaal welke connectOpdrachtID's dubbele CPnrs hebben
    multipleCP = connect.df_orig[['con_opdrachtid', 'bpnr']].copy()
    multipleCP = multipleCP.fillna('ontbreekt')
    multipleCP = multipleCP.groupby('con_opdrachtid')[
        'bpnr'].nunique().reset_index()
    multipleCP = multipleCP[multipleCP['bpnr'] > 1]['con_opdrachtid'].tolist()
    overview.at[(overview['con_opdrachtid'].isin(multipleCP)),
                'error'] = 'Deze Connect opdracht valt over meerdere bouwplannummers'

    # %% Bepaal welke CP orders bestaan
    overview.at[
        ((~overview['bpnr'].isin(cp.nb['bpnr'].tolist())) &
         overview['bpnr'].notna() &
         (overview['bpnr'] != '')),
        'cp_nummer'] = 'CP nummer is niet als nieuwbouwproject geregisteerd in CP'
    overview.at[
        ((~overview['bpnr'].isin(cp.df['bpnr'].tolist())) &
         overview['bpnr'].notna() &
         (overview['bpnr'] != '')),
        'cp_nummer'
    ] = 'CP nummer bestaat niet in CP'

    # %% Bepaal welke Connect orders bestaan
    overview.at[~overview['con_opdrachtid'].isin(set(connect.df['con_opdrachtid'].tolist()) | set(
        [np.nan])), 'con_nummer'] = 'Connect opdracht is niet als actief aanleg project geregisteerd in Connect'
    overview.at[~overview['con_opdrachtid'].isin(set(connect.df_orig['con_opdrachtid'].tolist(
    )) | set([np.nan])), 'con_nummer'] = 'Connect opdracht bestaat niet in Connect'

    # %% Bepaal welke connectorders in meerdere LNnummers zitten
    temp = overview.fillna('').groupby('con_opdrachtid').nunique()
    templist = set(temp[temp['ln_id'] > 1].index.tolist())-set([''])

    message = "Dit connectId bevindt zich in meerdere LN projecten"
    overview.at[overview['con_opdrachtid'].isin(templist), 'meerln'] = message
    templist = overview[(overview['meerln'] == message) & overview['ln_id'].fillna(
        '') == '']['con_opdrachtid'].tolist()
    overview.at[overview['con_opdrachtid'].isin(
        templist), 'meerln'] = "Dit connectId bevindt zich zowel in een LN project als in intake"

    # %% Bepaal welke CPnummers in meerdere LNnummers zitten
    temp = overview.fillna('').groupby('bpnr').nunique()
    templist = set(temp[temp['ln_id'] > 1].index.tolist())-set([''])
    message = "Dit CPnummer bevindt zich in meerdere LN projecten"
    overview.at[overview['bpnr'].isin(templist), 'meercp_ln'] = message
    templist = overview[(overview['meercp_ln'] == message) & (
        overview['ln_id'].fillna('') == '')]['bpnr'].unique()
    overview.at[overview['bpnr'].isin(
        templist), 'meercp_ln'] = "Dit CPnummer bevindt zich zowel in een LN project als in intake"

    # %% Bepaald welke CPnummers gecanceled of afgesloten zijn
    afgesloten = cp.df[cp.df['cp_fase'].str.lower(
    ).str.contains('project completed')]
    cancel = cp.df[cp.df['cp_fase'].str.lower().str.contains('cancel')]
    overview['cp_closed'] = ''
    overview.at[overview['bpnr'].isin(afgesloten['bpnr'].tolist()) &
                overview['bpnr'].notna(), 'cp_closed'] = "Dit CPnummer is afgesloten"
    overview.at[overview['bpnr'].isin(cancel['bpnr'].tolist()) &
                overview['bpnr'].notna(), 'cp_closed'] = "Dit CPnummer is gecanceled"

    # %% Bijzondere regels:
    # Wel LNnummers gevonden, maar niet binnen de juiste categorie:
    notintake = ((overview['categorie'].str.endswith('_intake')) &
                 (overview['ln_id'].notna()))
    overview.at[notintake,
                'categorie'] = overview[notintake]['categorie'].str[:-7]

    # Als alles in connect is afgerekend en in een afgesloten LN project heeft:
    afgesloten = (overview['status'].str.contains('alle Connect objecten zijn afgerekend en de aanvraag is gereed, maar er is geen LN project bekend')) & \
        (overview['let_op'].str.contains('LNnr is niet actief')) & \
        ((overview['bpnr'].isna()) | (overview['cp_nummer'] == 'CP nummer bestaat niet in CP') |
         (overview['cp_closed'] != ''))
    afgesloten = afgesloten | (
        (overview['status'].str.contains('alle Connect objecten zijn afgerekend en de aanvraag is gereed, maar er is geen LN project bekend')) |
        ((overview['status'].str.contains(
            'alle Connect objecten zijn vervallen')) & overview['ln_id'].isna())
    )
    afgesloten = afgesloten | \
        (overview['let_op'].str.contains('LNnr is niet actief')) & \
        ((overview['cp_nummer'] == 'CP nummer bestaat niet in CP') |
         (overview['cp_closed'] != ''))
    temp = overview[afgesloten].copy()
    temp['categorie'] = temp['categorie'].str[:3] + 'afgesloten'
    overview = overview[~afgesloten]
    overview = overview.append(temp, sort=False)

    # %% reformat
    overview = merge_columns_text(overview, ['error', 'let_op', 'let_op2', 'meerln', 'meercp_ln',
                                             'status', 'cp_nummer', 'con_nummer', 'cp_closed'], 'Projectstructuur constateringen')
    overview.at[overview['categorie'].str.contains(
        'afgesloten'), 'Projectstructuur constateringen'] = ''

    # %% DETERMINE CAPACITY
    determine_capacity = False
    if determine_capacity:
        capacity = overview[['bpnr', 'ln_id', 'con_opdrachtid',
                             'cp_nummer', 'con_nummer', 'categorie']].copy()
        capacity = capacity[
            (capacity['categorie'].str[:2].isin(['34', '35'])) &
            (capacity['cp_nummer'].isna()) &
            (capacity['con_nummer'].isna())
        ]

        for i, row in capacity.iterrows():
            if len(cp.df[cp.df['bpnr'] == row['bpnr']]) > 0:
                capacity.at[i, 'cp'] = cp.df[cp.df['bpnr']
                                             == row['bpnr']]['capaciteit'].values[0]
            if row['con_opdrachtid'] != np.nan:
                con_temp = connect.df[connect.df['con_opdrachtid']
                                      == row['con_opdrachtid']]
                capacity.at[i, 'connect_orig'] = con_temp['con_objectid'].count()
                capacity.at[i, 'connect_now'] = con_temp[
                    (con_temp['status_object'] != 'Vervallen') &
                    (con_temp['Afzegreden'].isna())
                ]['con_objectid'].count()
            if row['ln_id'] != np.nan:
                capacity.at[i, 'inforln'] = ln.df_proj[ln.df_proj['ln_id']
                                                       == row['ln_id']]['Aantal_aansluitingen'].sum()

        capacity[['bpnr', 'ln_id', 'con_opdrachtid']] = capacity[[
            'bpnr', 'ln_id', 'con_opdrachtid']].fillna('')

        # %% Bepaal filters capaciteit
        cap_34_nb = capacity['categorie'].str.contains('nieuwbouw')
        cap_35 = (capacity['categorie'].str.startswith('35')) & ~(
            capacity['categorie'].str.contains('intake'))
        cap_34_va = capacity['categorie'].str.contains('vooraanleg')
        cap_cp2con = (capacity.cp.between(capacity['connect_now'], capacity['connect_orig'])) | \
            (capacity.cp.between(
                capacity['connect_orig'], capacity['connect_now']))
        cap_ln2con = (capacity.inforln.between(capacity['connect_now'], capacity['connect_orig'])) | \
            (capacity.inforln.between(
                capacity['connect_orig'], capacity['connect_now']))
        cap_ln2cp = (capacity.inforln == capacity.cp)

        # %% Capacity Nieuwbouw
        capacity_nb = capacity[cap_34_nb].groupby(['ln_id', 'bpnr', 'categorie']).agg({
            'cp': 'first', 'inforln': 'first', 'connect_orig': 'sum', 'connect_now': 'sum'
        })

        cap_cp2con = (capacity_nb.cp.between(capacity_nb['connect_now'], capacity_nb['connect_orig'])) | \
            (capacity_nb.cp.between(
                capacity_nb['connect_orig'], capacity_nb['connect_now']))
        cap_ln2con = (capacity_nb.inforln.between(capacity_nb['connect_now'], capacity_nb['connect_orig'])) | \
            (capacity_nb.inforln.between(
                capacity_nb['connect_orig'], capacity_nb['connect_now']))
        cap_ln2cp = (capacity_nb.inforln == capacity_nb.cp)

        capacity_nb.at[
            :, 'cap'] = 'capaciteit in een of meerdere systemen nul'

        capacity_nb.at[
            (capacity_nb['connect_orig'] > 0) &
            (capacity_nb['inforln'] > 0) &
            (capacity_nb['cp'] > 0), 'cap'] = 'capaciteit overal ingevuld, maar niet gelijk'

        capacity_nb.at[cap_cp2con & cap_ln2con,
                       'cap'] = 'de capaciteit in LN en CP vallen beide tussen het aantal initiele aanvragen in Connect en het aantal huidige aanvragen in Connect'

        capacity_nb.at[
            (capacity_nb['connect_now'] == capacity_nb['cp']) &
            (capacity_nb['connect_now'] == capacity_nb['inforln']), 'cap'] = 'de capaciteit in LN en CP komt overeen met het aantal huidige openstaande objecten in Connect'

        capacity_nb.at[
            (capacity_nb['connect_orig'] == capacity_nb['cp']) &
            (capacity_nb['connect_orig'] == capacity_nb['inforln']),
            'cap'] = 'de capaciteit in LN en CP komt overeen met het initiele aantal objecten in Connect'

        capacity_nb = capacity_nb.reset_index()
        capacity_nb = capacity[cap_34_nb].merge(capacity_nb[['ln_id', 'bpnr', 'categorie', 'cap']], on=[
                                                'ln_id', 'bpnr', 'categorie'], how='left')

        # %% Capacity Vooraanleg
        cap_cp2con = (capacity.cp.between(capacity['connect_now'], capacity['connect_orig'])) | \
            (capacity.cp.between(
                capacity['connect_orig'], capacity['connect_now']))
        cap_ln2con = (capacity.inforln.between(capacity['connect_now'], capacity['connect_orig'])) | \
            (capacity.inforln.between(
                capacity['connect_orig'], capacity['connect_now']))
        cap_ln2cp = (capacity.inforln == capacity.cp)

        capacity.at[cap_34_va &
                    ((capacity['cp'] == 0) |
                     (capacity['inforln'] == 0)), 'cap'] = 'capaciteit in een of meer systemen gelijk aan nul'

        capacity.at[cap_34_va &
                    (capacity['cp'] > 0) &
                    (capacity['inforln'] > 0), 'cap'] = 'de capaciteiten zijn niet gelijk'

        capacity.at[cap_34_va & cap_ln2cp,
                    'cap'] = 'de capaciteit in LN komt overeen met de capaciteit in CP'

        # %% Capacity Aanleg
        capacity.at[cap_35 &
                    ((capacity['connect_orig'] == 0) |
                     (capacity['inforln'] == 0)), 'cap'] = 'capaciteit in een of meer systemen gelijk aan nul'

        capacity.at[cap_35 &
                    ((capacity['connect_orig'] > 0) &
                     (capacity['inforln'] > 0)), 'cap'] = 'de capaciteiten zijn niet gelijk'

        capacity.at[cap_35 & cap_ln2con,
                    'cap'] = 'de capaciteit in LN valt tussen het aantal initiele aanvragen in Connect en het aantal huidige aanvragen in Connect'

        capacity.at[cap_35 & (capacity['connect_now'] == capacity['inforln']),
                    'cap'] = 'de capaciteit in LN komt overeen met het aantal huidige openstaande objecten in Connect'

        capacity.at[cap_35 &
                    (capacity['connect_orig'] == capacity.inforln),
                    'cap'] = 'de capaciteit in LN komt overeen met het initiele aantal objecten in Connect'

        # %% Combineer nieuwbouw en de rest
        capacity = capacity[~cap_34_nb].append(capacity_nb, sort=False)

        # %% Complete list and append
        overview[['bpnr', 'ln_id', 'con_opdrachtid']] = overview[[
            'bpnr', 'ln_id', 'con_opdrachtid']].fillna('')
        overview = overview.merge(capacity[['bpnr', 'ln_id', 'con_opdrachtid', 'cap', 'inforln', 'connect_orig', 'connect_now', 'cp']], how='left', on=[
                                  'bpnr', 'ln_id', 'con_opdrachtid']).rename(columns={'cap': 'capaciteit'})

    # %% Save list
    to_export = ['ln_id', 'bpnr', 'con_opdrachtid', 'categorie', 'Projectstructuur constateringen', 'koppeling'
                 ]

    if determine_capacity:
        to_export = to_export + ['capaciteit',
                                 'inforln', 'connect_orig', 'connect_now', 'cp']

    exportexcel = False
    if exportexcel:
        overview_export = overview[to_export].rename(columns={
            'ln_id': 'LN nummer',
            'bpnr': 'CP nummer',
            'con_opdrachtid': 'Connect Opdracht ID',
            'inforln': 'Capaciteit LN',
            'connect_orig': 'Capaciteit Connect (origineel)',
            'connect_now': 'Capaciteit Connect (nu)',
            'cp': 'Capaciteit CP'
        })

        # Schrijf naar Excel
        overview_export.to_excel('overview.xlsx', index=False)

    # #%% Get list of actions
    # a = [el for el in overview['Projectstructuur constateringen'].str.split('; ').tolist() if isinstance(el,list)]
    # b = []
    # for el in a:
    #     for ell in el:
    #         b.append(ell)
    # b = set(b)
    # print(b)

    # %% doe de checks
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

    # %% Xaris koppeling
    # xaris = xaris_()
    # overview, relevante_xaris = xaris.toevoegen_xaris_foutmeldingen(connect, overview)
    # overview = overview.reset_index()
    # to_export = ['ln_id'
    #             ,'bpnr'
    #             ,'con_opdrachtid'
    #             ,'Xaris'
    #             ,'categorie'
    #             ,'Projectstructuur constateringen'
    #             ,'koppeling'
    #             ]

    ## split 'LNnr niet actief' --> CP nog open, Connect nog open, Cp en Connect nog open
    cp_fases_closed = ['46. Cancel', '99. Project Completed', '301. Voortijdig Afsluiten']
    overview['cp_is_afgesloten'] = overview['cp_fase'].isin(cp_fases_closed)
    overview['connect_is_afgesloten'] = overview['verhouding'] == 1
    
    # categorie 34_vooraanleg --> LNnr is niet actief, CP nog open
    mask = (
        (overview['let_op'] == 'LNnr is niet actief') &
        (overview['categorie'] == '34_vooraanleg')
    )
    overview.at[mask, 'Projectstructuur constateringen'] = overview[mask]['Projectstructuur constateringen'].\
        str.replace('LNnr is niet actief', 'LNnr is niet actief, CP nog open')
    # categorie 35_aanleg en 35_afgesloten --> LNnr is niet actief, Connect nog open
    mask = (
        (overview['let_op'] == 'LNnr is niet actief') &
        (overview['categorie'].isin(['35_aanleg', '35_afgesloten']))
    )
    overview.at[mask, 'Projectstructuur constateringen'] = overview[mask]['Projectstructuur constateringen'].\
        str.replace('LNnr is niet actief', 'LNnr is niet actief, Connect nog open')   
    
    # categorie 34_nieuwbouw en 34_afgesloten: Check welke nog open is
    # alleen CP nog open
    mask = (
        (overview['let_op'] == 'LNnr is niet actief') &
        (overview['categorie'].isin(['34_nieuwbouw', '34_afgesloten'])) &
        (~overview['cp_is_afgesloten']) &
        (overview['connect_is_afgesloten'])
    )
    overview.at[mask, 'Projectstructuur constateringen'] = overview[mask]['Projectstructuur constateringen'].\
        str.replace('LNnr is niet actief', 'LNnr is niet actief, CP nog open')
    # alleen Connect nog open
    mask = (
        (overview['let_op'] == 'LNnr is niet actief') &
        (overview['categorie'].isin(['34_nieuwbouw', '34_afgesloten'])) &
        (overview['cp_is_afgesloten']) &
        (~overview['connect_is_afgesloten'])
    )
    overview.at[mask, 'Projectstructuur constateringen'] = overview[mask]['Projectstructuur constateringen'].\
        str.replace('LNnr is niet actief', 'LNnr is niet actief, Connect nog open')
    # Beide nog open 
    mask = (
        (overview['let_op'] == 'LNnr is niet actief') &
        (overview['categorie'].isin(['34_nieuwbouw', '34_afgesloten'])) &
        (~overview['cp_is_afgesloten']) &
        (~overview['connect_is_afgesloten'])
    )
    overview.at[mask, 'Projectstructuur constateringen'] = overview[mask]['Projectstructuur constateringen'].\
        str.replace('LNnr is niet actief', 'LNnr is niet actief, CP en Connect nog open')

    return overview[to_export], intake  # , relevante_xaris
