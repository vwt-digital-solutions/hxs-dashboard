import os
import re
import numpy as np
import pandas as pd


class connect_vz():

    def __init__(self, path, file, from_pickle=False):
        if from_pickle:
            self.df = pd.read_pickle(file + '_df.p')
            self.df_orig = pd.read_pickle(file + '_df_orig.p')
        else:
            self.df_orig = pd.DataFrame([])
            for f in file:
                df_temp = pd.read_excel(os.path.join(path, f))
                self.df_orig = self.df_orig.append(df_temp)
            self.df_orig = self.df_orig.drop_duplicates(subset='Uniek ID', keep='first')

            # Filter products:
            self.df_orig = self.df_orig[~self.df_orig['Product'].isin([11, 62, 112, 140])]
            self.df = self.df_orig.copy()

        self.standard_columns = [
             'Uniek ID',
             'Code extern',
             'Product',
             'Bouwplan nummer',
             'Locatiecode',
             'Huisaannemer gebied',
             'Aanvraagdatum',
             'Aantal objecten',
             'Type uitvoering',
             'Type opdracht',
             'NearNet',
             'Status aanvraag',
             'Samengevoegd met',
             'Multi-aanvraag',
             'Kostenkenmerk',
             'Opdrachtstatus',
             'Clean order',
             'Clean order sinds',
             'Reden geen clean order',
             'Pc4code',
             'Afzegreden',
             'Sloopreden',
             'Object straat',
             'Object huisnummer',
             'Object toevoeging',
             'Object postcode',
             'Object plaats',
             'Status object',
             'Datum gereed',
             'Uitvalreden adres',
             'Uitvalreden ORCA',
             'X-coördinaat',
             'Y-coördinaat',
             'Afrekenstatus',
             'Afrekenstatus datum',
        ]

    def selectColumns(self, columns=[]):
        self.df = self.df_orig[
            self.standard_columns +
            columns
        ]
        self.addbpnr()
        self.renamecols()

    def to_pickle(self, name):
        self.df.to_pickle(name + '_df.p')
        self.df_orig.to_pickle(name + '_df_orig.p')

    def reduceNieuwbouw(self, pc4codes, vervallen=True):
        """Reduceer de originele set door alleen nieuwbouw-aanvragen over te houden
        pc4codes: list of postcodes (first four numbers) that define the working ares
        vervallen: if False: include orders with 'Object Status' == Vervallen and that do not have a 'Afzegreden',
        if True (default) exclude them from the set"""

        # Pas filters toe op originele set
        postcode = self.df['Object postcode'].str[:4].isin(pc4codes)
        aanleg = self.df['Type opdracht'].isin(['Aanleg', 'Sloop', 'Verplaatsing'])
        opdracht = self.df['Status aanvraag'] != 'Geen opdracht'
        ookopdracht = self.df['Status object'] != 'Geen opdracht'
        residentieel = self.df['Kostenkenmerk'] == 'Residentieel'

        niet_afgezegd = self.df['Afzegreden'].isna()
        nietvervallen = self.df['Status object'] != 'Vervallen'

        actief = self.extract_active_orders(self.df)

        self.df['active_con'] = actief
        self.df = self.df[aanleg & postcode & opdracht & ookopdracht & residentieel].reset_index(drop=True)

        if vervallen:
            self.df = self.df[niet_afgezegd & nietvervallen]

    def extract_active_orders(self, df):
        # Vind alle con opdracht ids waarvan alle objecten de Opdrachtstatus Gereed, Status object Gereed | Vervallen,
        # Status aanvraag Gereed en Afrekenstatus Afgerekend hebben
        check = df.copy()
        check.at[check['Status object'] == 'Vervallen', 'Afrekenstatus'] = 'Afgerekend'
        check.at[check['Afzegreden'].notna(), 'Afrekenstatus'] = 'Afgerekend'
        check = check.groupby(['con_opdrachtid', 'Afrekenstatus'])['con_objectid'].count().reset_index().rename(
            columns={'con_objectid': 'afgerekend'})
        check = check.merge(df.groupby(['con_opdrachtid'])['con_objectid'].count().reset_index(),
                            on='con_opdrachtid',
                            how='left')
        check = check.merge(df[['con_opdrachtid', 'Opdrachtstatus', 'Status aanvraag']].drop_duplicates(),
                            on='con_opdrachtid',
                            how='left')

        gereed = (check['Status aanvraag'] == 'Gereed') & \
                 (check['Afrekenstatus'] == 'Afgerekend') & \
                 (check['con_objectid'] == check['afgerekend']) & \
                 (check['Opdrachtstatus'] == 'Gereed')

        gereed = check[gereed]['con_opdrachtid'].tolist()
        gereed = df['con_opdrachtid'].isin(gereed)

        return np.logical_not(gereed)

    def addbpnr(self):
        self.df['bpnr'] = self.df['Bouwplan nummer'].astype(str).apply(get_bpnr_regex)
        self.df_orig['bpnr'] = self.df_orig['Bouwplan nummer'].astype(str).apply(get_bpnr_regex)

    def renamecols(self, cols={'Code extern': 'con_opdrachtid', 'Uniek ID': 'con_objectid'}):
        if cols:
            self.df = self.df.rename(columns=cols)
            self.df_orig = self.df_orig.rename(columns=cols)

    def getColumns(self, orig=True):
        if orig:
            return self.df_orig.columns
        else:
            return self.df.columns


class inforln(connect_vz):

    def __init__(self, path, file, from_pickle=False):
        if from_pickle:
            self.df_proj_orig = pd.read_pickle(file + '_df_proj_orig.p')
            self.df = pd.read_pickle(file + '_df.p')
            self.df_proj = pd.read_pickle(file + '_df_proj.p')
            self.df_non3435 = pd.read_pickle(file + '_df_non3.p')
            self.df_nonActive = pd.read_pickle(file + '_df_nona.p')

        else:
            self.df_proj_orig = pd.read_excel(os.path.join(path, file), converters={
                'Project': str
            }).rename(columns={"Project": 'lnnr'})
            self.df_proj = self.df_proj_orig.copy()

            try:
                self.df_hist_orig = pd.read_excel(os.path.join(path, file), sheet_name="ProjectInforLN - Fase historie")
                self.df_hist = self.df_hist_orig.copy()
            except Exception:
                pass

    def to_pickle(self, name):
        self.df.to_pickle(name + '_df.p')
        self.df_proj_orig.to_pickle(name + '_df_proj_orig.p')
        self.df_proj.to_pickle(name + '_df_proj.p')
        self.df_non3435.to_pickle(name + '_df_non3.p')
        self.df_nonActive.to_pickle(name + '_df_nona.p')

    def get_active(self):
        nummer34 = self.df_proj['lnnr'].str.startswith('34')
        nummer35 = self.df_proj['lnnr'].str.startswith('35')
        nummer45 = self.df_proj['lnnr'].str.startswith('45')
        self.df_proj['active_ln'] = self.df_proj.Projectstatus == 'Actief'
        actief = self.df_proj['active_ln']
        self.df = self.df_proj[(nummer34 | nummer35 | nummer45) & actief]
        self.df_non3435 = self.df_proj[~(nummer34 | nummer35 | nummer45) & actief]
        self.df_nonActive = self.df_proj.loc[(nummer34 | nummer35 | nummer45) & ~actief]

    def filterProjectNumbers(self):
        """Examination of data shows that 31 out of 21169 project numbers have length less than nine, so we filter those out
        as they seem to be indicative of messy entries.

        For example, the length-8 project numbers all start with either `SJAB` or `TEMP`.
        All length-9 project numbers (in our initial data dumps) are fully numeric and can be retained."""

        mask = self.df_proj['lnnr'].str.len() == 9
        self.df_proj = self.df_proj.loc[mask]

    def getbpnr(self):
        referentie = pd.DataFrame(self.df_proj['Zoekargument'].astype(str).apply(get_bpnr_regex))
        referentie['bpnr'] = ''
        for i, row in referentie.iterrows():
            referentie.at[i, 'bpnr'] = ', '.join(list(set(row.values) - set(['nan', '', np.nan])))

        self.df_proj = self.df_proj.join(referentie['bpnr'])

    def getconopdrachtid(self):
        pattern_connect = r'(1000\d{6}|H\d{8})'
        referentie = self.df_proj['Zoekargument'].astype(str).str.extract(pattern_connect, expand=True)

        referentie['con_opdrachtid'] = ''
        for i, row in referentie.iterrows():
            referentie.at[i, 'con_opdrachtid'] = ', '.join(list(set(row.values) - set(['nan', '', np.nan])))

        self.df_proj = self.df_proj.join(referentie['con_opdrachtid'])


class xaris_sub(connect_vz):

    def __init__(self, path, file):
        self.df_orig = pd.read_excel(os.path.join(path, file))
        self.df = self.df_orig.copy()
        self.getHnr()

    def getHnr(self):
        self.df.rename(columns={'Aanvraagnummer': 'x_pjnr'})
        self.df['x_hnr'] = self.df['Hoofdleidingenprojectnummer'].str.split(' - ', expand=True)[0]
        self.df['x_pjnr'] = self.df['Hoofdleidingenprojectnummer'].str.split(' - ', expand=True)[1]


class xaris_hoofd(connect_vz):

    def __init__(self, path, file):
        self.df_orig = pd.read_excel(os.path.join(path, file))
        self.df = self.df_orig.copy()
        self.getHnr()

    def getHnr(self):
        self.df = self.df.rename(columns={'Nummer': 'x_hnr'})
        self.df['x_pjnr'] = self.df['Projectnummer'].astype(str).str.extract(r'(\d\d\d\d\d\d\d\d)')


class cp(connect_vz):

    def __init__(self, path, file, from_pickle=False):
        if from_pickle:
            self.df = pd.read_pickle(file + '_df.p')
        else:
            self.df_orig = pd.DataFrame([])
            for f in file:
                df_temp = pd.read_excel(os.path.join(path, f), converters={'User-defined project ID': str})
                self.df_orig = self.df_orig.append(df_temp)
            self.df = self.df_orig.copy()

        self.standard_columns = [
            'User-defined project ID',
            'Project name',
            'Project Classification',
            'HoofdProjectNummer',
            'Location',
            'Related Program',
            'Partner',
            'Fase',
            'Aantal aansluitingen',
            'Step name',
            'Date Created',
        ]

    def addbpnr(self):
        self.df_orig['bpnr'] = self.df_orig['Project name'].astype(str).apply(get_bpnr_regex)
        self.df['bpnr'] = self.df['Project name'].astype(str).apply(get_bpnr_regex)

    def add_contractor(self, contractor=None):
        if contractor is None:
            self.df['contractor'] = np.nan
        else:
            self.df = self.df.merge(contractor, on=['bpnr'], how='left')

    def add_place(self, place=None):
        if place is None:
            self.df['plaats'] = np.nan
        else:
            self.df = self.df.merge(place, on=['bpnr'], how='left')

    def add_potency(self, potency=None):
        if potency is None:
            self.df['potentie'] = np.nan
        else:
            self.df = self.df.merge(potency, on=['bpnr'], how='left')

    def renamecols(self, cols={}):
        self.df = self.df.rename(columns=dict({'Aantal aansluitingen': 'capaciteit'}, **cols))

    def reduceConnectZ(self):
        self.nb = self.nb[self.nb['contractor'] == 'Connect-Z Utrecht']

    def splitFrames(self):
        cp = self.df.copy()
        self.nb = cp[cp['Project Classification'].str.lower().str.contains('nieuwbouw')]
        self.rec = cp[(cp['Project Classification'].str.contains('Reconstructies')) &
                      ~(cp['Project Classification'].str.contains('Kleine'))]
        self.krec = cp[(cp['Project Classification'].str.contains('Kleine'))]

    def to_pickle(self, name):
        self.df.to_pickle(name + '_df.p')


def expand_column(df, column_name, splitter=','):
    containssplitter = df[column_name].fillna('').str.contains(splitter)
    check = df[containssplitter]
    if len(check) == 0:
        return df
    checkjoin = check[column_name].str.split(splitter, expand=True)
    checkjoined = checkjoin[0]
    for i in checkjoin.columns[1:]:
        checkjoined = checkjoined.append(checkjoin[i])
    checkjoined = pd.DataFrame(checkjoined).rename(columns={0: column_name})
    check = check.drop(column_name, axis=1).join(checkjoined)

    return df[~containssplitter].append(check)


def get_bpnr_regex(cell):
    reconstruction = re.compile(r'(REC20\d{7})')
    nonreconstruction = re.compile(r'(20\d{7})')
    bpnr = reconstruction.findall(cell)
    if len(bpnr) == 0:
        bpnr = nonreconstruction.findall(cell)
    if len(bpnr) == 0:
        return np.nan
    return bpnr[0]
