import config
import pandas as pd
import numpy as np
import sqlalchemy as sa
import utils

from connection import Connection
from db.queries import read
from db.models import czHierarchy, czLog, czCleaning
from analysis.projectstructuur import xaris_status_check


def compute_fases(lncpcon_data, overview, xar):

    ln = lncpcon_data.ln.df
    cp = lncpcon_data.cp.df
    connect = lncpcon_data.connect.df

    # functie om faseringen op te schonen en samen te smelten met info uit originele dataframes
    def fix_fases(df, ln=False, cp=False, con=False):
        df = df.replace(np.nan, '')
        if ln:
            df[ln_fase.columns[0]] = df[ln_fase.columns[0]].astype(str).str[:3]
            ln_fase[ln_fase.columns[0]] = ln_fase[ln_fase.columns[0]].astype(str).str[:3]
            df = df.merge(ln_fase, on=ln_fase.columns[0], how='left').rename(columns={'Fase': 'lnfase_fijn'})
        if cp:
            df = df.merge(cp_fase, on=cp_fase.columns[0], how='left').drop(cp_fase.columns[0], axis=1)
        if con:
            df = df.merge(con_aan_fase, on=con_aan_fase.columns[0], how='left').drop(con_aan_fase.columns[0], axis=1)
            df = df.merge(con_obj_fase, on=con_obj_fase.columns[0], how='left').drop(con_obj_fase.columns[0], axis=1)
            df = df.merge(con_afr_fase, on=con_afr_fase.columns[0], how='left').drop(con_afr_fase.columns[0], axis=1)
        return df.replace('', np.nan)

    # Fase check waardering file geleverd vanuit Connect-Z, bevat mapping ln, cp en con en waarderingen ln vs cp en lv vs connect
    ln_fase = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='ln').replace(np.nan, '')
    ln_fase['ln_fase'] = ln_fase['ln_fase'].str[:3].astype(str)
    cp_fase = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='cp').replace(np.nan, '')
    con_aan_fase = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='con_aanvraagstatus').replace(np.nan, '')
    con_obj_fase = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='con_objectstatus').replace(np.nan, '')
    con_afr_fase = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='con_afrekenstatus').replace(np.nan, '').replace('[empty]', '')
    con_cols = ['con_request', 'con_object', 'con_payment']

    LNvsCP = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='ln_vs_cp')
    LNvsCon = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='ln_vs_con')
    LNvsCon = pd.melt(LNvsCon,
                      id_vars=con_cols,
                      value_vars=ln_fase['lnfase'].drop_duplicates().tolist(),
                      var_name='lnfase',
                      value_name='aanleg')
    LNvsCon['nieuwbouw'] = LNvsCon['aanleg']
    LNvsCon = LNvsCon.drop(['aanleg'], axis=1)
    LNvsCon = LNvsCon.rename(columns={'nieuwbouw': 'status'})

    # Connect fase -- combineren van connect met overview
    con_fases_correct = fix_fases(connect, con=True)
    connect_fase = con_fases_correct[['con_opdrachtid', 'con_objectid'] + con_cols].fillna('')
    connect_tot = connect_fase.groupby(['con_opdrachtid'])['con_objectid'].count().reset_index().rename(columns={'con_objectid': 'totaal'})
    connect_fase = connect_fase.groupby(['con_opdrachtid'] + con_cols)['con_objectid'].count().reset_index().rename(columns={'con_objectid': 'aantal_conobj'})
    connect_fase = connect_fase.merge(connect_tot, on='con_opdrachtid').reset_index(drop=True)
    connect_fase.at[:, 'verhouding'] = connect_fase['aantal_conobj'] / connect_fase['totaal']
    connect_all = connect_fase

    # Ln fase -- combineren van ln met overview
    ln_all = fix_fases(ln, ln=True)
    ln_all = ln_all[['ln_id', 'lnfase']]
    ln_all = ln_all.merge(overview[['ln_id', 'bpnr', 'categorie']], on='ln_id', how='left')
    ln_all = ln_all[~ln_all['bpnr'].isna()]

    # CP fase -- combineren van cp met overview
    cp_all = fix_fases(cp, cp=True)
    cp_all = cp_all[['project_id', 'cpfase']]
    cp_all = cp_all.rename(columns={'project_id': 'bpnr'})

    # Fase check ln vs cp
    ln_vs_cp = pd.merge(ln_all, cp_all, how='inner', on='bpnr')
    ln_vs_cp = ln_vs_cp[~ln_vs_cp['cpfase'].isna()]
    ln_vs_cp_nb = ln_vs_cp[ln_vs_cp['categorie'] == '34_nieuwbouw']
    ln_vs_cp_al = ln_vs_cp[ln_vs_cp['categorie'] == '34_vooraanleg']

    status_ln_cp_nb = pd.merge(ln_vs_cp_nb, LNvsCP[['lnfase', 'cpfase', 'nieuwbouw']], how='left', left_on=['lnfase', 'cpfase'], right_on=['lnfase', 'cpfase'])
    status_ln_cp_nb = status_ln_cp_nb.rename(columns={'nieuwbouw': 'status'})
    status_ln_cp_al = pd.merge(ln_vs_cp_al, LNvsCP[['lnfase', 'cpfase', 'vooraanleg']], how='left', left_on=['lnfase', 'cpfase'], right_on=['lnfase', 'cpfase'])
    status_ln_cp_al = status_ln_cp_al.rename(columns={'vooraanleg': 'status'})
    status_ln_cp = pd.concat([status_ln_cp_nb, status_ln_cp_al], sort=True).reset_index(drop=True)
    status_ln_cp = status_ln_cp[['ln_id', 'bpnr', 'lnfase', 'cpfase', 'categorie', 'status']]
    status_ln_cp = status_ln_cp.drop_duplicates(subset=['ln_id', 'bpnr'])

    # Fase check ln vs connect
    ln_all_t = ln_all.merge(overview[['ln_id', 'con_opdrachtid']], how='left', on='ln_id')
    ln_vs_con = pd.merge(ln_all_t, connect_all, how='inner', on='con_opdrachtid')
    ln_vs_con = ln_vs_con.drop(['bpnr'], axis=1)
    status_ln_con = pd.merge(ln_vs_con, LNvsCon, how='left',
                             left_on=['lnfase', 'con_request', 'con_object', 'con_payment'],
                             right_on=['lnfase', 'con_request', 'con_object', 'con_payment'])
    status_ln_con = status_ln_con[['ln_id', 'con_opdrachtid', 'con_request', 'con_object', 'con_payment', 'lnfase', 'status', 'aantal_conobj', 'totaal', 'verhouding']]
    status_ln_con = status_ln_con.drop_duplicates()

    if status_ln_con['status'].isna().any() or (status_ln_con['status'] == '').any():
        missing = status_ln_con[(status_ln_con['status'].isna()) | (status_ln_con['status'] == '')]
        raise ValueError("Unknown status found ln vs connect: {}".format(missing.iloc[0]))

    if status_ln_cp['status'].isna().any() or (status_ln_cp['status'] == '').any():
        missing = status_ln_cp[(status_ln_cp['status'].isna()) | (status_ln_cp['status'] == '')]
        raise ValueError("Unknown status found ln vs cp: {}".format(missing.iloc[0]))

    status_ln_con['sourceKey'] = status_ln_con['ln_id'] + '|' + status_ln_con['con_opdrachtid']
    d = {}
    for i, row in status_ln_con.sort_values(by=['verhouding']).iterrows():
        if len(status_ln_con[status_ln_con['sourceKey'].str.startswith(row['sourceKey'])]) > 1:
            nr = d.setdefault(row['sourceKey'], 0)
            nr = nr + 1
            d[row['sourceKey']] = nr
            status_ln_con.loc[i, 'sourceKey'] = row['sourceKey'] + '_' + str(nr)

    # Fase check Xaris
    relevante_xaris = xaris_status_check(xar.df, lncpcon_data.connect.df_orig)
    relevante_xaris['Aanvraagdatum'] = relevante_xaris['Aanvraagdatum'].astype(str)
    xar_fases = utils.download_as_dataframe(config.tmp_bucket, config.files['fases'], sheet_name='xaris_vs_con')
    relevante_xaris = relevante_xaris.merge(
        xar_fases, on=['Con_status', 'Con_uitvoering', 'Status Xaris'], how='left'
        )

    return status_ln_cp, status_ln_con, relevante_xaris
