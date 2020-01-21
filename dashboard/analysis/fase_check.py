import pandas as pd
import numpy as np

from connection import Connection
from db.queries import read
from copy import copy


def fase_check():
    with Connection('r', 'get_fases') as session:
        status_ln_cp = read(session, 'fases_ln|cp').astype({'status': 'float'})
        status_ln_con = read(session, 'fases_ln|con').astype({'status': 'float'})
        relevante_xaris = read(session, 'fases_xaris|con').astype({'Mapping': 'float'})

    # Genereren van een lijst van foutmeldingen voor ln-cp (nieuwbouw en vooraanleg) met status en totaal
    status_ln_cp['totaal'] = 1
    status_ln_cp_nb = status_ln_cp[status_ln_cp['categorie'] == '34_nieuwbouw']
    types_fout_cp_nb = status_ln_cp_nb.pivot_table(index=['lnfase', 'cpfase', 'status'], aggfunc=np.sum, values=['totaal'])
    types_fout_cp_nb = pd.DataFrame(types_fout_cp_nb.to_records())

    status_ln_cp_al = status_ln_cp[status_ln_cp['categorie'] == '34_vooraanleg']
    types_fout_cp_al = status_ln_cp_al.pivot_table(index=['lnfase', 'cpfase', 'status'], aggfunc=np.sum, values=['totaal'])
    types_fout_cp_al = pd.DataFrame(types_fout_cp_al.to_records())

    # Genereren van een lijst van foutmeldingen voor ln-connect met status en totaal
    status_ln_con['totaal'] = 1
    types_fout_con = status_ln_con.pivot_table(index=['con_request', 'con_object', 'con_payment', 'lnfase', 'status'], aggfunc=np.sum, values=['totaal'])
    types_fout_con = pd.DataFrame(types_fout_con.to_records())

    # Genereren van pivot tabel voor status pagina met totalen (df_types_) en statussen (df_types_status_), 1: ln-cp-nb, 2: ln-cp-al, 3: ln-connect
    indexer = 'cpfase'
    df_types_1_ = types_fout_cp_nb.pivot_table(index=indexer, columns='lnfase', values='totaal').fillna(0).reset_index()
    df_types_1_status = types_fout_cp_nb.pivot_table(index=indexer, columns='lnfase', values='status').fillna(1).reset_index()
    df_types_2_ = types_fout_cp_al.pivot_table(index=indexer, columns='lnfase', values='totaal').fillna(0).reset_index()
    df_types_2_status = types_fout_cp_al.pivot_table(index=indexer, columns='lnfase', values='status').fillna(1).reset_index()

    indexer = ['con_request', 'con_object', 'con_payment']
    df_types_3_ = types_fout_con.pivot_table(index=indexer, columns='lnfase', values='totaal').fillna(0).reset_index()
    df_types_3_status = types_fout_con.pivot_table(index=indexer, columns='lnfase', values='status').fillna(1).reset_index()

    # Inlezen van Xaris
    totaal_tabel = relevante_xaris.copy()

    groupcount = relevante_xaris.groupby(['Con_status', 'Con_uitvoering', 'Status Xaris']).\
        agg({'juist_nummer': 'count', 'Mapping': 'first'}).\
        rename(columns={'juist_nummer': 'Totaal', 'Mapping': 'Kleuren'}).reset_index()
    count_pivot = groupcount.pivot_table(index=['Con_status', 'Con_uitvoering'],
                                         columns='Status Xaris', values='Totaal').fillna(0).reset_index()
    color_pivot = groupcount.pivot_table(index=['Con_status', 'Con_uitvoering'],
                                         columns='Status Xaris', values='Kleuren').fillna(0).reset_index()

    # Xaris data zonder dat we zonder connect, slechts tot 2018
    mask = ((relevante_xaris['Con_status'] == 'Geen connect') & (relevante_xaris['Aanvraagdatum'] < '2018-01-01'))
    relevante_xaris = relevante_xaris[~mask]

    totaal_tabel_f = relevante_xaris.copy()

    groupcount_f = relevante_xaris.groupby(['Con_status', 'Con_uitvoering', 'Status Xaris']).\
        agg({'juist_nummer': 'count', 'Mapping': 'first'}).\
        rename(columns={'juist_nummer': 'Totaal', 'Mapping': 'Kleuren'}).reset_index()
    count_pivot_f = groupcount_f.pivot_table(index=['Con_status', 'Con_uitvoering'], columns='Status Xaris', values='Totaal').fillna(0).reset_index()
    color_pivot_f = groupcount_f.pivot_table(index=['Con_status', 'Con_uitvoering'], columns='Status Xaris', values='Kleuren').fillna(0).reset_index()

    xaris = {
        'types_1': types_fout_cp_nb,
        'types_2': types_fout_cp_al,
        'types_3': types_fout_con,
        'status_1': status_ln_cp,
        'status_2': status_ln_con,
        'types_1_unique': df_types_1_,
        'types_1_unique_status': df_types_1_status,
        'types_2_unique': df_types_2_,
        'types_2_unique_status': df_types_2_status,
        'types_3_unique':  df_types_3_,
        'types_3_unique_status': df_types_3_status
    }

    xaris_all = {
        'types_4': groupcount,
        'status_4': totaal_tabel,
        'types_4_unique': count_pivot,
        'types_4_unique_status': color_pivot
    }

    xaris_filter = {
        'types_4': groupcount_f,
        'status_4': totaal_tabel_f,
        'types_4_unique': count_pivot_f,
        'types_4_unique_status': color_pivot_f
    }

    xaris_all.update(xaris)
    xaris_filter.update(xaris)

    for key in xaris_all:
        xaris_all[key] = xaris_all[key].to_dict()
    for key in xaris_filter:
        xaris_filter[key] = xaris_filter[key].to_dict()

    return xaris_all, xaris_filter
