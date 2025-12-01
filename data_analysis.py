#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 26 22:05:58 2025

@author: wil

data analysis
"""
import os
import pandas as pd
import fct_aux as aux
import generation_power as genPV
import loadInsertRun_ts as runts

import plotly.graph_objects as go

from pathlib import Path

###############################################################################
#                   plot ac_PV and loading_percent : debut
###############################################################################
def plot_data():
    df_ac = genPV.generate_pv_ac()
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    profiles_by_day = profiles.sum(axis=1)
    
    
    fig = go.Figure()

    # Tracer df_ac
    fig.add_trace(go.Scatter(x=df_ac.index, y=df_ac.values, 
                             mode='lines+markers', line=dict(color='blue', width=2),
                             marker=dict(size=6), name='df_ac',
                             hoverinfo='x+y',
                             hoverlabel=dict(namelength=0)))
    
    # Tracer pr20
    fig.add_trace(go.Scatter(x=profiles_by_day.index, y=profiles_by_day.values, 
                             mode='lines+markers', line=dict(color='red', width=2),
                             marker=dict(size=6), name='profiles_by_day',
                             hoverinfo='x+y',
                             hoverlabel=dict(namelength=0)))
    
    # Ajouter un effet au survol: augmenter la largeur de la ligne
    fig.update_traces(
        hoveron='points+fills',
        line_shape='linear'
    )

    # Configurer les axes
    fig.update_layout(
        xaxis_title='Datetime',
        yaxis_title='Values',
        hovermode='x unified',  # Affiche les valeurs des deux séries quand on survole l'axe x
        legend_title_text='Séries'
    )
    
    # Enregistrer dans un fichier HTML
    Path(aux.PLOT_DATA).mkdir(parents=True, exist_ok=True)
    fig.write_html( os.path.join(aux.PLOT_DATA,"plot_interactif_profile_PV.html"))

    
    return df_ac, profiles 
###############################################################################
#                   plot ac_PV and loading_percent : Fin
###############################################################################

###############################################################################
#                   count MW by bus : Debut
###############################################################################
def identify_loading_by_bus():
    
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    resultat = net.load.groupby('bus').apply(agregation_par_bus).round(6)
    
    dico_ts = dict()
    for ts in profiles.index:
        dico_bus = dict()
        for id_bus in resultat.index:
            print(f"--- ts={ts} id_bus={id_bus} ----")
            id_bats = resultat.loc[id_bus,:]['index_batiments']
            som_mw = 0
            for id_bat in id_bats:
                som_mw += profiles.loc[ts,id_bat]
                
            dico_bus["bus"] = {"id_bus": id_bus, "n_bats": len(id_bats),
                               "som_load_ts_mw": som_mw, 
                               "som_p_mw": resultat.loc[id_bus,"somme_p_mw"] }
        dico_ts[ts] = dico_bus
        
    return dico_ts

def agregation_par_bus(group):
    return pd.Series({
        'nombre_batiments': len(group),
        'batiments': sorted(list(group['name'].unique())),
        'index_batiments': sorted(list(group.index)),
        'somme_p_mw': group['p_mw'].sum(),
        'somme_q_mvar': group['q_mvar'].sum(),
        'nb_residential': (group['usage'] == 'residential').sum(),
        'nb_commercial': (group['usage'] == 'commercial').sum(),
        'nb_agriculture': (group['usage'] == 'agriculture').sum()
    })

###############################################################################
#                   count MW by bus : Fin
###############################################################################

###############################################################################
#                   runtime
###############################################################################
if __name__ == '__main__':
    
    df_ac, profiles = plot_data()
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    
    dico_ts = identify_loading_by_bus()
    pass
    
