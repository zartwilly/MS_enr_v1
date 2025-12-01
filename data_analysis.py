#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 26 22:05:58 2025

@author: wil

data analysis
"""
import os
import time
import pandas as pd
import fct_aux as aux
import generation_power as genPV
import loadInsertRun_ts as runts

import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
#                   count MW by bus et plot df_bus: Debut
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
        
    df_bus = pd.DataFrame.from_dict(
                {timestamp: info['bus'] for timestamp, info in dico_ts.items()},
                orient='index'
            )
    df_bus.index.name = 'datetime'
    df_bus.reset_index(inplace=True)
        
    return dico_ts, df_bus

def identify_loading_by_bus_BIS():
    
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    resultat = net.load.groupby('bus').apply(agregation_par_bus).round(6)
    
    df_bus = []
    for id_bus in resultat.index:
        liste_ts = list()
        print(f'**** bus = {id_bus} ****')
        for ts in profiles.index:
            id_bats = resultat.loc[id_bus,:]['index_batiments']
            som_mw = 0
            for id_bat in id_bats:
                som_mw += profiles.loc[ts,id_bat]
            liste_ts.append({"datetime": ts, "id_bus": id_bus, "n_bats": len(id_bats),
                             "som_load_ts_mw": som_mw, 
                             "som_p_mw": resultat.loc[id_bus,"somme_p_mw"] })
        df_ts = pd.DataFrame(liste_ts)
        df_bus.append(df_ts)
        
    df_bus = pd.concat(df_bus, axis=0)
    return df_bus

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

def plot_df_bus(df_bus):
    # Convertir datetime en pandas datetime si besoin
    df_bus['datetime'] = pd.to_datetime(df_bus['datetime'])
    
    id_bus_list = df_bus['id_bus'].unique()
    n_buses = len(id_bus_list)
    
    # Définir le nombre de colonnes et lignes pour le tableau des subplots
    cols = 2
    rows = (n_buses + cols - 1) // cols
    
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=[f"Bus {bus}" for bus in id_bus_list])
    
    for i, bus_id in enumerate(id_bus_list):
        row = i // cols + 1
        col = i % cols + 1
        group = df_bus[df_bus['id_bus'] == bus_id]
    
        # Courbe som_load_ts_mw
        fig.add_trace(
            go.Scatter(x=group['datetime'], y=group['som_load_ts_mw'], 
                       mode='lines+markers', name=f'load_bus_mw {bus_id}'),
            row=row, col=col
        )
        # Courbe som_p_mw
        fig.add_trace(
            go.Scatter(x=group['datetime'], y=group['som_p_mw'], 
                       mode='lines+markers', name=f'p_mw_bus_{bus_id}'),
            row=row, col=col
        )
        
        print(f" ***** id_bus = {bus_id}, row={row}, col={col} ***** rows={rows}, group={group.shape} ")
    
    fig.update_layout(
        height=300 * rows, width=800,  # ajuster taille selon nombre de subplots
        title_text='Evolution temporelle des charges par bus (p_mw, p_mw_max) activables/désactivables',
        showlegend=True,
        hovermode='x unified'
    )
    
    # Enregistrer dans un fichier HTML
    Path(aux.PLOT_DATA).mkdir(parents=True, exist_ok=True)
    fig.write_html( os.path.join(aux.PLOT_DATA,"plot_interactif_dataBusLoad.html"))
    
def plot_df_bus_saveFolder(df_bus):
    # Convertir datetime en pandas datetime si besoin
    df_bus['datetime'] = pd.to_datetime(df_bus['datetime'])
    
    id_bus_list = df_bus['id_bus'].unique()
    
    # Créer le dossier pour les figures individuelles
    dossier_figures = os.path.join(aux.PLOT_DATA, "figures_par_bus")
    Path(dossier_figures).mkdir(parents=True, exist_ok=True)
    
    for bus_id in id_bus_list:
        group = df_bus[df_bus['id_bus'] == bus_id]
        
        # Créer une figure individuelle pour ce bus
        fig = go.Figure()
        
        # Courbe som_load_ts_mw
        fig.add_trace(go.Scatter(
            x=group['datetime'], 
            y=group['som_load_ts_mw'], 
            mode='lines+markers', 
            name='load_bus_mw',
            line=dict(color='blue')
        ))
        
        # Courbe som_p_mw
        fig.add_trace(go.Scatter(
            x=group['datetime'], 
            y=group['som_p_mw'], 
            mode='lines+markers', 
            name='p_mw_bus',
            line=dict(color='red')
        ))
        
        fig.update_layout(
            height=500, width=1000,
            title_text=f'Évolution temporelle Bus {bus_id} (load vs p_mw)',
            xaxis_title='Datetime',
            yaxis_title='Puissance (MW)',
            showlegend=True,
            hovermode='x unified'
        )
        
        # Enregistrer la figure individuelle
        nom_fichier = f"bus_{bus_id}_evolution.html"
        chemin_fichier = os.path.join(dossier_figures, nom_fichier)
        fig.write_html(chemin_fichier)
        
        print(f"Figure sauvegardée: {chemin_fichier} (shape: {group.shape})")
        
        # Optionnel: afficher dans le notebook
        # fig.show()
    
    print(f"Toutes les figures ont été sauvegardées dans: {dossier_figures}")
    
    
def plot_df_bus_grouped(df_bus, group_size=50):
    df_bus['datetime'] = pd.to_datetime(df_bus['datetime'])
    id_bus_list = sorted(df_bus['id_bus'].unique())

    dossier_figures = os.path.join(aux.PLOT_DATA, "figures_par_bus_grouped")
    Path(dossier_figures).mkdir(parents=True, exist_ok=True)

    # Parcourir les buses par groupes de group_size
    for i in range(0, len(id_bus_list), group_size):
        group_buses = id_bus_list[i:i+group_size]
        n_buses = len(group_buses)

        # Définir la taille du subplot
        cols = 2
        rows = (n_buses + cols - 1) // cols

        fig = make_subplots(rows=rows, cols=cols, subplot_titles=[f"Bus {bus}" for bus in group_buses])

        for j, bus_id in enumerate(group_buses):
            row = j // cols + 1
            col = j % cols + 1

            group = df_bus[df_bus['id_bus'] == bus_id]

            fig.add_trace(go.Scatter(
                x=group['datetime'], y=group['som_load_ts_mw'],
                mode='lines+markers', name=f'load_bus_mw {bus_id}'
            ), row=row, col=col)

            fig.add_trace(go.Scatter(
                x=group['datetime'], y=group['som_p_mw'],
                mode='lines+markers', name=f'p_mw_bus_{bus_id}'
            ), row=row, col=col)

        fig.update_layout(
            height=300 * rows, width=1000,
            title_text=f'Évolution temporelle des charges par bus du {group_buses[0]} au {group_buses[-1]}',
            showlegend=True,
            hovermode='x unified'
        )

        nom_fichier = f"bus_{group_buses[0]}_to_{group_buses[-1]}_evolution.html"
        chemin_fichier = os.path.join(dossier_figures, nom_fichier)
        fig.write_html(chemin_fichier)
        print(f"Fichier sauvegardé : {chemin_fichier}")
###############################################################################
#                   count MW by bus : Fin
###############################################################################


###############################################################################
#                   search timestamp with max loading : Debut
###############################################################################
def get_all_full_loading_timestamps(df_bus):
    """
    Retourne un DataFrame avec TOUS les timestamps où som_load_ts_mw == som_p_mw pour chaque bus
    
    Parameters:
    df_bus: DataFrame avec colonnes datetime, id_bus, som_load_ts_mw, som_p_mw, n_bats
    
    Returns:
    DataFrame avec tous les timestamps de pleine charge par bus
    """
    # Comparaison avec tolérance de 3 décimales
    mask_full_load = df_bus['som_load_ts_mw'].round(3) == df_bus['som_p_mw'].round(3)
    # Identifier les bus où load == puissance max pour CHAQUE timestamp
    df_full_load = df_bus[mask_full_load].copy()
    #df_full_load = df_bus[df_bus['som_load_ts_mw'] == df_bus['som_p_mw']].copy()
    
    if df_full_load.empty:
        print("Aucun timestamp de pleine charge trouvé")
        return pd.DataFrame()
    
    # Trier par datetime et id_bus
    df_full_load = df_full_load.sort_values(['datetime', 'id_bus'])
    
    print(f"{len(df_full_load)} cas de pleine charge trouvés sur {len(df_bus)} timestamps total")
    print(f"Nombre de bus uniques concernés: {df_full_load['id_bus'].nunique()}")
    
    return df_full_load[['datetime', 'id_bus', 'n_bats', 'som_load_ts_mw', 'som_p_mw']]

def plot_full_loading_evolution(df_full_timestamps):
    """
    Graphique interactif : nombre de bus en pleine charge par timestamp
    Hover affiche les id_bus, som_p_mw et n_bats
    """
    
    # Grouper par datetime et compter le nombre de bus
    # df_agg = df_full_timestamps.groupby('datetime').agg({
    #     'id_bus': 'count',  # Nombre de bus
    #     'id_bus': lambda x: ', '.join(x.astype(str)),  # Liste des id_bus
    #     'som_p_mw': lambda x: ', '.join(x.round(7).astype(str)),  # Liste som_p_mw
    #     'n_bats': lambda x: ', '.join(x.astype(str))  # Liste n_bats
    # }).reset_index()
    df_agg = df_full_timestamps.groupby('datetime').agg(
                nb_buses=pd.NamedAgg(column='id_bus', aggfunc='count'),
                liste_id_bus=pd.NamedAgg(column='id_bus', aggfunc=lambda x: ', '.join(x.astype(str))),
                liste_som_p_mw=pd.NamedAgg(column='som_p_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                liste_n_bats=pd.NamedAgg(column='n_bats', aggfunc=lambda x: ', '.join(x.astype(str)))
                ).reset_index()
    
    df_agg.columns = ['datetime', 'nb_buses', 'liste_id_bus', 'liste_som_p_mw', 'liste_n_bats']
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_agg['datetime'],
        y=df_agg['nb_buses'],
        name='Nombre de bus en pleine charge',
        text=df_agg['nb_buses'],
        hovertemplate=(
            '<b>%{x}</b><br>' +
            'Nombre de bus: <b>%{y}</b><br>' +
            'ID Bus: %{customdata[0]}<br>' +
            'Puissance max (MW): %{customdata[1]}<br>' +
            'Nb bâtiments: %{customdata[2]}' +
            '<extra></extra>'
        ),
        customdata=df_agg[['liste_id_bus', 'liste_som_p_mw', 'liste_n_bats']].values,
        marker_color='darkblue',
        textposition='auto'
    ))
    
    fig.update_layout(
        title='Évolution du nombre de bus en pleine charge (som_load_ts_mw == som_p_mw)',
        xaxis_title='Timestamp',
        yaxis_title='Nombre de bus en pleine charge',
        hovermode='x unified',
        height=600,
        showlegend=False
    )
    
    
    
    
    
    # Sauvegarder
    Path(aux.PLOT_DATA).mkdir(parents=True, exist_ok=True)
    fig.write_html(os.path.join(aux.PLOT_DATA, "bus_full_loading_evolution.html"))
    print("Graphique sauvegardé dans plot_full_loading_evolution.html")
    
    return fig
###############################################################################
#                   search timestamp with max values : Fin
###############################################################################


###############################################################################
#                   runtime
###############################################################################
if __name__ == '__main__':
    ti = time.time()
    df_ac, profiles = plot_data()
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    
    #dico_ts, df_bus = identify_loading_by_bus()
    #plot_df_bus(df_bus=df_bus) 
    
    df_bus_BIS = identify_loading_by_bus_BIS()
    #plot_df_bus(df_bus=df_bus_BIS)                                 # saving file too heavy 250Mo 
    #plot_df_bus_saveFolder(df_bus_BIS)
    #plot_df_bus_grouped(df_bus_BIS, group_size=50)
    
    # Récupérer TOUS les timestamps de pleine charge
    df_full_timestamps = get_all_full_loading_timestamps(df_bus_BIS)
    fig = plot_full_loading_evolution(df_full_timestamps)
    
    print(f" ---> runtime = {time.time() - ti} <---")
    pass
    
