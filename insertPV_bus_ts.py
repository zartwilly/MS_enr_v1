#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 17 10:32:55 2025

@author: wil

insert pv on selected bus at specified ts
"""
import os
import time
import numpy as np
import pandas as pd
import itertools as it

import fct_aux as aux
import generation_power as genPV
import data_analysis as dat_an

import pandapower as pp
from pandapower.timeseries import DFData 
from pandapower.timeseries import OutputWriter 
from pandapower.timeseries import run_timeseries
from pandapower.control import ConstControl

from pathlib import Path

def insertPV_bus_ts(df_bus):
    df_ac = genPV.generate_pv_ac()
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    
    date = pd.Timestamp(year=aux.YEAR, month=aux.DICO_TS_HIVER["month"], 
                        day=aux.DICO_TS_HIVER["day"], hour=aux.DICO_TS_HIVER["hour"], 
                        minute=aux.DICO_TS_HIVER["minute"], second=0, tz=aux.tz)
    
    # selectionner les profiles avec la date
    profiles_date = profiles[profiles.index == date]
    
    ## selectionner les bus overloaded
    df_bus_dat = df_bus[df_bus.datetime == date].groupby('datetime').agg(
                    nb_buses=pd.NamedAgg(column='id_bus', aggfunc='count'),
                    liste_id_bus=pd.NamedAgg(column='id_bus', aggfunc=lambda x: ', '.join(x.astype(str))),
                    liste_som_p_mw=pd.NamedAgg(column='som_p_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                    liste_som_load_ts_mw=pd.NamedAgg(column='som_load_ts_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                    liste_n_bats=pd.NamedAgg(column='n_bats', aggfunc=lambda x: ', '.join(x.astype(str)))
                ).reset_index()
    
    # selectionner les bus dont charge > 10^(4) 
    liste_id_bus = [float(x) for x in df_bus_dat['liste_id_bus'].tolist()[0].split(',')]
    liste_p_mw = [float(x) for x in df_bus_dat['liste_som_p_mw'].tolist()[0].split(',')]
    liste_load_ts_mw = [float(x) for x in df_bus_dat['liste_som_load_ts_mw'].tolist()[0].split(',')]
    idbus_p_load = zip(liste_id_bus, liste_p_mw, liste_load_ts_mw)
    
    nouvelle_liste = [(x, z, y, y - z) for (x, y, z) in idbus_p_load ]
    sorted_idbus_p_load = sorted(nouvelle_liste, key=lambda t: t[3], reverse=False)
    
    sorted_idbus_p_loaded = [t for t in sorted_idbus_p_load if t[3] >= aux.VAL_BUS_MAX_CHARGE]
    
    # ajouter les PV aux bus selectionnés
    
    return profiles, date, df_bus_dat, sorted_idbus_p_loaded

###############################################################################
#                   Controllers, output writer : Debut
###############################################################################
def create_controllers(net, ds, profiles_day):
    load_idx = net.load.index
    ConstControl(net, "load", "p_mw", element_index=load_idx, 
                 data_source=ds, 
                 profile_name=profiles_day.columns.tolist() )
    
def create_output_writer(net, time_steps, output_dir):
    ow = OutputWriter(net, time_steps, output_path=output_dir, 
                      output_file_type=".xlsx", log_variables=[])
    # these variables are saved to the harddisk after / during the time series loop
    ow.log_variable('res_load', 'p_mw')
    ow.log_variable('res_bus', 'vm_pu')
    ow.log_variable('res_line', 'loading_percent')
    ow.log_variable('res_line', 'i_ka')
    return ow
###############################################################################
#                   Controllers, output writer : FIN
###############################################################################

###############################################################################
#                   get bus by policy : DEBUT
###############################################################################
def build_prefix_tuples(data):
    """
    data : liste de tuples (x0, x1, x2, x3)
    retourne : [[(x0, x3)], [(x0, x3), (x0_2, x3_2)], ...]
    """
    result = []
    for i in range(1, len(data) + 1):
        result.append(data[:i])
    return result

def build_list_of_prefixes(data, bool_sort=True):
    """
    data : liste de tuples (x0, x1, x2, x3), (x10, x11, x12, x13), ...
    retourne : [[(x0, x1, x2, x3),], [(x0, x1, x2, x3), (x10, x11, x12, x13)], ...]
    """
    
    sorted_data = None
    if bool_sort:
        # 1) trier par le 5e élément (indice 4) décroissant
        sorted_data = sorted(data, key=lambda t: t[4], reverse=True)
        # sorted_data vaut :
        # [(600...,5), (606...,4), (674...,2), (507...,1), (555...,0)]
    else:
        # deja trier par le 4-th item of tuple data 
        sorted_data = data

    # 2) construire les préfixes
    result = []
    for i in range(1, len(sorted_data) + 1):
        result.append(sorted_data[:i])
    return result

def selected_bus_by_strategy(sorted_idbus_p_loaded, strategy="most_loaded_bus"):
    """
    

    Parameters
    ----------
    sorted_idbus_p_loaded : Tuple
        DESCRIPTION.
        sorted tuple by 4-th item. items are 
        id_bus, som_p_mw, som_load_ts_mw, miss_load, nb_bats
        
    strategy: str = most_loaded_bus, most_connected_bus
        policy to geneerate combinaison of choices

    Returns
    -------
    None.

    """
    match strategy:
        case "most_loaded_bus":
            return build_list_of_prefixes(data=sorted_idbus_p_loaded, bool_sort=False)
        case "most_connected_bus":
            return build_list_of_prefixes(data=sorted_idbus_p_loaded, bool_sort=True)
        case _:
            return sorted_idbus_p_loaded                                        # Default case
    pass
###############################################################################
#                   get bus by policy : FIN
###############################################################################

###############################################################################
#          ajouter les PV aux bus selectionnés and runtimes : Debut
###############################################################################
def check_limits(net,vmin=0.9, vmax=1.1, loading_max=100.0):

    #pp.runpp(net)

    if not net["converged"]:
        print("⚠️ Le flux de puissance n'a pas convergé.")
        return

    # Tensions
    vm = net.res_bus.vm_pu
    buses_hors_limites = net.res_bus[~vm.between(vmin, vmax)]
    percent_bus_hors_limites = buses_hors_limites.shape[0] / net.res_bus.shape[0]
    print(f" --> Tensions bus hors limites : { percent_bus_hors_limites }")

    # Lignes
    lignes_surchargees = net.res_line[net.res_line.loading_percent >= loading_max]
    lignes_total = net.res_line.shape[0]
    percent_lignes_surchargees = lignes_surchargees.shape[0] / lignes_total
    print(f"\n --> Lignes surchargées : {percent_lignes_surchargees}")

    # Transformateurs
    print("\nTransformateurs surchargés :")    
    loading_trafos = net.res_trafo.loading_percent  # % de charge trafos
    trafos_surchargees = net.res_trafo[net.res_trafo.loading_percent >= loading_max]
    percent_trafos_surchargees = trafos_surchargees.shape[0]/loading_trafos.shape[0]
    print(f"--> transfos surchargees = {percent_trafos_surchargees}")
    
    dico={"bus_hors_limites": percent_bus_hors_limites, 
          "Lignes_surchargees": percent_lignes_surchargees, 
          "transfos surchargees": percent_trafos_surchargees}
    return dico

def runtime_with_added_PV_on_overloaded_bus(output_dir, df_bus):
    """
    runtime net at one timestamp with added PV on computed overloaded bus 

    Parameters
    ----------
    sorted_idbus_p_loaded : TYPE
        DESCRIPTION.
        liste de tuples avec (id_bus, som_p_mw, som_load_ts_mw, som_load_ts_mw-som_p_mw)
    date : pd.Timestamp
        DESCRIPTION.
        exple: Timestamp('2023-03-02 13:30:00-0100', tz='Etc/GMT+1')
    df_ac : pd.Series
        DESCRIPTION.
        generation des Puissances nominales sur 1 m² en kw en AC

    Returns
    -------
    None.

    """
    # 1. load net
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    
    # 2. load data source from profiles
    df_ac = genPV.generate_pv_ac()
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    ### format date to timestamp
    date = pd.Timestamp(year=aux.YEAR, month=aux.DICO_TS_HIVER["month"], 
                        day=aux.DICO_TS_HIVER["day"], hour=aux.DICO_TS_HIVER["hour"], 
                        minute=aux.DICO_TS_HIVER["minute"], second=0, tz=aux.tz)
    
    ### selectionner les bus overloaded
    df_bus_dat = df_bus[df_bus.datetime == date].groupby('datetime').agg(
                    nb_buses=pd.NamedAgg(column='id_bus', aggfunc='count'),
                    liste_id_bus=pd.NamedAgg(column='id_bus', aggfunc=lambda x: ', '.join(x.astype(str))),
                    liste_som_p_mw=pd.NamedAgg(column='som_p_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                    liste_som_load_ts_mw=pd.NamedAgg(column='som_load_ts_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                    liste_n_bats=pd.NamedAgg(column='n_bats', aggfunc=lambda x: ', '.join(x.astype(str)))
                ).reset_index()
    
    ### selectionner les bus dont charge > 10^(4) 
    liste_id_bus = [float(x) for x in df_bus_dat['liste_id_bus'].tolist()[0].split(',')]
    liste_p_mw = [float(x) for x in df_bus_dat['liste_som_p_mw'].tolist()[0].split(',')]
    liste_load_ts_mw = [float(x) for x in df_bus_dat['liste_som_load_ts_mw'].tolist()[0].split(',')]
    liste_n_bats = [float(x) for x in df_bus_dat['liste_n_bats'].tolist()[0].split(',')]
    idbus_p_load = zip(liste_id_bus, liste_p_mw, liste_load_ts_mw, liste_n_bats)
    
    nouvelle_liste = [(x, z, y, y - z, u) for (x, y, z, u) in idbus_p_load ]
    sorted_idbus_p_load = sorted(nouvelle_liste, key=lambda t: t[3], reverse=False)
    
    sorted_idbus_p_loaded = [t for t in sorted_idbus_p_load if t[3] >= aux.VAL_BUS_MAX_CHARGE]
    
    selected_bus = selected_bus_by_strategy(sorted_idbus_p_loaded)
    # TODO : replace sorted_idbus_p_loaded by selected_bus line 2.1
    
    # 2.1 : add PV at various bus in the network 
    for id_bus, som_p_mw, som_load_ts_mw, miss_load, nb_bats in sorted_idbus_p_loaded:
        
        bus_pv = int(id_bus)
        val_puis_nominale = df_ac[date]
        nb_square_meter = np.ceil(miss_load/val_puis_nominale)
        
        # Création d’une génératrice PV de Puissance_Nominale MW avec consigne de tension 1.02 p.u.
        pp.create_gen(net, bus=bus_pv,
                      p_mw=np.ceil(miss_load), #val_puis_nominale*nb_square_meter,     # puissance active (mw) injectée (positive pour la production)
                      vm_pu=1.02,   # consigne de tension au nœud
                      name=f"PV_bus_{id_bus}"
                      )
    
    ### selectionner les profiles avec la date
    row_profiles_date_ts = profiles[profiles.index == date]
    for col in row_profiles_date_ts.columns:
        row_profiles_date_ts[col] = row_profiles_date_ts[col].astype('float64')
    
    ds = DFData(row_profiles_date_ts.reset_index(drop=True))
    
    
    
    # 3. create controllers (to control P values of the load and perhaps the sgen)
    # sgen if you use P and Q
    # gen if you use only P
    load_idx = net.load.index
    ConstControl(net, "load", "p_mw", element_index=load_idx, 
                 data_source=ds, 
                 profile_name=row_profiles_date_ts.columns.tolist() )
    
    
    # time steps to be calculated. Could also be a list with non-consecutive time steps
    n_timesteps = row_profiles_date_ts.shape[0]
    time_steps = range(0, n_timesteps)
    
    # 4. the output writer with the desired results to be stored to files.
    ow = create_output_writer(net, time_steps, output_dir=output_dir)
    
    # 5. the main time series function
    run_timeseries(net, time_steps)
    print(net.res_line.loading_percent)
    
    # 6. Analysis
    ## check 
    dico = check_limits(net) 
    # df_loadpercent = net.res_line.loading_percent
    # df_buspercent = net.res_bus.vm_pu
    # mask = (df_loadpercent < 0.9) | (df_loadpercent > 1.1)
    # nb_hors_intervalle_par_col_line = mask.sum()
    # mask = (df_buspercent < 0.9) | (df_buspercent > 1.1)
    # nb_hors_intervalle_par_col_bus = mask.sum()
    # print(f"% lines not in [0.9, 1.1] = {nb_hors_intervalle_par_col_line/df_loadpercent.shape[0]}")
    # print(f"% bus not in [0.9, 1.1] = {nb_hors_intervalle_par_col_bus/df_buspercent.shape[0]}")
    
    # # bus hors limites
    # vm = net.res_bus.vm_pu
    # buses_hors_limites = net.res_bus[~vm.between(0.9, 1.1)]
    # print(f"--> bus_hors_limites = {buses_hors_limites.shape[0] / net.res_bus.shape[0]}")
    # print(f"----> bus charges = { len(sorted_idbus_p_loaded) }")
    
    # # lines surchargées
    # lignes_surchargees = net.res_line[net.res_line.loading_percent >= 100]
    # print(f"--> lignes surchargees = {lignes_surchargees.shape[0]}")
    # print(f"----> lignes total = {net.res_line.shape[0]}")
    
    # # transfo surchargees 
    # loading_trafos = net.res_trafo.loading_percent  # % de charge trafos
    # trafos_surchargees = net.res_trafo[net.res_trafo.loading_percent >= 100]
    # print(f"--> trafos_surchargees = {trafos_surchargees.shape[0]/loading_trafos.shape[0]}")
    
    return net
###############################################################################
#          ajouter les PV aux bus selectionnés and runtimes : FIN
############################################################################### 

###############################################################################
#    ajouter les PV aux bus selectionnés VIA STRATEGIES and runtimes : Debut
###############################################################################
def runtime_with_added_PV_on_overloaded_bus_strategy(output_dir, df_bus):
    """
    runtime net at one timestamp with added PV on computed overloaded bus 

    Parameters
    ----------
    sorted_idbus_p_loaded : TYPE
        DESCRIPTION.
        liste de tuples avec (id_bus, som_p_mw, som_load_ts_mw, som_load_ts_mw-som_p_mw)
    date : pd.Timestamp
        DESCRIPTION.
        exple: Timestamp('2023-03-02 13:30:00-0100', tz='Etc/GMT+1')
    df_ac : pd.Series
        DESCRIPTION.
        generation des Puissances nominales sur 1 m² en kw en AC
    strategy: str
        DESCRIPTION
        values : most_loaded_bus, most_connected_load

    Returns
    -------
    None.

    """
    # 1. load net
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    
    # 2. load data source from profiles
    df_ac = genPV.generate_pv_ac()
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    ### format date to timestamp
    date = pd.Timestamp(year=aux.YEAR, month=aux.DICO_TS_HIVER["month"], 
                        day=aux.DICO_TS_HIVER["day"], hour=aux.DICO_TS_HIVER["hour"], 
                        minute=aux.DICO_TS_HIVER["minute"], second=0, tz=aux.tz)
    
    ### selectionner les bus overloaded
    df_bus_dat = df_bus[df_bus.datetime == date].groupby('datetime').agg(
                    nb_buses=pd.NamedAgg(column='id_bus', aggfunc='count'),
                    liste_id_bus=pd.NamedAgg(column='id_bus', aggfunc=lambda x: ', '.join(x.astype(str))),
                    liste_som_p_mw=pd.NamedAgg(column='som_p_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                    liste_som_load_ts_mw=pd.NamedAgg(column='som_load_ts_mw', aggfunc=lambda x: ', '.join(x.round(7).astype(str))),
                    liste_n_bats=pd.NamedAgg(column='n_bats', aggfunc=lambda x: ', '.join(x.astype(str)))
                ).reset_index()
    
    ### selectionner les bus dont charge > 10^(4) 
    liste_id_bus = [float(x) for x in df_bus_dat['liste_id_bus'].tolist()[0].split(',')]
    liste_p_mw = [float(x) for x in df_bus_dat['liste_som_p_mw'].tolist()[0].split(',')]
    liste_load_ts_mw = [float(x) for x in df_bus_dat['liste_som_load_ts_mw'].tolist()[0].split(',')]
    liste_n_bats = [float(x) for x in df_bus_dat['liste_n_bats'].tolist()[0].split(',')]
    idbus_p_load = zip(liste_id_bus, liste_p_mw, liste_load_ts_mw, liste_n_bats)
    
    nouvelle_liste = [(x, z, y, y - z, u) for (x, y, z, u) in idbus_p_load ]
    sorted_idbus_p_load = sorted(nouvelle_liste, key=lambda t: t[3], reverse=False)
    
    sorted_idbus_p_loaded = [t for t in sorted_idbus_p_load if t[3] >= aux.VAL_BUS_MAX_CHARGE]
    

    data_res_net = []
    
    for strategy in aux.STRATEGIES_ADD_PV:
        selected_bus = selected_bus_by_strategy(sorted_idbus_p_loaded, strategy=strategy)
        # TODO : replace sorted_idbus_p_loaded by selected_bus line 2.1
        
        print(f"---> strategy={strategy} bus selectes <---")
        
        for id_selec, sorted_idbus_p_loaded in enumerate(selected_bus):
            print(f" {id_selec} -->")
            # 1. load net
            net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
            # 2.1 : add PV at various bus in the network
            liste_busPV = list()
            for id_bus, som_p_mw, som_load_ts_mw, miss_load, nb_bats in sorted_idbus_p_loaded:
                
                bus_pv = int(id_bus)
                liste_busPV.append(bus_pv)
                val_puis_nominale = df_ac[date]
                nb_square_meter = np.ceil(miss_load/val_puis_nominale)
                
                # Création d’une génératrice PV de Puissance_Nominale MW avec consigne de tension 1.02 p.u.
                pp.create_gen(net, bus=bus_pv,
                              p_mw=np.ceil(miss_load), #val_puis_nominale*nb_square_meter,     # puissance active (mw) injectée (positive pour la production)
                              vm_pu=1.02,   # consigne de tension au nœud
                              name=f"PV_bus_{id_bus}"
                              )
            
            ### selectionner les profiles avec la date
            row_profiles_date_ts = profiles[profiles.index == date]
            for col in row_profiles_date_ts.columns:
                row_profiles_date_ts[col] = row_profiles_date_ts[col].astype('float64')
            
            ds = DFData(row_profiles_date_ts.reset_index(drop=True))
            
            
            
            # 3. create controllers (to control P values of the load and perhaps the sgen)
            # sgen if you use P and Q
            # gen if you use only P
            load_idx = net.load.index
            ConstControl(net, "load", "p_mw", element_index=load_idx, 
                         data_source=ds, 
                         profile_name=row_profiles_date_ts.columns.tolist() )
            
            
            # time steps to be calculated. Could also be a list with non-consecutive time steps
            n_timesteps = row_profiles_date_ts.shape[0]
            time_steps = range(0, n_timesteps)
            
            # 4. the output writer with the desired results to be stored to files.
            ow = create_output_writer(net, time_steps, output_dir=output_dir)
            
            # 5. the main time series function
            run_timeseries(net, time_steps)
            print(net.res_line.loading_percent)
            
            # 6. Analysis
            ## check 
            dico = dict()
            dico = check_limits(net)
            dico["bus_with_PV"] = liste_busPV
            dico["strategy"] = strategy
            
            data_res_net.append(dico)
        
    pd.DataFrame(data_res_net).to_csv(os.path.join(output_dir, "df_res_net.csv"), index=True)
    
    return net
###############################################################################
#    ajouter les PV aux bus selectionnés VIA STRATEGIES and runtimes : FIN
###############################################################################    



if __name__ == '__main__':
    ti = time.time()
    
    df_ac = genPV.generate_pv_ac()
    index_pv = df_ac.index
        
    path = Path(os.path.join(aux.ELEC_NET, "df_bus.csv"))
    if path.is_file():
        df_bus = pd.read_csv(path, index_col=0)
        df_bus['datetime'] = pd.to_datetime(df_bus['datetime']) 
    else:
        df_bus = dat_an.identify_loading_by_bus_BIS(index_pv=index_pv)
    
    # # profiles, date, df_bus_dat, sorted_idbus_p_loaded = insertPV_bus_ts(df_bus)
    

    output_dir = os.path.join(os.getcwd(), "PV_RES_DATA", 
                              f"NETWORK_{aux.DICO_TS_HIVER['period_name']}_day{aux.DICO_TS_HIVER['day']}_month{aux.DICO_TS_HIVER['month']}_H{aux.DICO_TS_HIVER['hour']}_Min{aux.DICO_TS_HIVER['minute']}")
    print("Results can be found in your local temp folder: {}".format(output_dir))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
    #net_res = runtime_with_added_PV_on_overloaded_bus(output_dir, df_bus)
    
    strategy = "most_loaded_bus" #most_connected_bus
    net_res = runtime_with_added_PV_on_overloaded_bus_strategy(output_dir, df_bus)
        
    
    
    print(f" ---> runtime = {time.time() - ti} <---")
