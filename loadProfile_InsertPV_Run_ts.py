#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  6 10:24:12 2025

@author: wil

load profiles by timestamp ts defined by datetime (year, month, day, hour,minute)
run timeseries for this timestamp ts
"""
import os
import datetime
import fct_aux as aux
import pandas as pd


import generation_power as genPV

from pandapower.timeseries import DFData 
from pandapower.timeseries import OutputWriter 
from pandapower.timeseries import run_timeseries
from pandapower.control import ConstControl

###############################################################################
#                   constances : Debut
###############################################################################

DICO_TS_HIVER = {"period_name":"HIVER", "month": 3, "day": 2, "hour": 10, "minute":30}
DICO_TS_SUMMER = {"period_name":"SUMMER", "month": 7, "day": 4, "hour": 10, "minute":30}
DICO_TS_AUTUMN = {"period_name":"AUTUMN", "month": 12, "day": 2, "hour": 13, "minute":30}

###############################################################################
#                   constances : Fin
###############################################################################


###############################################################################
#                   load profiles by timestamp : Debut
###############################################################################
def load_profile_ts(dico):
        
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    df_ac = genPV.generate_pv_ac()
    profiles['datetime'] = df_ac.index
    profiles = profiles.set_index('datetime')
    
    date = pd.Timestamp(year=aux.YEAR, month=dico["month"], 
                        day=dico["day"], hour=dico["hour"],
                        minute=dico["minute"], second=0, tz=aux.tz)
    
    row_profile_ts = profiles.loc[profiles.index == date]
    for col in row_profile_ts.columns:
        row_profile_ts[col] = row_profile_ts[col].astype('float64')
    
    ds = DFData(row_profile_ts.reset_index(drop=True))
    
    return row_profile_ts, ds
###############################################################################
#                   load profiles by timestamp : FIN
###############################################################################

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
#                       Run timeseries: Debut
###############################################################################
def timeseries_run_network(output_dir, dico):
    # 1. load net
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    
    # 2. load data source from profiles
    row_profile_ts, ds = load_profile_ts(dico=dico)
    n_timesteps = row_profile_ts.shape[0]
    
    # 3. create controllers (to control P values of the load and perhaps the sgen)
    create_controllers(net, ds, profiles_day=row_profile_ts)
    
    # time steps to be calculated. Could also be a list with non-consecutive time steps
    time_steps = range(0, n_timesteps)
    
    # 4. the output writer with the desired results to be stored to files.
    ow = create_output_writer(net, time_steps, output_dir=output_dir)

    # 5. the main time series function
    run_timeseries(net, time_steps)
    print(net.res_line.loading_percent)
###############################################################################
#                       Run timeseries: FIN
###############################################################################

###############################################################################
#                   runtime
###############################################################################
if __name__ == '__main__':
    
    output_dir = os.path.join(os.getcwd(), "RES_DATA", 
                              f"NETWORK_{DICO_TS_HIVER['period_name']}_day{DICO_TS_HIVER['day']}_month{DICO_TS_HIVER['month']}_H{DICO_TS_HIVER['hour']}_Min{DICO_TS_HIVER['minute']}")
    print("Results can be found in your local temp folder: {}".format(output_dir))
    if not os.path.exists(output_dir):
        #os.mkdir(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        
    timeseries_run_network(output_dir=output_dir, dico=DICO_TS_HIVER)
    pass
