#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 22 09:53:16 2025

@author: wil
load profile, insert into network and run timeseries.
"""
import os
import fct_aux as aux
import pandas as pd

import datetime

from pandapower.timeseries import DFData 
from pandapower.timeseries import OutputWriter 
from pandapower.timeseries import run_timeseries
from pandapower.control import ConstControl


def load_insert_profiles(day, month):
    
    profiles = aux.load_profiles(profiles_file=aux.PROFILES)
    
    dico_days_ts = aux.create_day_timestamps(year=aux.YEAR, nb_hrs_yr=profiles.shape[0])
    date = datetime.datetime(year=aux.YEAR, month=month, day=day)
    ts_min = min(dico_days_ts[date])
    ts_max = max(dico_days_ts[date])
    
    profiles_day = profiles.iloc[ts_min:ts_max+1, :]
    
    ds = DFData(profiles_day)
    
    return profiles_day, ds   

def create_controllers(net, ds, profiles_day):
    load_cols = profiles_day.columns
    for idx, load_col in enumerate(load_cols):
        ConstControl(net, element="load", variable='p_mw', 
                    # element_index=[0], data_source=ds, 
                    element_index=[net.load.index[idx]], data_source=ds, 
                     profile_name=[load_col])
    
def create_output_writer(net, time_steps, output_dir):
    ow = OutputWriter(net, time_steps, output_path=output_dir, 
                      output_file_type=".xlsx", log_variables=[])
    # these variables are saved to the harddisk after / during the time series loop
    ow.log_variable('res_load', 'p_mw')
    ow.log_variable('res_bus', 'vm_pu')
    ow.log_variable('res_line', 'loading_percent')
    ow.log_variable('res_line', 'i_ka')
    return ow

def timeseries_run_network(output_dir, day, month):
    # 1. load net
    net = aux.load_network(jsonfile=aux.JSONFILE_NETWORK)
    
    # 2. load data source from profiles
    profiles_day, ds = load_insert_profiles(day=day, month=month)
    n_timesteps = profiles_day.shape[0]
    
    # 3. create controllers (to control P values of the load and perhaps the sgen)
    create_controllers(net, ds, profiles_day=profiles_day)
    
    # time steps to be calculated. Could also be a list with non-consecutive time steps
    time_steps = range(0, n_timesteps)
    
    # 4. the output writer with the desired results to be stored to files.
    ow = create_output_writer(net, time_steps, output_dir=output_dir)

    # 5. the main time series function
    run_timeseries(net, time_steps)
    print(net.res_line.loading_percent)
    
###############################################################################
#                   runtime
###############################################################################
if __name__ == '__main__':
    
    day=1; month=1
    
    output_dir = os.path.join(os.getcwd(), "RES_DATA", f"NETWORK_day{day}_month{month}")
    print("Results can be found in your local temp folder: {}".format(output_dir))
    if not os.path.exists(output_dir):
        #os.mkdir(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        
    timeseries_run_network(output_dir=output_dir, day=day, month=month)