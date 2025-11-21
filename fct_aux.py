#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 21 13:22:24 2025

@author: wil
 fonctions auxiliaires
"""
import os
import pandas as pd
import pandapower as pp


YEAR = 2023

LAT_BLAGNAC = 43.621
LON_BLAGNAC = 1.379
ALT_BLAGNAC = 150               # degre
ELEC_NET = "./ELEC_NETWORK"
PV_DATA = "./PV_DATA"

JSON_LOAD_NX = os.path.join(ELEC_NET, "76_MVFeeder2015.json")
NETWORK = os.path.join(ELEC_NET, "76_MVFeeder2015_load_profiles.parquet")

PV_DATA2023 = os.path.join(PV_DATA, "FRA_Toulouse-Blagnac_2023.csv")


###############################################################################
#                   load Data : start
###############################################################################
def load_pv_data(pathfile=PV_DATA2023, skiprows=12, year=YEAR):
    df_pv = pd.read_csv(pathfile, sep=',', skiprows=skiprows)
    df_pv.drop(0, inplace=True)
    df_pv['Year'] = YEAR
    
    return df_pv

def load_network(jsonfile=JSON_LOAD_NX):
    net = pp.from_json(jsonfile)
    return net


###############################################################################
#                   load Data : end
###############################################################################

###############################################################################
#                   create dico day/period timestamp : start
###############################################################################
def create_day_timestamps(year:int, n_ts:int=8760):
    """
    create a dictionary having day as key and values a dictionary with 2 keys 
    ts_min: the timestamp at 00 hour
    ts_max: the timestamp at 24 hour not included

    Parameters
    ----------
    year : int
        DESCRIPTION.
    n_ts : int, optional
        DESCRIPTION. The default is 8760.

    Returns
    -------
    None.

    """
    
    
###############################################################################
#                   create dico day/period timestamp : end
###############################################################################

