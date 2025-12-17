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

from datetime import datetime, timedelta, date  
import calendar


###############################################################################
#                       Constances : start
###############################################################################
YEAR = 2023

LAT_BLAGNAC = 43.621
LON_BLAGNAC = 1.379
ALT_BLAGNAC = 150               # degre
TIMEZONE = 1
tz='Etc/GMT+1'
ELEC_NET = "./ELEC_NETWORK"
PV_DATA = "./PV_DATA"
RES_DATA = "./RES_DATA"
PLOT_DATA = os.path.join(RES_DATA, "PLOT_DATA")

JSONFILE_NETWORK = os.path.join(ELEC_NET, "76_MVFeeder2015.json")
PROFILES = os.path.join(ELEC_NET, "76_MVFeeder2015_load_profiles.parquet")

PV_DATA2023 = os.path.join(PV_DATA, "FRA_Toulouse-Blagnac_2023.csv")



###############################################################################
#                       Constances PVLIB : end
###############################################################################
# PVLIB
Surface_tilt_min = 30
Surface_tilt_max = 35
Surface_Azimuth_Min = 20
Surface_Azimuth_Max = 40

Surface_champSolaire = 15               # en m²
###############################################################################
#                   constances PVLIB : Debut
###############################################################################

###############################################################################
#                   constances PV : FIN
###############################################################################

###############################################################################
#                   constances Timestamp SUMME, HIVER, AUTUMN: Debut
###############################################################################

DICO_TS_HIVER = {"period_name":"HIVER", "month": 3, "day": 2, "hour": 13, "minute":30}
DICO_TS_SUMMER = {"period_name":"SUMMER", "month": 7, "day": 4, "hour": 10, "minute":30}
DICO_TS_AUTUMN = {"period_name":"AUTUMN", "month": 12, "day": 2, "hour": 13, "minute":30}

VAL_BUS_MAX_CHARGE = 1.5e-2 #0.00015
###############################################################################
#                   constances Timestamp SUMME, HIVER, AUTUMN : Fin
###############################################################################

###############################################################################
#                   load Data : start
###############################################################################
def load_pv_data(pathfile=PV_DATA2023, skiprows=12, year=YEAR):
    df_pv = pd.read_csv(pathfile, sep=',', skiprows=skiprows)
    df_pv.drop(0, inplace=True)
    df_pv['Year'] = YEAR
    
    return df_pv

def load_network(jsonfile=JSONFILE_NETWORK):
    net = pp.from_json(jsonfile)
    return net

def load_profiles(profiles_file=PROFILES):
    profiles = pd.read_parquet(PROFILES)
    return profiles
###############################################################################
#                   load Data : end
###############################################################################

###############################################################################
#                   create dico day/period timestamp : start
###############################################################################
def split_list(lst, nb_hr_day=24):
    return [lst[i:i + nb_hr_day] for i in range(0, len(lst), nb_hr_day)]

def create_day_timestamps(year:int, nb_hrs_yr:int=8760):
    """
    create a dictionary having day as key and values a dictionary with 2 keys 
    ts_min: the timestamp at 00 hour
    ts_max: the timestamp at 24 hour not included

    Parameters
    ----------
    year : int
        DESCRIPTION.
    nb_hrs_yr : int, optional
        DESCRIPTION. number of hours in one year. The default is 8760.

    Returns
    -------
    None.

    """
    # Date de départ : 1er janvier 2023 à 00:00
    debut = datetime(year, 1, 1)
    
    timestamps = range(0, nb_hrs_yr, 1)
    days_by_ts =  split_list(lst=timestamps, nb_hr_day=24)
    
    dico_days_ts = dict()
    for num_day in range(365):
        jour = (debut + timedelta(days=num_day))
        dico_days_ts[jour] = days_by_ts[num_day]
    
    return dico_days_ts

def create_mois_timestamps(year:int, nb_hrs_yr:int=8760):
    """
    create a dictionary having month as keys and values as list of min and 
    max timestamps.

    Parameters
    ----------
    year : int
        DESCRIPTION.
    nb_hrs_yr : int, optional
        DESCRIPTION. The default is 8760.

    Returns
    -------
    None.

    """
    ts_min = 0
    month_days = {}
    for mois in range(1, 13):
        nb_days = calendar.monthrange(year, mois)[1]
        name_month = calendar.month_name[mois].lower()
        
        nb_ts = nb_days * 24
        ts_max = ts_min + nb_ts
        month_days[name_month] = {"nb_days":nb_days, 
                                  name_month:range(ts_min, ts_max)}
        ts_min = ts_max
        
    return month_days
###############################################################################
#                   create dico day/period timestamp : end
###############################################################################

###############################################################################
#               extract sub-dataframes by row-block timestamp : start
###############################################################################
def extract_sub_dataframes(profiles: pd.DataFrame, ts_min:int, ts_max:int):
    """
    extract sub-dataframes by row-block timestamp

    Parameters
    ----------
    profiles : pd.DataFrame
        DESCRIPTION.
    ts_min : int
        DESCRIPTION.
    ts_max : int
        DESCRIPTION.

    Returns
    -------
    None.

    """
    return profiles.iloc[ts_min:ts_max+1, :].reset_index()
    
###############################################################################
#               extract sub-dataframes by row-block timestamp : end
###############################################################################