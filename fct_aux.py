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


###############################################################################
#                       Constances : start
###############################################################################
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
#                       Constances : end
###############################################################################

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
    

  
def creer_dict_jours_timestamps():
    # Date de départ : 1er janvier 2023 à 00:00
    debut = datetime(2023, 1, 1)
    # Nombre total d'heures dans 2023 = 8760 (année non bissextile)
    nb_heures = 8760
    
    # Générer la liste complète des timestamps horaires
    timestamps = [debut + timedelta(hours=i) for i in range(nb_heures)]
    
    dict_jours = {}
    
    # Pour chaque jour de l'année 2023
    for i in range(365):  # 365 jours en 2023
        jour = (date(2023, 1, 1) + timedelta(days=i))
        # Extraire les timestamps correspondant à ce jour
        debut_jour = datetime.combine(jour, datetime.min.time())
        fin_jour = debut_jour + timedelta(days=1)
        ts_jour = [ts for ts in timestamps if debut_jour <= ts < fin_jour]
        dict_jours[jour] = ts_jour
    
    return dict_jours
    
###############################################################################
#                   create dico day/period timestamp : end
###############################################################################

