#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 21:30:06 2025

@author: wil

pvlib : generation of Power from solar 
"""
import pvlib

import random as rd
import fct_aux as aux
import pandas as pd


from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS

temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['sapm']['open_rack_glass_glass']

sandia_modules = pvlib.pvsystem.retrieve_sam('SandiaMod')
cec_inverters = pvlib.pvsystem.retrieve_sam('cecinverter')
sandia_module = sandia_modules['Canadian_Solar_CS5P_220M___2009_']
cec_inverter = cec_inverters['ABB__MICRO_0_25_I_OUTD_US_208__208V_']


def model_pv():
    location = Location(latitude=aux.LAT_BLAGNAC, longitude=aux.LON_BLAGNAC, altitude=aux.ALT_BLAGNAC, tz=aux.TIMEZONE)

    surface_tilt = rd.randint(a=aux.Surface_tilt_min, b=aux.Surface_tilt_max)
    surface_azimuth = rd.randint(a=aux.Surface_Azimuth_Min, b=aux.Surface_Azimuth_Max)
    system = PVSystem(surface_tilt=surface_tilt, surface_azimuth=surface_azimuth,
                      module_parameters=sandia_module,
                      inverter_parameters=cec_inverter,
                      temperature_model_parameters=temperature_model_parameters)
    
    mc = ModelChain(system, location)
    
    weather = aux.load_pv_data(pathfile=aux.PV_DATA2023, skiprows=12, year=aux.YEAR)
    weather['timezome'] = aux.TIMEZONE
    
    weather["datetime"] = pd.to_datetime(weather[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    # Ajout de la timezone (exemple fuseau -07:00)
    weather['datetime'] = weather['datetime'].dt.tz_localize(f'Etc/GMT+{aux.TIMEZONE}')  # note inversé par rapport à UTC-7
    # Mise en index de la colonne datetime
    weather = weather.set_index('datetime')
    
    weather = weather.rename(columns={"GHI":"ghi", "DHI":"dhi", "DNI":"dni"})
    
    cols = ['ghi', 'dhi', 'dni']
    weather[cols] = weather[cols].apply(pd.to_numeric, errors='coerce')
    return mc, weather

def generate_pv_ac():
    mc, weather = model_pv()
    mc.run_model(weather)
    df_ac = mc.results.ac 
    # value in Kwh
    df_ac /= pow(10,3)
    return df_ac
###############################################################################
#                   runtime
###############################################################################
if __name__ == '__main__':
    mc, weather = model_pv()
    df_ac = generate_pv_ac()
    pass
    
