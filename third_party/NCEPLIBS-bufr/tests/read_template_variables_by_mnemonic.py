import sys

import ncepbufr
import numpy as np

# Things I see in the dump output - do we need these?
#   Pressure           POB MB PRESSURE OBSERVATION
#   Specific Humidity  QOB MG/KG SPECIFIC HUMIDITY OBSERVATION
#   Dewpoint           TDO DEG C DEWPOINT TEMPERATURE OBSERVATION (NOT ASSIMILATE
#   Temperature        TOB DEG C TEMPERATURE OBSERVATION
#   Height             ZOB METER HEIGHT OBSERVATION
#   XDR  DEG E PROFILE LEVEL LON (FOR RAOB/PIBAL BASED ON BALLO
#   YDR  DEG N PROFILE LEVEL LAT (FOR RAOB/PIBAL BASED ON BALLO
#   HRDR HOURS PROFILE LVL TIME-CYCLE (FOR RAOB/PIBAL, BASED ON
#   U-Wind  UOB M/S  U-COMPONENT WIND OBSERVATION
#   V-Wind  VOB M/S  V-COMPONENT WIND OBSERVATION
#   Wind Direction     DDO DEGREES TRUE WIND DIRECTION OBSERVATION (NOT ASSIMILATED)
#   Wind Speed         FFO KNOTS        WIND SPEED OBSERVATION (kts) (NOT ASSIMILATED)
# Do we need these BACKGROUND things?
#   QFC MG/KG FORECAST (BACKGROUND) SPECIFIC HUMIDITY VALUE
#   PFC MB FORECAST (BACKGROUND) PRESSURE VALUE
#   QFC MG/KG FORECAST (BACKGROUND) SPECIFIC HUMIDITY VALUE
#   TFC DEG C FORECAST (BACKGROUND) TEMPERATURE VALUE
#   ZFC METER FORECAST (BACKGROUND) HEIGHT VALUE
#   fcst_background_u_wind  UFC M/S  FORECAST (BACKGROUND) U-COMPONENT WIND VALUE
#   fcst_background_v-wind  VFC M/S  FORECAST (BACKGROUND) V-COMPONENT WIND VAL

# hdstr
# station  SID ( 8)CCITT IA5  STATION IDENTIFICATION
# lon  XOB DEG E  LONGITUDE
# lat  YOB DEG N  LATITUDE
# obs_t_sub_cycle_t DHR HOURS  OBSERVATION TIME MINUS CYCLE TIME
# rpt_obs_time  RPT  HOURS REPORTED OBSERVATION TIME
# elev  ELV METER  STATION ELEVATION
# type  TYP CODE TABLE  PREPBUFR REPORT TYPE
# inst_type  ITP CODE TABLE  INSTRUMENT TYPE
# rpt_type  T29 CODE TABLE  DATA DUMP REPORT TYPE
# tcpr TCOR INDICATOR WHETHER OBS. TIME IN "DHR" WAS CORRECT
# source  SAID  SOURCE OF REPORT

# pressure  POB MB PRESSURE OBSERVATION
# specific_humidity  QOB MG/KG SPECIFIC HUMIDITY OBSERVATION
# temperature  TOB DEG C TEMPERATURE OBSERVATION
# Dewpoint  TDO DEG C DEWPOINT TEMPERATURE OBSERVATION (NOT ASSIMILATE
# height  ZOB METER HEIGHT OBSERVATION
# U-Wind  UOB M/S  U-COMPONENT WIND OBSERVATION
# V-Wind  VOB M/S  V-COMPONENT WIND OBSERVATION
# fcst_background_u_wind  UFC M/S  FORECAST (BACKGROUND) U-COMPONENT WIND VALUE
# fcst_background_v-wind  VFC M/S  FORECAST (BACKGROUND) V-COMPONENT WIND VAL
# wind_direction     DDO DEGREES TRUE WIND DIRECTION OBSERVATION (NOT ASSIMILATED)
# wind_speed         FFO KNOTS        WIND SPEED OBSERVATION (kts) (NOT ASSIMILATED)
# obs_level_lon  XDR  DEG E PROFILE LEVEL LON (FOR RAOB/PIBAL BASED ON BALLO
# obs_level_lat  YDR  DEG N PROFILE LEVEL LAT (FOR RAOB/PIBAL BASED ON BALLO


if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <bufrfile> <mnemonic>")
    sys.exit(1)
bufr = ncepbufr.open(sys.argv[1])
mnemonic = sys.argv[2]

first_dump = True  # after first write, append to existing file.
verbose = False  # this produces more readable output.
while bufr.advance() == 0:  # loop over messages.
    if mnemonic == bufr.msg_type:
        vars = {
            "temperature":"TOB",
            "dewpoint":"TDO",
            "rh":"RHO",
            "specific_humidity":"QOB",
            "pressure":"POB",
            "height:":"ZOB",
            "wind_speed":"FFO",
            "U-Wind":"UOB",
            "V-Wind":"VOB",
            "wind_direction":"DDO",
        }
        while bufr.load_subset() == 0:  # loop over subsets in message.
            for _v in vars:
                val=bufr.read_subset(vars[_v]).squeeze()
                print(f"{_v}: {val}")
bufr.close()
