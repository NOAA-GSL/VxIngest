#!/usr/bin/env python3
"""Define constants."""
from collections import OrderedDict

# name to use for a group when no group tag is included in load_spec
DEFAULT_DATABASE_GROUP = "NO GROUP"

# Maximum number of columns
MAX_COL = 120

MYSQL = "mysql"
MARIADB = "mariadb"
AURORA = "aurora"
RELATIONAL = [MYSQL, MARIADB, AURORA]
CB = "cb"

# miscellaneous CB schema related constants
ID = "id"
TYPE = "type"
DATATYPE = "dataType"
SUBSET = "subset"
DATAFILE_ID = "dataFile_id"
DATASOURCE_ID = "datasource_id"
GEOLOCATION_ID = "geoLocation_id"
DATA = "data"

# used for date conversions
TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TS_VSDB_FORMAT = "%Y%m%d%H"

# default port for MySQL
SQL_PORT = 3306

# Lower Case true and false
LC_TRUE = "true"
LC_FALSE = "false"

# separator for csv files
SEP = '$'

# Equal Sign
EQS = '='

# Forward Slash
FWD_SLASH = '/'

# Underscore
U_SCORE = '_'

# Left paren for searching
L_PAREN = '('

# Right paren for searching
R_PAREN = ')'

# Triple zero for tests for MODE files
T_ZERO = '000'

# Characters expected in dates
DATE_CHARS = set('yYmMdDhHsSz')

# Substitutions, Java date format to Python
DATE_SUBS = OrderedDict([('yyyy', '%Y'),
                         ('yy', '%y'),
                         ('MM', '%m'),
                         ('dd', '%d'),
                         ('hh', '%I'),
                         ('HH', '%H'),
                         ('mm', '%M'),
                         ('SSS', '%f'),
                         ('ss', '%S'),
                         ('z', '%z'),
                         ('D', '%j')])

# Generic count of variable fields
N_VAR = 'n_var'

# STAT line types - comments from the v8.1.1 MET user's guide
FHO = "FHO"        # Forecast, Hit, Observation Rates
CTC = "CTC"        # Contingency Table Counts
CTS = "CTS"        # Contingency Table Statistics
MCTC = "MCTC"      # Multi-category Contingency Table Counts
MCTS = "MCTS"      # Multi-category Contingency Table Statistics
CNT = "CNT"        # Continuous Statistics
SL1L2 = "SL1L2"    # Scalar L1L2 Partial Sums
SAL1L2 = "SAL1L2"  # Scalar Anomaly L1L2 Partial Sums when climatological data is supplied
VL1L2 = "VL1L2"    # Vector L1L2 Partial Sums
VAL1L2 = "VAL1L2"  # Vector Anomaly L1L2 Partial Sums when climatological data is supplied
PCT = "PCT"        # Contingency Table Counts for Probabilistic Forecasts
PSTD = "PSTD"      # Contingency Table Stats for Probabilistic Forecasts with Dichotomous outcomes
PJC = "PJC"        # Joint and Conditional Factorization for Probabilistic Forecasts
PRC = "PRC"        # Receiver Operating Characteristic for Probabilistic Forecasts
ECLV = "ECLV"      # Economic Cost/Loss Value derived from CTC and PCT lines
MPR = "MPR"        # Matched Pair Data
NBRCTC = "NBRCTC"  # Neighborhood Contingency Table Counts
NBRCTS = "NBRCTS"  # Neighborhood Contingency Table Statistics
NBRCNT = "NBRCNT"  # Neighborhood Continuous Statistics
ISC = "ISC"        # Intensity-Scale
RHIST = "RHIST"    # Ranked Histogram
PHIST = "PHIST"    # Probability Integral Transform Histogram
ORANK = "ORANK"    # Observation Rank
SSVAR = "SSVAR"    # Spread Skill Variance
GRAD = "GRAD"      # Gradient statistics (S1 score)
VCNT = "VCNT"      # Vector Continuous Statistics
RELP = "RELP"      # Relative Position
ECNT = "ECNT"      # Ensemble Continuous Statistics - only for HiRA
ENSCNT = "ENSCNT"  #
PERC = "PERC"      #
DMAP = "DMAP"      # Distance Map
RPS = "RPS"        # Ranked Probability Score

# VSDB line types
BSS = "BSS"        # same as PSTD
RELI = "RELI"      # same as PCT
HIST = "HIST"      # same as RHIST
ECON = "ECON"      # same as ECLV
RMSE = "RMSE"      # same as CNT
FSS = "FSS"        # same as NBRCNT

UC_LINE_TYPES = [FHO, CTC, CTS, MCTC, MCTS, CNT, SL1L2, SAL1L2, VL1L2, VAL1L2,
                 PCT, PSTD, PJC, PRC, ECLV, MPR, NBRCTC, NBRCTS, NBRCNT, ISC,
                 RHIST, PHIST, ORANK, SSVAR, GRAD, VCNT, RELP, ECNT, ENSCNT, PERC,
                 DMAP, RPS]

LC_LINE_TYPES = [ltype.lower() for ltype in UC_LINE_TYPES]

LINE_TABLES = ['line_data_' + hname for hname in LC_LINE_TYPES]

ALPHA_LINE_TYPES = [CTS, NBRCTS, NBRCNT, MCTS, SSVAR, VCNT, DMAP, RPS, CNT, PSTD]

COV_THRESH_LINE_TYPES = [NBRCTC, NBRCTS, PCT, PSTD, PJC, PRC]

VAR_LINE_TYPES = [PCT, PSTD, PJC, PRC, MCTC, RHIST, PHIST, RELP, ORANK, ECLV]

OLD_VSDB_LINE_TYPES = [BSS, ECON, HIST, RELI, RMSE, RPS, FHO, FSS]

VSDB_TO_STAT_TYPES = [PSTD, ECLV, RHIST, PCT, CNT, ENSCNT, CTC, NBRCNT]

ENS_VSDB_LINE_TYPES = [BSS, ECON, HIST, RELI, RELP, RMSE, RPS]

ALL_VSDB_LINE_TYPES = OLD_VSDB_LINE_TYPES + [RELP, SL1L2, SAL1L2, VL1L2, VAL1L2, GRAD]

# column names
# MET column names are UC, SQL are LC
UC_DESC = "DESC"
UC_FCST_UNITS = "FCST_UNITS"

VERSION = "version"
MODEL = "model"
# MET file contains DESC. SQL field name is descr
DESCR = "descr"
FCST_LEAD = "fcst_lead"
FCST_VALID_BEG = "fcst_valid_beg"
FCST_VALID_END = "fcst_valid_end"
FCST_INIT_EPOCH = "fcst_init_epoch"
FCST_VALID_EPOCH = "fcst_valid_epoch"
OBS_LEAD = "obs_lead"
OBS_VALID_BEG = "obs_valid_beg"
OBS_VALID_END = "obs_valid_end"
FCST_VAR = "fcst_var"
FCST_UNITS = "fcst_units"
FCST_LEV = "fcst_lev"
OBS_VAR = "obs_var"
OBS_UNITS = "obs_units"
OBS_LEV = "obs_lev"
OBTYPE = "obtype"
VX_MASK = "vx_mask"
INTERP_MTHD = "interp_mthd"
INTERP_PNTS = "interp_pnts"
FCST_THRESH = "fcst_thresh"
OBS_THRESH = "obs_thresh"
COV_THRESH = "cov_thresh"
ALPHA = "alpha"
LINE_TYPE = "line_type"
FCST_INIT_BEG = "fcst_init_beg"

FCST_LEAD_HR = "fcst_lead_hr"

# After units added in MET 8.1
LONG_HEADER = [VERSION, MODEL, DESCR, FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END,
               OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END, FCST_VAR, FCST_UNITS, FCST_LEV,
               OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK, INTERP_MTHD, INTERP_PNTS,
               FCST_THRESH, OBS_THRESH, COV_THRESH, ALPHA, LINE_TYPE]

# Contains DESC but not UNITS
MID_HEADER = LONG_HEADER[0:10] + LONG_HEADER[11:13] + LONG_HEADER[14:]

# No DESC and no UNITS
SHORT_HEADER = MID_HEADER[0:2] + MID_HEADER[3:]

STAT_HEADER_KEYS = [VERSION, MODEL, DESCR, FCST_VAR, FCST_UNITS, FCST_LEV,
                    OBS_VAR, OBS_UNITS, OBS_LEV, OBTYPE, VX_MASK,
                    INTERP_MTHD, INTERP_PNTS, FCST_THRESH, OBS_THRESH]

VSDB_HEADER = [VERSION, MODEL, FCST_LEAD, FCST_VALID_BEG, OBTYPE,
               VX_MASK, LINE_TYPE, FCST_VAR, FCST_LEV]

STAT_HEADER = 'stat_header'
STAT_HEADER_ID = 'stat_header_id'
LINE_DATA_ID = 'line_data_id'
LINE_NUM = 'line_num'
TOTAL_LC = 'total'
FCST_PERC = 'fcst_perc'
OBS_PERC = 'obs_perc'

DATA_FILE = 'data_file'
FULL_FILE = 'full_file'
DATA_FILE_ID = 'data_file_id'
DATA_FILE_LU_ID = 'data_file_lu_id'
FILE_ROW = 'file_row'
FILENAME = 'filename'
FILEPATH = 'path'
LOAD_DATE = 'load_date'
MOD_DATE = 'mod_date'
FY_OY = 'fy_oy'
FY_ON = 'fy_on'
FN_OY = 'fn_oy'
FN_ON = 'fn_on'
BASER = 'baser'
FMEAN = 'fmean'

INSTANCE_INFO = 'instance_info'
INSTANCE_INFO_ID = 'instance_info_id'

DATA_FILE_FIELDS = [DATA_FILE_ID, DATA_FILE_LU_ID, FILENAME, FILEPATH,
                    LOAD_DATE, MOD_DATE]

STAT_HEADER_FIELDS = [STAT_HEADER_ID] + STAT_HEADER_KEYS

ALL_LINE_DATA_FIELDS = [STAT_HEADER_ID, DATA_FILE_ID, LINE_NUM,
                        FCST_LEAD, FCST_VALID_BEG, FCST_VALID_END, FCST_INIT_BEG,
                        OBS_LEAD, OBS_VALID_BEG, OBS_VALID_END]

TOT_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [TOTAL_LC]

ALPH_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [ALPHA, TOTAL_LC]

COV_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, TOTAL_LC]

COVA_LINE_DATA_FIELDS = ALL_LINE_DATA_FIELDS + [COV_THRESH, ALPHA, TOTAL_LC]


