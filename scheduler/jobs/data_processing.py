import pandas as pd
import os
import sys
from datetime import date, datetime, timedelta
import time
from dotenv import load_dotenv
from pymongo import InsertOne, DeleteMany
from jobs.lib.utils.logger import setup_logging
from jobs.lib.utils.mongo_wrapper import MongoWrapper
from jobs.lib.utils.mysql_wrapper import MysqlWrapper
import argparse
import logging
import re



#load configuration from .env
load_dotenv()
#get logger instance
setup_logging()
logger = logging.getLogger('data_processing_etl')


#configuration & definitions
TICKETING_INPUT_FOLDER = os.path.join(os.getcwd(), os.getenv('INPUT_FOLDER'), 'historical')   #folder where input UPLIFT files should be
PREDEPARTURE_INPUT_FOLDER = os.path.join(os.getcwd(), os.getenv('INPUT_FOLDER'), 'forward')   #folder where input AMELIA files should be
DICTIONARY_FOLDER = os.path.join(os.getcwd(), os.getenv('DICTIONARY_FOLDER'))   #folder to store static data(e.g. locations, airports, etc)
OUTPUT_FOLDER = os.path.join(os.getcwd(), os.getenv('OUTPUT_FOLDER'))
TEMP_FOLDER = os.path.join(os.getcwd(), os.getenv('TEMPORARY_FOLDER'))

#those are the columns that are going to be loaded into DB (mysql/mongo)
OUTPUT_COLUMNS = ['pnr_idx', 'fare_basis', 'ticket_type', 'agency_id', 'agency_name', 'agency_city', 'agency_country',
        'agency_continent', 'country_of_sale',
        'distribution_channel', 'orig_code', 'stop_1', 'dest_code', 'days_sold_prior_to_travel', 'seg_class',
        'rbkd', 'op_flt_num',
        'equip', 'pax', 'duration', 'dom_op_al_code', 'blended_fare', 'blended_rev', 'travel_date', 'travel_month',
        'travel_year',
        'travel_day_of_week', 'is_direct', 'bound', 'cos_norm', 'is_group', 'local_dep_time', 'dep_time_block',
        'prev_dest', 'next_dest',
        'is_ticket', 'is_historical']

#### COMMON FUNCTIONS USED IN PRE & POST departure data processing

def clean_temporary_folder():
    pass

def store_dataframe_in_temporary_file(df, filename):
    """Sometimes it's useful to store temporary files for debugging purposes.
    This function stores dataframe as CSV file in a temporary folder"""
    if args.tempfiles == True:
        tmp_filename = os.path.join(TEMP_FOLDER, filename)
        df.to_csv(tmp_filename)

def upload_data_to_db(input_filename, snapshot_date:date):
    """Insert data to mongo and mysql, add insertion timestamp to each record, perform required conversion for mongo"""
    df = pd.read_csv(os.path.join(OUTPUT_FOLDER,input_filename))[list(OUTPUT_COLUMNS)]
    df.reset_index(drop=True)
    insert_timestamp=time.strftime('%Y%m%d%H%M%S')
    logger.info(f"Insert timestamp:{insert_timestamp}, will be added to every record added to a database, dryrun={args.dryrun}")
    #add timestemp to each record so that it's easier to identify records which were added (e.g. in case of failure we will know which should be removed)
    df['insert_timestamp']=[insert_timestamp]*len(df)
    OUTPUT_COLUMNS.append('insert_timestamp')
    logger.debug(f"Inserting data to MYSQL, number of records to load:{len(df)}")
    _upload_data_to_mysql(df)
    logger.debug(f"Inserting data to Mongo, number of records to load:{len(df)}")
    _upload_data_to_mongo(df)
    logger.info("....Done")

def _upload_data_to_mysql(df):
    """Upload dataframe into mysql (if dryrun = false) """
    if args.dryrun == False:  # only store data in DB if dryrun argument is false
        query=f"INSERT INTO {MysqlWrapper.TABLE_DDS} ({','.join(OUTPUT_COLUMNS)}) values ({','.join(['%s' for x in OUTPUT_COLUMNS])})"
        mysql = MysqlWrapper()
        cnx = mysql.get_connection()
        buffer=[]
        batch_size=10000
        cnx.start_transaction()
        cursor = cnx.cursor()
        start_time = time.time()
        total_count=0

        for index, row in df.iterrows():
            buffer.append(tuple(row))
            if len(buffer)>=batch_size:
                logger.debug(f"Execute mysql insert batch, buffer size:{len(buffer)}")
                cursor.executemany(query, buffer)
                total_count+=len(buffer)
                logger.info(f"Progress: {total_count}/{len(df)}")
                buffer=[]

        if len(buffer)>0:
            logger.debug(f"Execute last mysql batch, buffer size:{len(buffer)}")
            cursor.executemany(query, buffer)
            total_count += len(buffer)
            logger.info(f"Progress: {total_count}/{len(df)}")

        cnx.commit()
        logger.debug("--- %s seconds ---" % (time.time() - start_time))
    else:
        logger.info("Dryrun mode is ON - no data was stored in database")
    store_dataframe_in_temporary_file(df, 'data_added_to_mysql.csv')

def _upload_data_to_mongo(df):
    """Upload data to mongo (only if dryrun == false). It also performs some minor data conversions to MSDv2 expected formats (e.g. 00date format conversion from YYYY-MM-DD to YYYYMMDD)"""

    #convert 0,1 fields to bool (false, true)
    df['is_group'] = df['is_group'].astype(bool)
    df['is_direct'] = df['is_direct'].astype(bool)
    df['is_ticket'] = df['is_ticket'].astype(bool)
    df['is_historical'] = df['is_historical'].astype(bool)

    def calc_sell_date(x):
        return pd.to_datetime(x['travel_date'], format="%Y-%m-%d") - timedelta(days=int(x['days_sold_prior_to_travel']))

    #calculate sell date/month/year (needed for KPI calculations)
    df['sell_date'] = df.apply(lambda x: int(calc_sell_date(x).strftime("%Y%m%d")), axis=1)
    df['sell_month'] = df.apply(lambda x: calc_sell_date(x).month, axis=1)
    df['sell_year'] = df.apply(lambda x: calc_sell_date(x).year, axis=1)
    #below is needed to add those columns to mongo
    OUTPUT_COLUMNS.append('sell_date')
    OUTPUT_COLUMNS.append('sell_year')
    OUTPUT_COLUMNS.append('sell_month')

    #convert travel_date from 'YYYY-MM-DD' to YYYYMMDD
    df['travel_date'] = df.apply(lambda x: int(pd.to_datetime(x['travel_date'], format="%Y-%m-%d").strftime("%Y%m%d")), axis=1)

    if args.dryrun == False:  # only store data in DB if dryrun argument is false
        mongo = MongoWrapper()
        buffer=[]
        batch_size=10000    #size of batch (higher = faster)
        start_time = time.time()
        total_count = 0
        for index, row in df.iterrows():
            rec = {}
            for key in OUTPUT_COLUMNS:
                rec[key] = row[key]
            buffer.append(InsertOne(rec))
            if len(buffer)>=batch_size:
                logger.debug(f"Execute mongo batch, buffer size:{len(buffer)}")
                mongo.col_dds().bulk_write(buffer)
                total_count+=len(buffer)
                logger.info(f"Progress: {total_count}/{len(df)}")
                buffer=[]

        if len(buffer) > 0:
            logger.debug(f"Execute last mongo batch, buffer size:{len(buffer)}")
            mongo.col_dds().bulk_write(buffer)
            total_count += len(buffer)
            logger.info(f"Progress: {total_count}/{len(df)}")

        logger.debug("--- %s seconds ---" % (time.time() - start_time))
    else:
        logger.info("Dryrun mode is ON - no data was stored in database")
    store_dataframe_in_temporary_file(df, 'data_added_to_mongo.csv')

def delete_outdated_forward_data_from_db(snapshot_date:date):
    """When new batch of historical data is loaded into database, we should remove forward looking data that was already in the DB (but only for departure dates older than 'snapshot_date'"""
    logger.info(f"Deleting forward looking data from database where travel_date < {snapshot_date}, dryrun={args.dryrun}")
    if args.dryrun == False:
        #delete data from mysql
        mysql_query = f"DELETE FROM {MysqlWrapper.TABLE_DDS} WHERE is_historical = 0 and travel_date<%s"
        mysql = MysqlWrapper()
        cnx = mysql.get_connection()
        cnx.start_transaction()
        cursor = cnx.cursor()
        cursor.execute(mysql_query,[snapshot_date])
        cnx.commit()

        #delete data in mongo
        travel_date=int(snapshot_date.strftime("%Y%m%d"))
        mongo = MongoWrapper()
        mongo.col_dds().delete_many({'is_historical':False, 'travel_date':{'$lt':travel_date}})
    else:
        logger.info("Dryrun mode is ON - no data was deleted from database")

def delete_all_forward_data_from_db():
    """When new batch of future data is loaded into database, we should remove all forward looking data before loading new batch"""
    logger.info(f"Deleting all forward looking data from database, dryrun={args.dryrun}")
    if args.dryrun == False:
        #delete data from mysql
        mysql_query = f"DELETE FROM {MysqlWrapper.TABLE_DDS} WHERE is_historical = 0"
        mysql = MysqlWrapper()
        cnx = mysql.get_connection()
        cnx.start_transaction()
        cursor = cnx.cursor()
        cursor.execute(mysql_query)
        cnx.commit()

        #delete data in mongo
        mongo = MongoWrapper()
        mongo.col_dds().delete_many({'is_historical':False})
    else:
        logger.info("Dryrun mode is ON - no data was deleted from database")


########## historical data processing (loading, cleaning, processing, storing)

def normalize_country_of_sale_historical(df):
    locs = pd.read_csv(os.path.join(DICTIONARY_FOLDER,
                                    'locations.csv'))  ## locations file that gives country name for each airport code (,IATA,Latitude,Longitude,Timezone,country_full,City,Continent,Country)
    loc_dict = dict(zip(locs['IATA'], locs['country_full']))
    df['country_of_sale'] = df['agency_market'].apply(lambda x: 'Guyana' if x == 'FR-GUYANA'
    else 'United Kingdom' if x == 'GREAT BRITAIN'
    else 'United States of America' if x == 'USA'
    else 'Misc' if x == 'MISCELLANEOUS'
    else x.title())

    def get_cos_norm(cos, orig, dest):
        currs = [loc_dict[orig], loc_dict[dest]]
        if cos in currs:
            return cos
        else:
            return 'Others'

    df['cos_norm'] = df[['country_of_sale', 'orig_code', 'dest_code']].apply(
        lambda x: get_cos_norm(x['country_of_sale'], x['orig_code'], x['dest_code']), axis=1)

    return df


def fill_equipment_code(df):
    """map aircraft registration number to equipment code"""
    # (aircraft_registration,equip)
    eqps = pd.read_csv(os.path.join(DICTIONARY_FOLDER, "aircraft_types.csv"))
    df = df.merge(eqps, on='aircraft_registration', how='left')
    df['equip'] = df['equip'].fillna('-')
    return df


def normalize_distribution_channel(df):
    ",raw_dist_channel,distribution_channel"
    dist_channel_conv = pd.read_csv(os.path.join(DICTIONARY_FOLDER, 'distribution_channel_correction.csv'))
    df = df.rename(columns={'distribution_channel': 'raw_dist_channel'}).merge(dist_channel_conv, on='raw_dist_channel')
    return df


def normalize_agency_names(df):
    "agency_id,agency_name,corr_agency_name"
    agency_conv = pd.read_csv(os.path.join(DICTIONARY_FOLDER, 'agency_name_correction.csv'))
    agency_corr = df[['agency_id', 'agency_name']].drop_duplicates().merge(agency_conv, on=['agency_id', 'agency_name'],
                                                                           how='left')
    agency_corr['final_agency_name'] = agency_corr.apply(
        lambda x: x['corr_agency_name'] if not pd.isna(x['corr_agency_name']) else x['agency_name'], axis=1)
    df = df.merge(
        agency_corr[['agency_id', 'agency_name', 'final_agency_name']],
        on=['agency_id', 'agency_name']
    )
    del df['agency_name']
    df = df.rename(columns={'final_agency_name': 'agency_name'})
    return df

def load_ticketing_data():
    """Load UPLIFT data to be processed, rename columns to desired names"""
    cols = {
        'FTDA': 'travel_date',
        'ORAC': 'orig_code',
        'DSTC': 'dest_code',
        'CARR': 'dom_op_al_code',
        'FTNR': 'flt_num',
        'FARE_TYPE': 'type_pax',
        'PNRR': 'pnr',
        'NPAX': 'pax',
        'DAIS': 'purchase_date',
        'EMIS_CUTP': 'payment_currency',
        'CUTP_TO_EUR': 'payment_curr_to_eur',
        'AGTN': 'agency_id',
        'AGTN_NAME': 'agency_name',
        'MARK': 'agency_market',
        'CPVL': 'fare',
        'STPO': 'stopover',  # * transit, O stopover
        'PLANNED_FTDA': 'planned_travel_date',
        'PLANNED_FTNR': 'planned_op_flt_num',
        'RBKD': 'rbkd',
        'FBTD': 'fare_basis',
        'CABC': 'seg_class',
        'OCFT': 'op_flt_num',
        'SRCI': 'distribution_channel',
        'PXNM': 'pax_name',
        'TACN': 'ticketing_airline_code',
        'ICNX': 'prev_dest',
        'OCNX': 'next_dest',
        'MUSN': 'multiple_indicator',
        'IMMA': 'aircraft_registration',
        'RPSI': 'reporting_system_id',
        'FLTP': 'flight_type',
        'CSHR_BLNG': 'codeshare',
        'RVNR_FARE': 'rev_flag',
        'YR_CUTP': 'yr',
        'YQ_CUTP': 'yq'
    }
    logger.debug(f"Loading files from folder:{TICKETING_INPUT_FOLDER}")

    regex = re.compile('(.*csv$)|(.*xlsx$)')
    dfs = []
    for file in os.listdir(TICKETING_INPUT_FOLDER):
        if regex.match(file):
            basename, extension = os.path.splitext(file)
            logger.debug(f"Loading file:{file}, extension:{extension}")
            if extension==".csv":
                df = pd.read_csv(os.path.join(TICKETING_INPUT_FOLDER, file), skiprows=1, sep=';')[list(cols.keys())]
            elif extension==".xlsx":
                df = pd.read_excel(os.path.join(TICKETING_INPUT_FOLDER, file), skiprows=1, usecols=list(cols.keys()))
            else:
                logger.error(f'Unknown file extension: {extension}, file:{file}')
            df = df.rename(columns=cols)
            dfs.append(df)
        else:
            logger.info(f"Ignoring file:{file}, does not match regex")

    dfs=pd.concat(dfs, axis=0).reset_index(drop=True)
    store_dataframe_in_temporary_file(dfs, 'historical_merged_raw.csv')
    return dfs

def historical_process(output_filename, snapshot_date:date):
    #load input data
    logger.info("Loading historical data")
    df = load_ticketing_data()
    logger.debug(f"Loaded total records:{len(df)}")
    def parse_date(x):
        # print(f"type={type(x)}, x={x}")
        if type(x) is pd.Timestamp:
            dt_obj=x.date()
        elif x.find(':')>-1:
            dt_obj = datetime.strptime(x, "%d/%m/%Y %H:%M").date()
        elif x.find('/')>-1:
            dt_obj = datetime.strptime(x, "%d/%m/%Y").date()
        else:
            dt_obj = datetime.strptime(x, "%Y-%m-%d").date()
        return dt_obj

    #preprocess
    # df['travel_date'] = pd.to_datetime(df['travel_date'], format="%d/%m/%Y").apply(lambda x: x.date())
    # df['purchase_date'] = pd.to_datetime(df['purchase_date'], format="%d/%m/%Y").apply(lambda x: x.date())
    df = df.query("pnr != 'NO ADD'")\
        .dropna(subset=['pnr']).sort_values(by='multiple_indicator', ascending=True)\
        .drop_duplicates(subset=['pnr', 'pax_name', 'orig_code', 'dest_code'], keep='last')\
        .reset_index(drop=True)
    df['travel_date'] = df.apply(lambda x:parse_date(x['travel_date']), axis=1)
    df['purchase_date'] = df.apply(lambda x:parse_date(x['purchase_date']), axis=1)

    
    df['travel_date'] = pd.Series(pd.DatetimeIndex(df['travel_date']).normalize()).apply(lambda x: x.date())
    df['purchase_date'] = pd.Series(pd.DatetimeIndex(df['purchase_date']).normalize()).apply(lambda x: x.date())
    #calculate days_sold_prior_to_travel
    df['days_sold_prior_to_travel'] = df.apply(lambda x: (x['travel_date'] - x['purchase_date']).days, axis=1)
    df['travel_day_of_week'] = df['travel_date'].apply(lambda x: x.weekday() + 1)
    df['travel_year'] = df['travel_date'].apply(lambda x: x.year)
    df['travel_month'] = df['travel_date'].apply(lambda x: x.month)
    df['itin'] = df['orig_code'] + '-' + df['dest_code']
    df['blended_fare'] = (df['fare'] + df['yq'] + df['yr']) * df['payment_curr_to_eur']
    df['blended_rev'] = df['blended_fare'] * df['pax']

    is_group_df = df.groupby("pnr").agg({'pax_name': 'nunique'}).reset_index()
    is_group_df['is_group'] = (is_group_df['pax_name'] >= 9).astype(int)
    is_group_df = is_group_df[['pnr', 'is_group']]

    df['local_dep_time'] = ['-'] * len(df)
    df['dep_time_block'] = ['-'] * len(df)
    df['agency_country'] = df['agency_market']
    df['agency_city'] = ['-'] * len(df)
    df['agency_continent'] = ['-'] * len(df)
    df['duration'] = [100] * len(df)
    df['stop_1'] = ['-'] * len(df)
    df['prev_dest'] = df['prev_dest'].replace('*', '-')
    df['next_dest'] = df['next_dest'].replace('*', '-')
    df['bound'] = df[['prev_dest', 'next_dest']].apply(lambda x:
                                                       'both' if x['prev_dest'] != '-' and x['next_dest'] != '-'
                                                       else 'inbound' if x['prev_dest'] != '-' and x['next_dest'] == '-'
                                                       else 'outbound' if x['next_dest'] != '-' and x[
                                                           'prev_dest'] == '-'
                                                       else 'other', axis=1)
    df['is_direct'] = df['bound'].apply(lambda x: x == 'other').astype(int)
    df['op_flt_num'] = 'PY' + df['op_flt_num'].astype(str)
    #recognize type of a ticket (oneway vs roundtrip vs openjaw)
    ttype_dict = {}
    for g, g_df in df.groupby(["pnr", "pax_name"]):
        g_df = g_df.sort_values(by=['travel_date', 'prev_dest'], ascending=True)
        if g_df.itin.nunique() > 1:
            curr_itins = list(set([e for el in g_df.itin.unique().tolist() for e in el.split('-')]))
            if len(curr_itins) == g_df.shape[0] and g_df.iloc[0].orig_code == g_df.iloc[-1].dest_code:
                ttype_dict[g] = 'Round Trip'
            else:
                if g_df.shape[0] == 2 and g_df.orig_code.iloc[0] == g_df.dest_code.iloc[1]:
                    ttype_dict[g] = 'Open Jaw'
                elif g_df.orig_code.iloc[0] != g_df.dest_code.iloc[1]:
                    ttype_dict[g] = 'Complex'
                else:
                    ttype_dict[g] = 'Complex'
        else:
            ttype_dict[g] = 'One Way'

    df['ticket_type'] = df[['pnr', 'pax_name']].apply(lambda x: ttype_dict[(x['pnr'], x['pax_name'])]
    if (x['pnr'], x['pax_name']) in ttype_dict else 'One Way', axis=1)

    df['is_ticket'] = [1] * len(df)
    df['is_historical'] = [1] * len(df)

    df = df.merge(is_group_df, on='pnr')

    df['pax_name'] = df['pax_name'].fillna('UNK')

    pnr_df = df[['pnr', 'pax_name']].drop_duplicates()
    id2pnr = dict(enumerate(pnr_df.pnr.unique().tolist()))
    pnr2id = {v: k for k, v in id2pnr.items()}

    pnr_df['pnr_id'] = pnr_df['pnr'].map(pnr2id)
    pnr_df['name_id'] = pnr_df.groupby("pnr")['pax_name'].rank(method='dense').astype(int)
    pnr_df['pnr_idx'] = pnr_df['pnr_id'].astype(str) + '-' + pnr_df['name_id'].astype(str)
    pnr_df = pnr_df[['pnr', 'pax_name', 'pnr_idx']]

    df = df.merge(pnr_df, on=['pnr', 'pax_name'])
    store_dataframe_in_temporary_file(df, 'historical_processed_before_normalisation.csv')
    df=fill_equipment_code(df)
    df=normalize_country_of_sale_historical(df)
    df=normalize_distribution_channel(df)
    df=normalize_agency_names(df)
    output_filename = os.path.join(os.getenv('OUTPUT_FOLDER'), output_filename)
    logger.info(f"Storing processed historical data in file: {output_filename}")
    logger.info(f"Columns: {OUTPUT_COLUMNS}")
    store_dataframe_in_temporary_file(df, 'historical_processed_after_normalisation.csv')
    df[OUTPUT_COLUMNS].to_csv(output_filename)



### PREDEPARTURE DATA PROCESSING
def load_predeparture_data():
    """Load AMELIA data to be processed, rename columns to desired names"""
    amelia_cols = {
        'Agency': 'agency_name',
        'Reservation': 'pnr',
        'Passenger': 'passenger_name',
        'strLEGS': 'travel_date',
        'textbox106': 'orig_to_dest',
        'FareClass': 'fare_basis',
        'dtm_res_Booking_Date': 'purchase_date',
        'textbox107': 'blended_fare'
    }
    logger.debug("Loading files from folder:{PREDEPARTURE_INPUT_FOLDER}")
    regex = re.compile('(.*csv$)')

    dfs = []
    for file in os.listdir(PREDEPARTURE_INPUT_FOLDER):
        if regex.match(file):
            logger.debug(f"Loading file:{file}")
            df = pd.read_csv(os.path.join(PREDEPARTURE_INPUT_FOLDER, file))[list(amelia_cols.keys())]
            dfs.append(df)
        else:
            logger.info(f"Ignoring file:{file}, does not match regex")

    dfs = pd.concat(dfs, axis=0).reset_index(drop=True)  # .to_csv("C:/Users/deniz/Desktop/py_raw_comb_0726.csv")
    store_dataframe_in_temporary_file(dfs,'forward_merged_raw.csv')
    return dfs



def normalize_country_of_sale_predeparture(df):
    locs = pd.read_csv(os.path.join(DICTIONARY_FOLDER,
                                    'locations.csv'))  ## locations file that gives country name for each airport code (,IATA,Latitude,Longitude,Timezone,country_full,City,Continent,Country)
    loc_dict = dict(zip(locs['IATA'], locs['country_full']))

    def get_cos_norm(cos, orig, dest):
        currs = [loc_dict[orig], loc_dict[dest]]
        if cos in currs:
            return cos
        else:
            return 'Others'

    df['cos_norm'] = df[['country_of_sale','orig_code','dest_code']].apply(lambda x: get_cos_norm(x['country_of_sale'], x['orig_code'], x['dest_code']), axis=1)

    return df


def forward_processing(output_filename, snapshot_date:date):
    logger.info("Loading future data")

    df = load_predeparture_data()
    logger.debug(f"Loaded total records:{len(df)}")
    str_to_code = {
        'Cayenne': 'CAY',
        'Paramaribo': 'PBM',
        'Georgetown': 'GEO',
        'Augusto c Sandino': 'MGA',
        'OHare Intl Airport': 'ORD',
        'toussaint louverture': 'PAP',
        'Miami': 'MIA',
        'Curacao': 'CUR',
        'Belem': 'BEL',
        'Aruba': 'AUA',
        'Port of Spain': 'POS',
        'Amsterdam': 'AMS',
        'Las Américas International Airport': 'SDQ',
        'Princess Juliana International Airport': 'SXM',
        'Sarasota Bradenton International Airport': 'SRQ'
    }

    df['agency_name'] = df['Agency'].apply(lambda x: x.split(':')[1].strip())
    df['pnr'] = df['Reservation'].apply(lambda x: x.split(':')[-1].strip())
    df = df.query("strLEGS != '#Error' and pnr != 'NO ADD'").dropna(subset=['FareClass'])
    df['pax_name'] = df['Passenger'].apply(lambda x: x.split(':')[1].strip())
    df['travel_date'] = pd.to_datetime(df['strLEGS'], format="%b %d, %Y").apply(lambda x: x.date())
    df['purchase_date'] = pd.to_datetime(df['dtm_res_Booking_Date'], format="%b %d, %Y").apply(lambda x: x.date())
    df['blended_fare'] = df['textbox107'].apply(lambda x: float(x.replace('$', '').replace(',', '').strip()))
    df['rbkd'] = df['FareClass'].apply(lambda x: x[0])
    df['orig_name'] = df['textbox106'].apply(lambda x: x.split(' To ')[0].strip())
    df['dest_name'] = df['textbox106'].apply(lambda x: x.split(' To ')[1].strip())
    df['orig_code'] = df['orig_name'].map(str_to_code)
    df['dest_code'] = df['dest_name'].map(str_to_code)
    df['itin'] = df['orig_code'] + '-' + df['dest_code']
    df = df.drop_duplicates(
        subset=['pnr', 'pax_name', 'purchase_date', 'travel_date', 'orig_code', 'dest_code', 'rbkd', 'blended_fare'])

    # main_df = pd.read_csv("py_processed_0726.csv")
    # df = df[df['travel_date'] >= datetime.strptime(main_df.travel_date.max(), '%Y-%m-%d').date()]
    df = df[df['travel_date'] >= snapshot_date]     #ignore old data(prior the date when data was extracted from amelia)

    pnr_df = df[['pnr', 'pax_name']].drop_duplicates()
    id2pnr = dict(enumerate(pnr_df.pnr.unique().tolist()))
    pnr2id = {v: k for k, v in id2pnr.items()}

    pnr_df['pnr_id'] = pnr_df['pnr'].map(pnr2id)
    pnr_df['name_id'] = pnr_df.groupby("pnr")['pax_name'].rank(method='dense').astype(int)
    pnr_df['pnr_idx'] = pnr_df['pnr_id'].astype(str) + '-' + pnr_df['name_id'].astype(str)
    pnr_df = pnr_df[['pnr', 'pax_name', 'pnr_idx']]
    df = df.merge(pnr_df, on=['pnr', 'pax_name'])

    ttype_dict = {}
    for g, g_df in df.groupby(["pnr", "pax_name"]):
        g_df = g_df.sort_values(by=['travel_date'], ascending=True)
        if g_df.itin.nunique() > 1:
            curr_itins = list(set([e for el in g_df.itin.unique().tolist() for e in el.split('-')]))
            if len(curr_itins) == g_df.shape[0] and g_df.iloc[0].orig_code == g_df.iloc[-1].dest_code:
                ttype_dict[g] = 'Round Trip'
            else:
                if g_df.shape[0] == 2 and g_df.orig_code.iloc[0] == g_df.dest_code.iloc[1]:
                    ttype_dict[g] = 'Open Jaw'
                elif g_df.orig_code.iloc[0] != g_df.dest_code.iloc[1]:
                    ttype_dict[g] = 'Complex'
                else:
                    ttype_dict[g] = 'Complex'
        else:
            ttype_dict[g] = 'One Way'

    is_group_dict = {}
    for g, g_df in df.groupby("pnr"):
        if g == 'NO ADD':
            is_group_dict[g] = 0
        else:
            is_group_dict[g] = int(g_df.pax_name.nunique() >= 9)

    df['ticket_type'] = df[['pnr', 'pax_name']].apply(lambda x: ttype_dict[(x['pnr'], x['pax_name'])]
    if (x['pnr'], x['pax_name']) in ttype_dict else 'One Way', axis=1)
    df['is_group'] = df['pnr'].map(is_group_dict)
    df['is_group'] = df['is_group'].astype(int)

    df['agency_id'] = [-1] * len(df)
    df['agency_country'] = ['UNKNOWN'] * len(df)
    df['country_of_sale'] = ['Unknown'] * len(df)
    df['local_dep_time'] = ['-'] * len(df)
    df['dep_time_block'] = ['-'] * len(df)
    df['agency_city'] = ['-'] * len(df)
    df['agency_continent'] = ['-'] * len(df)
    df['duration'] = [100] * len(df)
    df['stop_1'] = ['-'] * len(df)
    df['prev_dest'] = ['-'] * len(df)
    df['next_dest'] = ['-'] * len(df)
    df['bound'] = ['-'] * len(df)
    df['is_direct'] = [1] * len(df)

    df['distribution_channel'] = ['UNK'] * len(df)
    df['seg_class'] = ['Y'] * len(df)
    df['op_flt_num'] = ['-'] * len(df)
    df['equip'] = ['-'] * len(df)
    df['pax'] = [1] * len(df)
    df['dom_op_al_code'] = ['PY'] * len(df)
    df['blended_rev'] = df['blended_fare'] * df['pax']
    df['travel_month'] = df['travel_date'].apply(lambda x: x.month)
    df['travel_year'] = df['travel_date'].apply(lambda x: x.year)
    df['days_sold_prior_to_travel'] = df.apply(lambda x: (x['travel_date'] - x['purchase_date']).days, axis=1)
    df['travel_day_of_week'] = df['travel_date'].apply(lambda x: x.weekday() + 1)
    df['is_ticket'] = [1] * len(df)
    df['is_historical'] = [0] * len(df)
    df['fare_basis'] = df['FareClass']
    store_dataframe_in_temporary_file(df, 'forward_processed_before_normalisation.csv')
    normalize_country_of_sale_predeparture(df)
    store_dataframe_in_temporary_file(df, 'forward_processed_after_normalisation.csv')
    #store as CSV
    output_filename = os.path.join(os.getenv('OUTPUT_FOLDER'), output_filename)
    pd.concat([
        # main_df[OUTPUT_COLUMNS],
        df[OUTPUT_COLUMNS]
    ], axis=0).query("dom_op_al_code == 'PY'").reset_index(drop=True)[OUTPUT_COLUMNS].to_csv(output_filename)

def process_parameters(params):
    parser = argparse.ArgumentParser(
        description='Process historical(uplift) or forward looking(amelia) data, store it in the database')
    parser.add_argument('type',
                        help='historical_process|historical_store|forward_process|forward_store',
                        type=str)
    parser.add_argument('snapshot_date',
                        help='date of data snapshot (YYYY-MM-DD)',
                        type=str)
    parser.add_argument('--dryrun', type=bool, default=False,
                        help='Process everything but do not save data into database')
    parser.add_argument('--tempfiles', type=bool, default=True,
                        help='Store intermediate CSV files in temporary folder')
    arguments = parser.parse_args(params)
    logger.debug(f"Parsed arguments:{arguments}")
    return arguments

def set_last_updated(key,value,host) : 
    mongo = MongoWrapper()
    config = mongo.col_msd_config().find_one({'customer' : host})
    found = False 

    for item in config['configurationEntries'] : 
        if item['key'] == key : 
            found = True
            item['value'] = value 
    
    if not found : 
        config['configurationEntries'].append({
            "value" : value,
            'key' : key
        })

    mongo.col_msd_config().update_one(
        {'customer' : host},
        {'$set' : {"configurationEntries" : config['configurationEntries'] }}) 



args={}
if __name__ == '__main__':
    args = process_parameters(sys.argv[1:])
    logger.info(f"Starting DDS_PROCESS job, arguments:{sys.argv}")
    snapshot_date = datetime.strptime(args.snapshot_date, '%Y-%m-%d').date()
    logger.info(f"Processing type:{args.type}, snaphost_date:{snapshot_date}, dryrun:{args.dryrun}")
    logger.debug(f"Folders:\n\tUPLIFT input folder:{TICKETING_INPUT_FOLDER},\n\tAMELIA input folder:{PREDEPARTURE_INPUT_FOLDER},\n\tstatic data folder:{DICTIONARY_FOLDER},\n\toutput folder:{OUTPUT_FOLDER}")

    if args.type == 'historical_process':
        #load all UPLIFT data from input folder, process it and store output in output folder in a file 'historical_data.csv'
        historical_process('historical_data.csv', snapshot_date)
    elif args.type == 'historical_store':
        # load already processed data from output folder (from 'historical_data.csv') and store it in database (mysql + mongo)
        upload_data_to_db('historical_data.csv', snapshot_date)
        # once data is stored, delete forward looking data for departures for which we already have historical data
        delete_outdated_forward_data_from_db(snapshot_date)
    elif args.type == 'forward_process':
        # load all AMELIA data from input folder, process it and store output in output folder in a file 'forward_data.csv'
        forward_processing('forward_data.csv', snapshot_date)
    elif args.type == 'forward_store':
        # delete all forward looking data prior new batch is stored in the DB
        delete_all_forward_data_from_db()
        # load already processed forward looking data from 'forward_data.csv' and store it in the database (mysql + mongo)
        upload_data_to_db('forward_data.csv', snapshot_date)
    else:
        logger.error('Unknown TYPE parameter. Allowed values are: amelia or uplift')


