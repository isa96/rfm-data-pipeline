# 1 Requirements
import pytz
import pandas as pd
import numpy as np 
import datetime as dt
import requests
from urllib.parse import urlencode, quote


def _send_message(message):
    user_id = "xxx"
    url = "xxx" + quote(str(message)) + "&chat_id=" + str(user_id)
    requests.get(url)
    print(message)

# time
 


# 2 Create the connection
_send_message(" {} - Data pipeline enabled (confidential-RFM) \n ".format(dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


## Database connection 1 
from sqlalchemy import create_engine
POSTGRES_ADDRESS = 'xxx.xxx.xxx.xxx' 
POSTGRES_PORT = 'xxxxx'
POSTGRES_USERNAME = 'xxxx' ## CHANGE THIS TO YOUR PANOPLY/POSTGRES USERNAME
POSTGRES_PASSWORD = 'xxxx'
POSTGRES_DBNAME = 'xxxxx' ## CHANGE THIS TO YOUR DATABASE NAME
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME, password=POSTGRES_PASSWORD, ipaddress=POSTGRES_ADDRESS, port=POSTGRES_PORT,dbname=POSTGRES_DBNAME))
cnx_wh = create_engine(postgres_str)


## Data retrivied from database 
time1 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x1 = str(time1) + " - Retrieval of data from the database....."


df = pd.read_sql_query('''
    select 
    *
    from transaction

''', cnx_wh)

df_customer = pd.read_sql_query('''
    select 
	* 
    from customer
    
    
    ''', cnx_wh)

time2 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x2 = str(time2) + " - Data retrieved successfully, amount of data = {}".format(len(df))

from sqlalchemy import create_engine

POSTGRES_ADDRESS = 'xxx.xxx.xxx' 
POSTGRES_PORT = 'xxxx'
POSTGRES_USERNAME = 'xxxxxx' 
POSTGRES_PASSWORD = 'xxxxxxx'
POSTGRES_DBNAME = 'xxxxxx'
postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME, password=POSTGRES_PASSWORD, ipaddress=POSTGRES_ADDRESS, port=POSTGRES_PORT,dbname=POSTGRES_DBNAME))
cnx_wh = create_engine(postgres_str)
conn = cnx_wh.connect()




# 3 Data preprocessing
time3 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x3 = str(time3) + " - Data pre-processing starts....."

df['tgl_transaksi'] = pd.to_datetime(df['tgl_transaksi']).dt.date
df = df.sort_values(by=['tgl_transaksi'])

df['id_customer'] = df['id_customer'].astype('str')
df['id_customer'] = df['id_customer'].str.strip()

print(" == Data Merged Successfully == ")

# 4 Add new column

## favorite program
dfo = df.groupby(by=['id_customer','program']).count().reset_index()[['id_customer','program','id_transaksi']]
dfo = dfo.sort_values(by=['id_transaksi'],ascending=False).drop_duplicates(subset=['id_customer']).rename(columns = {'program':'program_fav','id_transaksi':'total'})[['id_customer','program_fav']]

## first_transaction, last_transaction, first_program, last_program
df_ft = df.sort_values(by='tgl_transaksi').drop_duplicates(subset=['id_customer'])[['id_customer','tgl_transaksi','program']].rename(columns={'tgl_transaksi':'first_transaction','program':'first_program'})
df_lt = df.sort_values(by='tgl_transaksi',ascending=False).drop_duplicates(subset=['id_customer'])[['id_customer','program','tgl_transaksi']].rename(columns={'program':'last_program','tgl_transaksi':'last_transaction'})


#favorite_payment
df_pay = df.groupby(by=['id_customer','id_via_himpun']).count().reset_index()[['id_customer','id_via_himpun','id_transaksi']]
df_pay = df_pay.sort_values(by=['id_transaksi'],ascending=False).drop_duplicates(subset=['id_customer'])
df_pay.drop(['id_transaksi'], axis=1,inplace=True)
df_pay.rename(columns={'id_via_himpun':'payment_fav'},inplace=True)

# firts_payment
df_pf = df.sort_values(by='tgl_transaksi').drop_duplicates(subset=['id_customer'])[['id_customer','tgl_transaksi','id_via_himpun']].rename(columns={'tgl_transaksi':'first_transaction','id_via_himpun':'first_payment'})
df_pf = df_pf[['id_customer','first_payment']]

# last_payment
df_lf = df.sort_values(by='tgl_transaksi',ascending=False).drop_duplicates(subset=['id_customer'])[['id_customer','tgl_transaksi','id_via_himpun']].rename(columns={'tgl_transaksi':'first_transaction','id_via_himpun':'last_payment'})
df_lf = df_lf[['id_customer','last_payment']]

time4 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x4 = str(time4) + " - Data pre-processing succesfully"


# 5 Calculating RFM
time5 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x5 = str(time5) + " - RFM segmentation starts....." 
## calculating recency
df_recency = df.groupby(by=['id_customer'], as_index=False)['tgl_transaksi'].max()
df_recency.columns = ['id_customer','LastTransactionDate']
recent_date = df_recency['LastTransactionDate'].max()
df_recency['Recency'] = df_recency['LastTransactionDate'].apply(lambda x: (recent_date - x).days)

## calculating frequency
df_frequency = df.drop_duplicates().groupby(by='id_customer',as_index=False)['tgl_transaksi'].count()
df_frequency.columns = ['id_customer','Frequency']

## calculating monetary
df['total'] = df['transaksi']
df_monetary = df.groupby(by='id_customer', as_index=False)['total'].sum()
df_monetary.columns = ['id_customer','Monetary']

## merging recency, frequency, monetary
df_tmp = df_recency.merge(df_frequency,on = 'id_customer')
df_rfm = df_tmp.merge(df_monetary,on='id_customer').drop(columns='LastTransactionDate')


## split out segmentation

## in customer
df_rfm_new = df_rfm[~df_rfm['id_customer'].isin(['1307190000295','999999999','9999999999','999999999999','9999999999999'])]
## ex customer
df_rfm_ex = df_rfm[df_rfm['id_customer'].isin(['1307190000295','999999999','9999999999','999999999999','9999999999999'])]
df_rfm = df_rfm_new[df_rfm_new['Monetary']>=1000]
df_out_100 = df_rfm_new[df_rfm_new['Monetary']<1000]
df_rfm_ex = pd.concat([df_rfm_ex, df_out_100], ignore_index=True)

# 6 Exploratory Data Analysis

## Monetary
dfm = df_monetary[~df_monetary['id_customer'].isin(['1307190000295','999999999','9999999999','999999999999','9999999999999'])]
dfm['Monetary'].quantile([.2, .4, .6, .8]).to_dict()
m1 = dfm['Monetary'].quantile(.2)
m2 = dfm['Monetary'].quantile(.4)
m3 = dfm['Monetary'].quantile(.6)
m4 = dfm['Monetary'].quantile(.8)

## Frequency
dff = df_frequency[~df_frequency['id_customer'].isin(['1307190000295','999999999','9999999999','999999999999','9999999999999'])]
dff = dff[dff['Frequency']>=5]['Frequency']
dff.quantile([.2, .4, .6, .8]).to_dict()
f1 = dff.quantile(.2)
f2 = dff.quantile(.4)
f3 = dff.quantile(.6)
f4 = dff.quantile(.8)

## Recency
dfr = df_recency[~df_recency['id_customer'].isin(['1307190000295','999999999','9999999999','999999999999','9999999999999'])]
dfr[dfr['Recency']<=60][['Recency']].quantile([.2, .4, .6, .8]).to_dict()
dfr = dfr[dfr['Recency']<=60]
r1 = dfr['Recency'].quantile(.2)
r2 = dfr['Recency'].quantile(.4)
r3 = dfr['Recency'].quantile(.6)
r4 = dfr['Recency'].quantile(.8)


## Segmentation level 
quintiles = df_rfm[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()

def r_score(x):
    if x <= r1:
        return 5
    elif x <= r2:
        return 4
    elif x <= r3:
        return 3
    elif x <= r4:
        return 2
    else:
        return 1

def f_score(x):
    if x <= f1:
        return 1
    elif x <= f2:
        return 2
    elif x <= f3:
        return 3
    elif x <= f4:
        return 4
    else:
        return 5
    
def m_score(x):
    if x <= m1:
        return 1
    elif x <= m2:
        return 2
    elif x <= m3:
        return 3
    elif x <= m4:
        return 4
    else:
        return 5

df_rfm['R'] = df_rfm['Recency'].apply(lambda x: r_score(x))
df_rfm['F'] = df_rfm['Frequency'].apply(lambda x: f_score(x))
df_rfm['M'] = df_rfm['Monetary'].apply(lambda x: m_score(x))

df_rfm['RFM_Score'] = df_rfm['R'].map(str) + df_rfm['F'].map(str) + df_rfm['M'].map(str)


# Segmentation level 2
def segmentation(x):
    
    if x['RFM_Score'] in ('555','545'):
        return 'champions'
    
    elif x['RFM_Score'] in ('543', '544', '454', '444', '435', '355', '354', '345', '344', '335'):
        return 'loyal'
    
    elif x['RFM_Score'] in ('553', '551', '554', '552', '541', '542', '533', '532', '531', '452', '451', '442', '441', '431', '453', '433', '432', '423', '353', '352', '351', '342', '341', '333', '323'):
        return 'potential loyalist'
    
    elif x['RFM_Score'] in ('512', '511', '422', '421', '412', '411', '311'):
        return 'new customers'
    
    elif x['RFM_Score'] in ('525', '524', '523', '522', '521', '515', '514', '513', '425','424', '413','414','415', '315', '314', '313'):
        return 'promising'
    
    elif x['RFM_Score'] in ('535', '534', '443', '434', '343', '334', '325', '324','445','445','455'):
        return 'need attention'
    
    elif x['RFM_Score'] in ('331', '321', '312', '221', '213', '231', '241', '251'):
        return 'about to sleep'
    
    elif x['RFM_Score'] in ('255', '254', '245', '244', '253', '252', '243', '242', '235','234', '225', '224', '153', '152', '145', '143', '142', '135', '134', '133', '125', '124'):
        return 'at risk'
    
    elif x['RFM_Score'] in ('155', '154', '144', '214','215','115', '114', '113'):
        return 'cannot lose them'
    
    elif x['RFM_Score'] in ('332', '322', '233', '232', '223', '222', '132', '123', '122', '212', '211'):
        return 'hibernating customers'
    
    elif x['RFM_Score'] in ('111', '112', '121', '131','141','151'):
        return 'lost customers'
    
    else:
        return x['RFM_Score']

df_rfm['Segment'] = df_rfm[['RFM_Score']].apply(segmentation, axis=1)



# 7 Merge with EX customer

df_rfm_ex['R'] = 0 
df_rfm_ex['F'] = 0 
df_rfm_ex['M'] = 0 
df_rfm_ex['RFM_Score'] = 0 
df_rfm_ex['Segment'] = 'outside of segmentation'

df_rfm = pd.concat([df_rfm,df_rfm_ex], ignore_index=True)
df_rfm = df_rfm.merge(dfo,on='id_customer',how='left')

## idx
df_rfm['iteration'] = np.arange(df_rfm.shape[0])
df_rfm['id'] = df_rfm['iteration'] + 1
df_rfm.drop('iteration', inplace=True, axis=1)

## write_date,write_uid,create_uid,create_date

u = dt.datetime.utcnow()
u = u.replace(tzinfo=pytz.utc)

df_rfm['write_date'] =  pd.to_datetime(format(u.strftime("%m/%d/%Y %H:%M:%S"))) 
df_rfm['write_uid'] = float("nan")
df_rfm['create_uid'] = float("nan")
df_rfm['create_date'] = float("nan")
df_rfm['id_customer'] = df_rfm['id_customer'].astype('str')

## drop column r,f,m
df_rfm.drop(['R','F','M'], axis=1,inplace=True)

## detail customer
df_rfm = df_rfm.merge(df_customer,on='id_customer',how='left')

## firts_transaction, last_transaction
df_rfm = df_rfm.merge(df_ft,on='id_customer',how='left')
df_rfm = df_rfm.merge(df_lt,on='id_customer',how='left')


## favorite_payment, firts_payment, last_payment
df_rfm = df_rfm.merge(df_pf,on='id_customer',how='left')
df_rfm = df_rfm.merge(df_lf,on='id_customer',how='left')
df_rfm = df_rfm.merge(df_pay,on='id_customer',how='left')

# rename columns
df_rfm.rename(columns = {'customer':'customer','Recency':'recency','Frequency':'frequency','Monetary':'monetary','R':'r','F':'f','M':'m','RFM_Score':'rfm_score','Segment':'segment'},inplace=True)


# 8 Data Manipulation
## avg monetary
df_rfm['avg_monetary'] = round(df_rfm['monetary'] / df_rfm['frequency'])

## RFM Score to int
df_rfm['rfm_score'] = df_rfm['rfm_score'].astype('int32')

## rfm_segment_group 
df_rfm['segment_group'] = df_rfm['segment'].map({

    'champions' : 'True Friend',
    'loyal' : 'True Friend',
    'potential loyalist': 'True Friend',

    'new customers' : 'Barnacle',
    'promising': 'Barnacle',
    'need attention': 'Barnacle',

    'about to sleep': 'Stranger', 
    'hibernating customers': 'Stranger',
    'lost customers': 'Stranger',

    'at risk': 'Butterfly',
    'cannot lose them': 'Butterfly',
    'outside of segmentation': 'outside of segmentation'
})

print(" == Data Manipulation Successful == ")


time6 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x6 = str(time6) + " - RFM segmentation successfully"

# 9 Send to Data Warehouse

time7 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x7 = str(time7) + " - Data sent to data warehouse,please wait....."

df_rfm.to_sql('rfm_predictive', con=conn, if_exists='replace',index=False, method='multi')

time8 = dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")  
x8 = str(time8) + " - Data sent succesfully, amount of data = {} ".format(len(df_rfm))

_send_message(" {} \n {} \n {} \n {} \n {} \n {} \n {} \n {}".format(x1,x2,x3,x4,x5,x6,x7,x8))
_send_message(" {} - Data pipeline disabled (confidential-RFM) @aarichan \n ".format(dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))



 