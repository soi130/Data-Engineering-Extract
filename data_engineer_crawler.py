#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 13:03:39 2024

@author: mynameisnak
"""

#%% import
import pandas as pd
import requests
import os
from datetime import datetime

#%% var
URL = '''https://th.jobsdb.com/api/chalice-search/v4/search?
        siteKey=TH-Main
        &sourcesystem=houston
        &userqueryid=['replace_with_eviron_var']
        &page=1
        &seekSelectAllPages=true
        &keywords=data+engineer
        &worktype=242
        &pageSize=100
        &include=seodata
        &locale=th-TH
        &seekerId=572229247&solId=2d4a9443-665b-4e99-a962-64a3a9f85ae5'''
S_URL = URL.replace('\n        ','')

NOW = datetime.now()
NOW_D = datetime.date(NOW)

# %%read, archive and transform
def extract():
    r = requests.get(S_URL)
    if r.status_code==200:
        rs = r.json()
        data = rs['data']
        nm  = pd.json_normalize(data)
        #add data_date
        nm.insert(0,'Data Date',NOW_D)
        #save as raw csv
        nm.to_csv(os.path.join('downloads',f'{NOW_D}_data_engineer_jdb.csv'),
                encoding='utf-8',
                header=True,
                index=False
                )

    else:
        print(f'Error status code = {r.status_code}')
            
def read_and_archive(archive=True):
    dl_file = os.listdir('downloads')
    fs = [f for f in dl_file if f.endswith('.csv')]
    len_fs = len(fs)
    #get_col
    count = 1
    dl_path = 'downloads'
    while count <= len_fs:
        for f in fs:
            if count == 1:
                df = pd.read_csv(os.path.join(dl_path,f),header=0,
                                 infer_datetime_format=True
                                 )
                if archive:
                    os.rename(os.path.join('downloads',f'{f}'),
                            os.path.join('downloads','archive',f'{f}')
                            )
                count +=1
            else:
                df_ = pd.read_csv(os.path.join(dl_path,f),header=0,
                                  infer_datetime_format=True
                                  )
                df = pd.concat([df,df_])
                if archive:
                    os.rename(os.path.join('downloads',f'{f}'),
                            os.path.join('downloads','archive',f'{f}')
                            )
                count +=1
    print(f'read and archive done with count = {len_fs}')
    return df

def transform(df):
    #drop unnecessary columns 
    col_to_keep = ['Data Date',
                   'bulletPoints',
                   'companyProfileStructuredDataId',
                   'location', 
                   'locationId', 
                   'locationWhereValue', 
                   'id', 
                   'isPremium',
                   'isStandOut', 
                   'listingDate', 
                   'salary', 
                   'teaser', 
                   'title',
                   'workType', 
                   'isPrivateAdvertiser', 
                   'advertiser.id',
                   'advertiser.description', 
                   'branding.id', 
                   'jobLocation.label', 
                   'jobLocation.countryCode',
                   'solMetadata.jobId',
                   'subClassification.id', 
                   'subClassification.description', 
                   'roleId',
                   'area', 
                   'areaId', 
                   'areaWhereValue'
                   ]
    df_drop = df[col_to_keep]
    return df_drop
    