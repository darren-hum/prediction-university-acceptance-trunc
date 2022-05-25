from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import re

df = pd.read_csv('temp.csv')

#### Imputation #### -------------------------------------------------------

#null indicates 'no' rather than absence of data
df['direct'] = df['recruit_agency_id'].where(df['recruit_agency_id'].isnull(),1).fillna(0).astype(int)

#reverse as we are predicting who does not enroll/pay
df['enrolled'] = df['paydate'].where(df['paydate'].isnull(),0).fillna(1).astype(int)

#classify null as new column for dipl_gpa and olvl passes and reclassify nulls as -1
#df['dipl_gpa_present'] = df['dipl_gpa'].where(df['dipl_gpa'].isnull(),1).fillna(0).astype(int)
df['dipl_gpa'] = df['dipl_gpa'].fillna(-1).astype(int)
df['sm_o_lvl_passes'] = df['sm_o_lvl_passes'].where(df['sm_o_lvl_passes'].isnull(),1).fillna(0).astype(int)
df['sm_o_lvl_passes'] = df['sm_o_lvl_passes'].fillna(-1).astype(int)



#### Feature Engineering #### -------------------------------------------------------

# Getting age at application 
df['dob'] = pd.to_datetime(df['dob'])
df['term_begin_dt'] = pd.to_datetime(df['term_begin_dt'])
df['age'] = (df['term_begin_dt'] - df['dob'])
df['age'] = [float(x.days/365.25) for x in df['age']]

# Getting time from application to start date of programme
df['appl_fee_dt'] = pd.to_datetime(df['appl_fee_dt'])
df['appl_time_to_start'] = (df['term_begin_dt'] - df['appl_fee_dt'])
df['appl_time_to_start'] = [float(x.days/30.4375) for x in df['appl_time_to_start']]

# Getting their expected study length (given at application)
df['term_end_dt'] = pd.to_datetime(df['term_end_dt'])
df['expected_study_length'] = (df['term_end_dt'] - df['term_begin_dt'])
df['expected_study_length'] = [float(x.days/30.4375) for x in df['expected_study_length']]
df['expected_study_length'].fillna(df['expected_study_length'].mean(), inplace=True)

#Identifying past students (i.e. entry qualification is from SIM)
df['past_student'] = ['1' if x == ('SG001' or 'E10000002') else '0' for x in df['ac_org_inst_id']]


# Collapsing Nationalities with Low Freq
counts = df.appl_first_nationality.value_counts()
df['nat_count'] = df.appl_first_nationality.map(counts)
df.loc[df['nat_count']<2000, 'appl_first_nationality'] = 'OTHR'
df.appl_first_nationality.value_counts()


# Collapsing A level and High School into 1 as they are more or less equivalent (A level - UK ; High School - US)
def edu_level(row):
    if row['qualification_level'] in ["'A' Level", "High School"]:
        return 'A lvl/High School'
    else: 
        return row['qualification_level']

df['qualification_lvl'] = df.apply(lambda row: edu_level(row), axis=1)


#### Drop #### -------------------------------------------------------
drop_cols = ['dob', 
            'appl_fee_dt', 
            'term_begin_dt', 
            'adm_appl_dt', 
            'term_end_dt', 
            'recruit_agency_id',
            'paydate', 
            'hs_gpa', 
            'ac_org_inst_id',
            'status', 
            'year_of_study', 
            'emplid',
            'nat_count',
            'qualification_level'
             
            ]

df.drop(drop_cols, axis=1, inplace=True)
df.dropna(axis=0, inplace=True)

# 9 data points; unsure meaning of 'Other'
df.drop(df[df['qualification_lvl'] == 'Other'].index, inplace=True) 

df.to_csv('processed_data.csv', index=None)
now = datetime.now()
current_time = now.strftime("%H:%M")
print("Completed at", current_time)