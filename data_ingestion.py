import boto3
import time
from datetime import datetime

### Query to run on Athena
query = """
select 
    a.paydate
    , a.emplid
    , a.institution
    , a.acad_career
    , a.appl_fee_dt
    , a.recruit_agency_id
    , a.adm_appl_dt
    , a.adm_appl_method
    , a.more_than_1_appl_cal_year
    , a.dipl_gpa
    , a.hs_gpa
    , a.sm_o_lvl_passes
    , a.ac_org_inst_id
    , appl_first_nationality
    , appl_first_citizenship
    , t.term_begin_dt
    , t2.term_end_dt
    , elvl.qualification_level
    , dob
    , gender_code
    , status
    , race_code
    , acad_load
    , RANK() OVER(PARTITION BY a.emplid ORDER BY a.appl_fee_dt) nth_appl
from mv_application a
left join aux_appl_first_info i 
    on a.adm_appl_nbr = i.adm_appl_nbr
left join dim_term t 
    on a.admit_term = t.term_code 
    and a.institution = t.institution 
    and a.acad_career = t.acad_career
left join dim_term t2 
    on a.appl_exp_grad_term = t2.term_code 
    and a.institution = t2.institution 
    and a.acad_career = t2.acad_career
left join dim_degree degree 
    on a.ac_degree_id = degree.degree_id
left join map_degree_edu_level elvl 
    on degree.edu_level_group = elvl.edu_level_group
left join dim_person person 
    on a.emplid = person.emplid
left join map_appl_action_reason status 
    on a.prog_action = status.prog_action 
    and a.prog_reason = status.prog_reason
left join dim_program prog 
    on a.prog_id = prog.prog_id 
    and a.institution = prog.institution 
    and a.stdnt_attr_value = prog.simpl_prog_code 
    and a.acad_career = prog.acad_career
where t.term_begin_dt < CAST ('2022-01-01' as date) 
    and (status = 'Offered and MATR' or status = 'Offered but not MATR')
    and a.acad_career = 'UGRD'
"""

#setting up athena and s3 session
athena = boto3.client('athena')
s3 = boto3.resource('s3')

#runs the query (uses personal env embedded aws auth)
response = athena.start_query_execution(
    QueryString=query,
    QueryExecutionContext={
        'Database':'sim_datalake'
    },
    ResultConfiguration={
        'OutputLocation':'s3://sim-athena-query-results/'
    },
    WorkGroup='primary'
)
execution_id = response['QueryExecutionId']


#waits til file is ready to be downloaded
#downloads file as temp.csv
local_filename = 'temp.csv'

while True:
    try:
        s3.Bucket("sim-athena-query-results").download_file(execution_id + ".csv", local_filename)
        break
    except:
        time.sleep(5)



now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Completed at", current_time)


