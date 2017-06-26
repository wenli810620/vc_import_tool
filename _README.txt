
## follow requirements.txt to install modules

$ pip install -r requirements.txt


## STEP 1: reading raw data from source file

Please refer to "vc_import_template.csv", the raw data will be in this format. A brief illustration on key columns:

"ID": identifier for each funding round (unique key, auto-increment)
"target": profile name extracted from json source for targets 
"mapped_profile_id": identifier for each target profile, retrieved from dupres api call
"mapped_profile_type" : profile type for target profile, retrieved togehter with mapped_profile_id

"investor": profile name extracted from json source for investors participated in the funding rounds
## call the dupres api to get the mapped profile id and profile type for investors 


## STEP 2: coding configuration

In "main.py" each variable is followed with definition after '#'. The variables that are needed to update every time: filepath
Related tables in this import process: dl_deal_import_job
                                dl_deal_import_row
                                dl_deal_import_entity
                                dl_deal_import_entity_match
                                dl_deal_import_deal_match
                                company_round
                                company_investor

For database setting variables: "dev_xxx" connects to development database, which is used for testing.
				"prod_xxx" connects to production database.


## STEP 3: run -python 'main.py' to invoke this import process 

#Before test with the same source file, run these delete queries

delete from dl_deal_import_job where id = last_job_id;
delete from dl_deal_import_row where job_id = last_job_id;
delete from dl_deal_import_entity where job_id = last_job_id;
delete from dl_deal_import_entity_match where job_id = last_job_id;
delete from dl_deal_import_deal_match where job_id = last_job_id;
delete from company_round where created_with_import_id = last_job_id;                             










