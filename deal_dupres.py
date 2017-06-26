import pymysql
import logging
from itertools import chain
import time
import errno

def list_convert(listvalue):

	str_list = '(' + ','.join(map(str, listvalue)) + ')'
	return str_list
    

def dupres_process(db, logger, hostname, username, passwdname, job_id, rownum):

	
	print 'status: Deal Dupe Res Process - START'
	int_t = int(time.time())
	job_id = list(job_id)

	try:

		cur = db.cursor()

		# Deal Info retrieval
		query_deal = """select row_number, unix_timestamp(str_to_date(concat(1,',',month,',',year),'%d,%m,%Y')) 'timestamp', round(price*conversion_rate_usd,1), round_type, round_id from dl_deal_import_row a join geo_currency b on a.currency = b.iso_code
		where job_id = """+"%s"%job_id[0]+""" and row_number >= """+"%s"%str(rownum)+""";"""

		cur.execute(query_deal)
		data_deal = cur.fetchall()

		for row in data_deal:
            
			stp_row = row[0]

			round_type = row[3]
			
			if row[3] == 'equity funding':
				str_round = """ AND cr.funding_type in ('equity funding', 'others_undisclosed')"""

			if row[3] == 'debt funding_financing':
				str_round = """ AND cr.funding_type in ('debt funding_financing', 'others_undisclosed')"""

			if row[3] == 'grant or non-equityfunding':
				str_round = """ AND cr.funding_type in ('grant or non-equityfunding', 'others_undisclosed')"""

			if row[3] == 'others_undisclosed':
				str_round = ""

			if row[3] == 'Crowdfunding':
				str_round = """ AND cr.funding_type in ('Crowdfunding')"""

			if row[3] == 'debt and equity':
				str_round = """ AND cr.funding_type in ('debt and equity')"""
				
			exclude_id = []
			### 1). VC deal match scenario I) 
			# Since we are not committing investor profile_id and profile_type into dl_deal_import_entity ,dl_deal_import_entity_match, dl_deal_import_deal_match ,this mapping logic can't find any duplicate deal information. 
			match_type = "(i) Deal within +/-2 months and 30% price + any investor or any target match + round type"
			timestamp_duration = 60*60*24*31*2
			timestamp_min = int(row[1]) - timestamp_duration
			timestamp_max = int(row[1]) + timestamp_duration
			price_percent = round(float(row[2])*.3, 2)
			price_min = float(row[2]) - price_percent
			price_max = float(row[2]) + price_percent

			if len(exclude_id) > 0:
				if len(exclude_id) == 1:
				    string_add = """ AND cr.id <> """ + "%s"%exclude_id[0]
				else:
				    string_add = """ AND cr.id NOT IN """ + "%s"%list_convert(exclude_id)
			else:
				string_add = ""

			query_deal_case = """SELECT cr.id, concat(cr.id,ci.id), cr.month, cr.day, cr.year, cr.total, cr.timestamp, cr.company_profile_id from company_round cr LEFT JOIN company_investor ci ON ci.company_round_id = cr.id and (concat(ci.profile_type,':', ci.profile_id,':',if(cr.funding_type = "others_undisclosed", '"""+round_type+"""',cr.funding_type)) IN (select concat(a.matched_profile_type,":",a.matched_profile_id,":",'"""+round_type+"""') from dl_deal_import_deal_match a where a.role = 'Investor' and a.matched_profile_type is not null and a.job_id = """+"%s"%str(job_id[0])+""" and a.row_number = """+"%s"%str(row[0])+""")
			OR `strip_special_name_words_company`(ci.name) IN (select entity_clean_name2 from dl_deal_import_deal_match where matched_profile_type is null and role = 'Investor' and job_id = """+"%s"%str(job_id[0])+""" and row_number = """+"%s"%str(row[0])+""")) 
			LEFT JOIN company_profile cp on cp.id = cr.company_profile_id AND (concat('Company:', cp.id, ':', if(cr.funding_type = "others_undisclosed", '"""+round_type+"""',cr.funding_type)) IN (select concat(a.matched_profile_type,":",a.matched_profile_id,":",'"""+round_type+"""') from dl_deal_import_deal_match a where a.role = 'Target' and a.matched_profile_type is not null and a.job_id = """+"%s"%str(job_id[0])+""" and a.row_number = """+"%s"%str(row[0])+""") 
			OR cp.clean_name2 IN (select entity_clean_name2 from dl_deal_import_deal_match where matched_profile_type is null and role = 'Target' and job_id = """+"%s"%str(job_id[0])+""" and row_number = """+"%s"%str(row[0])+"""))	 
			
			left join geo_currency gc on if(cr.currency = '', 'USD', cr.currency) = gc.iso_code
			WHERE (cr.total!="" AND cr.total IS NOT NULL AND cr.total*gc.conversion_rate_usd+0.0>="""+"%s"%str(price_min)+""" AND cr.total*gc.conversion_rate_usd+0.0<="""+"%s"%str(price_max)+""") AND (cr.timestamp >="""+"%s"%str(timestamp_min)+""" AND cr.timestamp <="""+"%s"%str(timestamp_max)+""") AND (cp.id IS NOT NULL and ci.id is not null) """+"%s"%str(str_round)+""" """+"%s"%str(string_add)+"""
			GROUP BY cr.id;"""

			cur.execute(query_deal_case)
			data_deal_case = cur.fetchall()

			for r in range(len(data_deal_case)):
				if len(str(data_deal_case[r][0])) > 0:
					exclude_id.append(data_deal_case[r][0])

			exclude_id = list(set(exclude_id))
			
			if len(data_deal_case):

			    for row_sub in data_deal_case:

				    query_update_1 = """update dl_deal_import_deal_match a join ((SELECT "Target" as `entity_type`, `name`, `id` as `profile_id`, "Company" as `profile_type` FROM `company_profile` as `cp` WHERE cp.id = """+"%s"%row_sub[7]+""")
				    
				    UNION (SELECT "Investor" as `entity_type`, `name`, `profile_id`, `profile_type` FROM `company_investor` WHERE concat(`company_round_id`,`id`) = """+"%s"%row_sub[1]+""")) b 
				    on a.role = b.entity_type and a.matched_profile_type = b.profile_type and a.matched_profile_id = b.profile_id
				    set a.matched_deal_id = """+"%s"%row_sub[0]+""", a.matched_type = '"""+"%s"%match_type+"""' 
				    where a.row_number = """+"%s"%str(row[0])+""" and a.job_id = """+"%s"%job_id[0]+""";""" 
				    
				    cur.execute(query_update_1)
				    db.commit()

				   
			    #logger.info('status: (Job: %s, Row: %s) Deal Mapping Scenario I) - DONE'%(job_id[0], str(stp_row)))
			    continue
			# print 'status: Deal Mapping Scenario I) - DONE'

            ### 2). Scenario II)  
			match_type = "(ii) Find any deal within 2 months and 20% price + target match + round type"
			timestamp_duration = 60*60*24*31*2
			timestamp_min = int(row[1]) - timestamp_duration
			timestamp_max = int(row[1]) + timestamp_duration
			price_percent = round(float(row[2])*.3, 2)
			price_min = float(row[2]) - price_percent
			price_max = float(row[2]) + price_percent

			if len(exclude_id) > 0:
				if len(exclude_id) == 1:
				    string_add = """ AND cr.id <> """ + "%s"%exclude_id[0]
				else:
				    string_add = """ AND cr.id NOT IN """ + "%s"%list_convert(exclude_id) 
			else:
				string_add = ""	

			query_deal_case = """SELECT cr.id, cr.month, cr.day, cr.year, cr.total, cr.timestamp, cr.company_profile_id from company_round cr
			LEFT JOIN company_profile cp on cp.id = cr.company_profile_id AND (concat('Company:', cp.id, ':', if(cr.funding_type = "others_undisclosed", '"""+round_type+"""',cr.funding_type)) IN (select concat(a.matched_profile_type,":",a.matched_profile_id,":",'"""+round_type+"""') from dl_deal_import_deal_match a where a.role = 'Target' and a.matched_profile_type is not null and a.job_id = """+"%s"%job_id[0]+""" and a.row_number = """+"%s"%str(row[0])+""")
			OR cp.clean_name2 IN (select entity_clean_name2 from dl_deal_import_deal_match where matched_profile_type is null and role = 'Target' and job_id = """+"%s"%job_id[0]+""" and row_number = """+"%s"%str(row[0])+"""))
			
			left join geo_currency gc on if(cr.currency = '', 'USD', cr.currency) = gc.iso_code

			WHERE (cr.total!="" AND cr.total IS NOT NULL AND cr.total*gc.conversion_rate_usd+0.0>="""+"%s"%str(price_min)+""" AND cr.total*gc.conversion_rate_usd+0.0<="""+"%s"%str(price_max)+""") AND 
			(cr.timestamp >="""+"%s"%str(timestamp_min)+""" AND cr.timestamp <="""+"%s"%str(timestamp_max)+""") AND (cp.id IS NOT NULL) """+"%s"%str(str_round)+""" """+"%s"%str(string_add)+"""
			GROUP BY cr.id;"""

			# print query_deal_case
		
			cur.execute(query_deal_case)
			data_deal_case = cur.fetchall()

			for r in range(len(data_deal_case)):
			    if len(str(data_deal_case[r][0])) > 0:
					exclude_id.append(data_deal_case[r][0])
                  
			exclude_id = list(set(exclude_id)) 

			if len(data_deal_case):

			    for row_sub in data_deal_case:
				    query_update_1 = """update dl_deal_import_deal_match a join (SELECT "Target" as `entity_type`, `name`, `id` as `profile_id`, "Company" as `profile_type` FROM `company_profile` as `cp` WHERE cp.id = """+"%s"%row_sub[6]+""") b 
				    on a.role = b.entity_type and a.matched_profile_type = b.profile_type and a.matched_profile_id = b.profile_id
				    set a.matched_deal_id = """+"%s"%row_sub[0]+""", a.matched_type = '"""+"%s"%match_type+"""' 
				    where a.row_number = """+"%s"%str(row[0])+""" and a.job_id = """+"%s"%job_id[0]+""";"""   
				    cur.execute(query_update_1)
				    db.commit()

				    logger.info('Deal Mapping: (Travese_ID: %s, Mapped_Deal_ID: %s, Deal_Mapping Scenario: %s)'%(row[4], row_sub[0], match_type))
			    continue
			
			# print 'status: Deal Mapping Scenario II) - DONE'



			### 3). Senario III) 
			'''match_type = "(iii) Find any deal within 2 months + any investor or any target match + round type //disabled 2014-03/18 per Anderson"

			timestamp_duration = 60*60*24*31*2
			timestamp_min = int(row[1]) - timestamp_duration
			timestamp_max = int(row[1]) + timestamp_duration

			if tuple(set(chain.from_iterable(exclude_id))):
				if len(tuple(set(chain.from_iterable(exclude_id)))) == 1:
					string_add = """ AND concat(cr.id,ci.id) <> """ + "%s"%tuple(set(chain.from_iterable(exclude_id)))
				else:
					string_add = """ AND concat(cr.id,ci.id) NOT IN """ + "%s"%str(tuple(set(chain.from_iterable(exclude_id))))
			else:
				string_add = ""

			query_deal_case = """SELECT concat(cr.id,ci.id), cr.id, cr.month, cr.day, cr.year, cr.total, cr.timestamp, cr.company_profile_id from company_round cr
			
			left join company_investor ci on ci.company_round_id = cr.id AND (concat(ci.profile_type,':', ci.profile_id,':',if(cr.funding_type = "others_undisclosed", '"""+round_type+"""',cr.funding_type))  IN (select concat(a.matched_profile_type,":",a.matched_profile_id,":",'"""+round_type+"""') from dl_deal_import_deal_match a where a.role = 'Investor' and a.matched_profile_type is not null and a.job_id = """+"%s"%str(job_id[0])+""" and a.row_number = """+"%s"%str(row[0])+""")
			OR `strip_special_name_words_company`(ci.name) IN (select entity_clean_name2 from dl_deal_import_deal_match where matched_profile_type is null and role = 'Investor' and job_id = """+"%s"%str(job_id[0])+""" and row_number = """+"%s"%str(row[0])+"""))
			
			LEFT JOIN company_profile cp on cp.id = cr.company_profile_id AND (concat('Company:', cp.id, ':',if(cr.funding_type = "others_undisclosed", '"""+round_type+"""',cr.funding_type)) IN (select concat(a.matched_profile_type,":",a.matched_profile_id,":",'"""+round_type+"""') from dl_deal_import_deal_match a where a.role = 'Target' and a.matched_profile_type is not null and a.job_id = """+"%s"%str(job_id[0])+""" and a.row_number = """+"%s"%str(row[0])+""")
			OR cp.clean_name2 IN (select entity_clean_name2 from dl_deal_import_deal_match where matched_profile_type is null and role = 'Target' and job_id = """+"%s"%str(job_id[0])+""" and row_number = """+"%s"%str(row[0])+"""))	
			
			WHERE (cr.timestamp >="""+"%s"%str(timestamp_min)+""" AND cr.timestamp <="""+"%s"%str(timestamp_max)+""") AND (cp.id is not null and ci.id IS NOT NULL) """+"%s"%str(str_round)+""" """+"%s"%str(string_add)+"""
			GROUP BY cr.id;"""

			# print query_deal_case
			
			cur.execute(query_deal_case)
			data_deal_case = cur.fetchall()

			exclude_id.append([row_sub[0] for row_sub in data_deal_case if row_sub[0]])
			
			for row_sub in data_deal_case:

				query_update_1 = """update dl_deal_import_deal_match a join ((SELECT "Target" as `entity_type`, `name`, `id` as `profile_id`, "Company" as `profile_type` FROM `company_profile` as `cp` WHERE cp.id = """+"%s"%row_sub[7]+""")
				UNION (SELECT "Investor" as `entity_type`, `name`, `profile_id`, `profile_type` FROM `company_investor` WHERE concat(`company_round_id`,`id`) = """+"%s"%row_sub[0]+""")) b 
				on a.role = b.entity_type and a.matched_profile_type = b.profile_type and a.matched_profile_id = b.profile_id
				set a.matched_deal_id = """+"%s"%row_sub[1]+""", a.matched_type = '"""+"%s"%match_type+"""' 
				where a.row_number = """+"%s"%str(row[0])+""" and a.job_id = """+"%s"%str(job_id[0])+""";"""    #### result 4 (duplicates)
				
				cur.execute(query_update_1)
				db.commit()

			if len(data_deal_case):
				continue'''

			# print 'status: Deal Mapping Scenario III) - DONE'

			## 4). Scenario IV) 
			match_type = "(iv) Find any deal within 2 months + any target match + round type"
			timestamp_duration = 60*60*24*31*2
			timestamp_min = int(row[1]) - timestamp_duration
			timestamp_max = int(row[1]) + timestamp_duration

			if len(exclude_id) > 0:
				if len(exclude_id) == 1:
				    string_add = """ AND cr.id <> """ + "%s"%(exclude_id[0])
				else:
				    string_add = """ AND cr.id NOT IN """ + "%s"%(list_convert(exclude_id))  
			else:
				string_add = ""	
            
			query_deal_case = """SELECT cr.id, cr.month, cr.day, cr.year, cr.total, cr.timestamp, cr.company_profile_id from company_round cr
			LEFT JOIN company_profile cp on cp.id = cr.company_profile_id AND (concat('Company:', cp.id, ':', if(cr.funding_type = "others_undisclosed", '"""+round_type+"""',cr.funding_type)) IN (select concat(a.matched_profile_type,":",a.matched_profile_id,":",'"""+round_type+"""') from dl_deal_import_deal_match a where a.role = 'Target' and a.matched_profile_type is not null and a.job_id = """+"%s"%job_id[0]+""" and a.row_number = """+"%s"%str(row[0])+""")
			OR cp.clean_name2 IN (select entity_clean_name2 from dl_deal_import_deal_match where matched_profile_type is null and role = 'Target' and job_id = """+"%s"%job_id[0]+""" and row_number = """+"%s"%str(row[0])+"""))	 
			
			WHERE (cr.timestamp >="""+"%s"%str(timestamp_min)+""" AND cr.timestamp <="""+"%s"%str(timestamp_max)+""") AND (cp.id IS NOT NULL) """+"%s"%str(str_round)+""" """+"%s"%str(string_add)+"""
			GROUP BY cr.id;"""

			cur.execute(query_deal_case)
			data_deal_case = cur.fetchall()

			for r in range(len(data_deal_case)):
			    if len(str(data_deal_case[r][0])) > 0:
					exclude_id.append(data_deal_case[r][0])

			exclude_id = list(set(exclude_id))
			
			if len(data_deal_case):
			    for row_sub in data_deal_case:

				    query_update_1 = """update dl_deal_import_deal_match a join (SELECT "Target" as `entity_type`, `name`, `id` as `profile_id`, "Company" as `profile_type` FROM `company_profile` as `cp` WHERE cp.id = """+"%s"%row_sub[6]+""") b 
				    on a.role = b.entity_type and a.matched_profile_type = b.profile_type and a.matched_profile_id = b.profile_id
				    set a.matched_deal_id = """+"%s"%row_sub[0]+""", a.matched_type = '"""+"%s"%match_type+"""' 
				    where a.row_number = """+"%s"%str(row[0])+""" and a.job_id = """+"%s"%job_id[0]+""";"""
				    print(row_sub)
				    cur.execute(query_update_1)
				    db.commit()

				    logger.info('Deal Mapping: (Travese_ID: %s, Mapped_Deal_ID: %s, Deal_Mapping Scenario: %s)'%(row[4], row_sub[0], match_type))

		end_t = int(time.time())
		print 'status: Deal Dupe Res Process - DONE, time elapsed: %s s'%(end_t - int_t)


	except pymysql.InternalError as e:
		end_t = int(time.time())
		print 'MySQL InternalError, time elapsed %s s and stopped at ROW: %s'%(end_t - int_t, stp_row)


	finally:
		cur.close()




	