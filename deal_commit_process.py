import pymysql, logging, time, sys
    
def deal_commit(db, logger, job_id, deal_row, source_type):
    cur = db.cursor()
	#logger.info('status: deal commit - START')
    int_t = int(time.time())

    query = ur"""select a.round_id, a.matched_profile_id from dl_deal_import_deal_match a join dl_deal_import_row b on a.job_id = b.job_id and a.row_number = b.row_number where a.job_id = """ + "%s" % str(job_id[0]) + """ and a.role = 'Target' and a.matched_deal_id is null and b.deal_type = 'VC Fundings' and a.round_id not in (select distinct round_id from dl_deal_import_deal_match where job_id = """ + "%s" % str(job_id[0]) + """ and matched_deal_id is not null) and a.matched_profile_id in (select id from company_profile) group by a.round_id order by a.round_id;"""
    
    cur.execute(query)
    row_res = cur.fetchall()
    round_id = [i[0] for i in row_res]	

    # find new deals need to be inserted 
    #new_deal_row = [deal_row[i] for i in range(len(deal_row)) if deal_row[i][0] in round_id]  
    new_deal_row = []
    row_res_de = []
    deal_row_log = []
    for i in range(len(deal_row)):
        if (deal_row[i][0] in round_id):
            for j in range(len(row_res)):
                if deal_row[i][0] == row_res[j][0]:
                    new_deal_row.append(deal_row[i])
                    row_res_de.append(row_res[j])
                    deal_row_log_tmp = deal_row[i] + (u'Yes',)
                    deal_row_log.append(deal_row_log_tmp)
        else:
            for n in range(len(row_res)):
                if deal_row[i][0] != row_res[n][0]:
                    deal_row_log_tmp = deal_row[i] + (u'No',)
                    deal_row_log.append(deal_row_log_tmp)

    # track last id in company_round
    query = ur"""select id from company_round order by id desc limit 1;"""
    cur.execute(query)
    last_id = cur.fetchone()
    # import data structure
    import_set = [(str(row_res_de[i][1]),) + new_deal_row[i][1:] + (str(row_res_de[i][0]),) + ("Import Tool",) + (job_id[0],) + (source_type,) + ("Yes",)
                  for i in range(len(new_deal_row))]

    # insert distinct target records
    query = ur"""insert ignore into company_round (company_profile_id, month, day, year, funding_type, currency, total, notes_internal, created_with, created_with_import_id, source_type, published) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    cur.executemany(query, import_set)
    db.commit()

    # update timestamp for the new insert
    query = ur"""update company_round set timestamp = unix_timestamp(concat(year,"-",if(month>0,month,1),"-",if(day>0,day,1))), dtc = unix_timestamp(now()), dte = unix_timestamp(now())
    where year > 0 and (timestamp is null or timestamp = '');"""
    cur.execute(query)
    db.commit()
   
    # get update rocords counts
    end_t = int(time.time())
    logger.info('status: deal commit - DONE, time elapsed: %s s, %s of new deals inserted. ' % ((end_t - int_t), len(import_set)))
    # committed deal id
    query = ur"""select id, notes_internal, company_profile_id from company_round where created_with_import_id =""" + "%s" % str(job_id[0]) + """;"""
    #log , traverse_id, deal_id, company_profile_id, 
    cur.execute(query)
    committed_deal_row = cur.fetchall()

    committed_deal_log_key = ('traverse_id', 'deal_id', 'commpany_profile_Id')
    for i in range(len(committed_deal_row)): 
        committed_deal_dic = {k:v for (k,v) in zip(committed_deal_log_key, committed_deal_row[i])}
        logger.info('Deal Committed %s' % committed_deal_dic)

    #logger.info('committed_deal_id: %s')
    print('status: deal commit - DONE, time elapsed: %s s, # of new deals inserted: %s' % ((end_t - int_t), len(import_set)))
    return last_id, deal_row_log

   