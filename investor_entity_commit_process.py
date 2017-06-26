import pymysql, time, itertools
import logging
from collections import OrderedDict

def investor_commit(db, logger, lead_investor, non_lead_investor, job_id, last_id, deal_row_log):
    cur = db.cursor()
    int_t = int(time.time())

    print 'status: Investor Commit - START'
    int_t = int(time.time())

    # investor type = "Investor"
    # select the deals which being committed, for new investor commit
    inv_query = ur"""select a.row_number, b.id, ifnull(a.matched_profile_name, a.entity_name) name, ifnull(a.matched_profile_id,0) profile_id, ifnull(a.matched_profile_type, "None") profile_type
    from dl_deal_import_deal_match a
    join company_round b on a.round_id = b.notes_internal and b.created_with = 'Import Tool' and b.id > """ + "%s" % str(last_id[0]) + """
    where round_id in (select a.round_id from dl_deal_import_deal_match a join dl_deal_import_row b
    on a.job_id = b.job_id and a.row_number = b.row_number
    where a.job_id = """ + "%s" % str(job_id[0]) + """ and a.role = 'Investor' and a.matched_deal_id is null
    group by a.round_id)
    and a.job_id = """ + "%s" % str(job_id[0]) + """ and a.role = 'Investor' order by a.row_number;"""
    cur.execute(inv_query)
    db.commit()
 
    row_res = cur.fetchall()
    #row_number 
    row_id = [i[0] for i in row_res] 
    
    inv_log = []
    inv_query_log = ur"""select a.round_id, b.id, a.entity_name from dl_deal_import_deal_match a 
    join company_round b on a.round_id = b.notes_internal and b.created_with = 'Import Tool' and b.id > """ + "%s" % str(last_id[0]) + """
    where round_id in (select a.round_id from dl_deal_import_deal_match a join dl_deal_import_row b
    on a.job_id = b.job_id and a.row_number = b.row_number
    where a.job_id = """ + "%s" % str(job_id[0]) + """ and a.role = 'Investor' and a.matched_deal_id is null
    group by a.round_id)
    and a.job_id = """ + "%s" % str(job_id[0]) + """ and a.role = 'Investor' order by a.row_number;"""
    cur.execute(inv_query_log)
    db.commit() 

    inv_log_row = cur.fetchall()
    
    
    # find new lead investors need to be inserted
    #lead_investor_import = [lead_investor[i] for i in range(len(lead_investor)) if lead_investor[i][1] in row_id]
    #row_res_lead = [row_res[i] for i in range(len(row_id)) if lead_investor[i][1] in row_id]     
    
 
    #non_lead_investor_import = [non_lead_investor[i] for i in range(len(non_lead_investor)) if non_lead_investor[i][1] in row_id]
    #row_res_non_lead = [row_res[i] for i in range(len(row_id)) if non_lead_investor[i][1] in row_id]
    
    # import data structure
    new_lead_investor_import = []
    new_non_lead_investor_import = []

    for i in range(len(row_res)):
         for j in range(len(lead_investor)):
              if lead_investor[j][1] == row_res[i][0]: 
                  new_lead_investor_import.append((row_res[i][1:] + (lead_investor[j][3],)))
    #logger.info('new lead investor to be committed %s' % new_lead_investor_import)
    
    for i in range(len(row_res)):
         for j in range(len(non_lead_investor)):
              if non_lead_investor[j][1] == row_res[i][0]: 
                  new_non_lead_investor_import.append(row_res[i][1:])
    #logger.info('new non lead investor to be commiteed %s' % new_non_lead_investor_import)           
    #new_non_lead_investor_import = [row_res[i][1:] for i in range(len(row_res)) if non_lead_investor_import[i][1] in row_id] 

    # import lead investors 
    inv_query = ur"""insert ignore into company_investor (company_round_id, name, profile_id, profile_type, lead_inve_type) values (%s,%s,%s,%s,%s)"""
    cur.executemany(inv_query, new_lead_investor_import)
    db.commit()
    
    # import non lead investors
    inv_query = ur"""insert ignore into company_investor (company_round_id, name, profile_id, profile_type) values (%s,%s,%s,%s)"""
    cur.executemany(inv_query, new_non_lead_investor_import)
    db.commit()

    # update company_investor.type by joining investor_profile table
    upd_query = ur"""update company_investor a join investor_profile b on a.profile_id = b.id and a.profile_type = 'Investor' set a.type = b.classification where a.type = '';"""
    cur.execute(upd_query)
    db.commit()

    # get update rocords counts
    record_counts = len(list(set(new_lead_investor_import))) + len(list(set(new_non_lead_investor_import)))

    print record_counts
    logger.info(' %s new investors committed' % record_counts)
    

    # get update rocords counts
    end_t = int(time.time())
    logger.info('status: investor commit - DONE, time elapsed: %s s' % (end_t - int_t))


    #update url_slug for newly committed deals 
    query = "select id from company_round where url_slug = '' and created_with = 'Import Tool' and created_with_import_id = %s;" % job_id[0]
    cur.execute(query)
    round_ids = cur.fetchall()

    for id in round_ids:
        cur.execute("call `UpdateCompanyRoundUrlSlug`(%s)" % id)
        db.commit()
    
    logger.info('status: update company_round url_slug - Done')

    logger.info('DATA BELOW ARE FOR PRODUCT TEAM REVIEW PROCESS')
    # construct dictionary for log file traverse data
    inv_key = ('traverse_id','deal_id_system','investor_free_text')
    for i in range(len(inv_log_row)):
        inv_dic = {}
        '''inv_dic.append(round_id: inv_log_row[0])
        inv_dic.append(deal_id: inv_log_row[1])
        inv_dic.append(entity_name: inv_log_row[2])'''
        '''inv_dic = dict([('traverse_id', inv_log_row[0]), ('deal_id_system', inv_log_row[1]), ('entity_name', inv_log_row[2])])'''
        inv_dic = {k:v for (k,v) in zip(inv_key, inv_log_row[i])}
        logger.info('free text investors committed %s' % inv_dic)
    #investor info  round_id, row_number, deal_id, entity_name
    '''for i in range(len(deal_row_log)):
        for j in range(len(inv_query_log)): 
            if deal_row_log[i][0] == inv_query_log[j][0]:
                inv_log.append(de)'''


    deal_log_key = ('traverse_id', 'month', 'day', 'year', 'funding_type', 'currency', 'deal_amount', 'was_imported')

    for i in range(len(deal_row_log)):
        deal_dic = {k:v for (k,v) in zip(deal_log_key, deal_row_log[i])}
        logger.info('Traverse Data %s' % deal_dic)


    


  


