# -*- coding: utf-8 -*-
import pdb, traceback, pymysql, logging, time, sys, json
import re
import operator
import grequests

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def getFromDict(datadict, maplist):
    try:
        return reduce(operator.getitem, maplist, datadict)
    except:
        return None

def apicall(data):

    try: 
        url = 'http://dev04.privco.com/deduplication/v8/close_matches/'
        headers = {'Content-Type': 'application/json'}
        r = (grequests.post(url, data=json.dumps(d), headers=headers)
             for d in data)
        res = grequests.map(r)
        idpath = ["document_from_database", "profile_id"]
        typepath = ["document_from_database", "type_of_profile"]
        namepath = ["document_from_database", "profile_name"] 

        #only return best match result (1 profile)
        resultsets = []

        for v in res:
            rec = json.loads(v.content)['records']
            if rec:
                resultsets.append((getFromDict(json.loads(v.content)['records'].values()[0], idpath)[0],\
                                   getFromDict(json.loads(v.content)['records'].values()[0], typepath)[0],\
                                   getFromDict(json.loads(v.content)['records'].values()[0], namepath)[0]))
            else:
                resultsets.append((None, None, None))
        return resultsets

    except:
        print "Exception in apicall"
        time.sleep(60)
        apicall(data)

#skip the identification api
'''def deduplication(db, logger, jobid, roletype, profiletype):
    cur = db.cursor()
    logger.info('status: dl_deal_import_entity - %s dedup START' % roletype)
    int_t = time.time()
    try:
        query_select = """select distinct entity_name, country_code from dl_deal_import_entity where job_id = """ + \
            "%s" % jobid + """ and role = '""" + "%s" % str(roletype) + \
            """' and is_skipped = '0' and mapped_profile_id is null and map_action = '';"""
        cur.execute(query_select)
        db.commit()

        rownum = cur.fetchall()
        datadump = [{"profile_name": rownum[i][0], 
                     "api_call_meta": {"type_of_profile": profiletype.split(', '), 
                                       "allowed_minimum_total_score_necessary_to_be_returned_from_best_match": '30', 
                                       "headquarters_country_code":rownum[i][1]
                                      }
                    } for i in range(len(rownum))]

        # print datadump, json.dumps(datadump)
        # sys.exit()
        for i, j in zip(rownum, apicall(datadump)):
            print(j[0])
            print(i[0])
            try:
                if j[0]:
                    query_update = ur"""update dl_deal_import_entity a
                    set a.mapped_profile_id = """ + "%s" % j[0] + """,
                    a.mapped_profile_type = '""" + "%s" % j[1] + """',
                    a.map_action = 'map_to_profile'
                    where a.job_id = """ + "%s" % jobid + """ and a.role = '""" + "%s" % str(roletype) + """'
                    and a.is_skipped = '0' and a.mapped_profile_id is null and map_action = '' and a.entity_name = \"""" + "%s" % i[0] + """\";"""
                else:
                    query_update = ur"""update dl_deal_import_entity a
                    set a.map_action = 'import_as_new'
                    where a.job_id = """ + "%s" % jobid + """ and a.role = '""" + "%s" % str(roletype) + """'
                    and a.is_skipped = '0' and a.mapped_profile_id is null and map_action = '' and a.entity_name = \"""" + "%s" % i[0] + """\";"""

                # print query_update
                # sys.exit()
                cur.execute(query_update)
            except:
                db.rollback()
                raise
            else:
                db.commit()
    finally:
        cur.close()
        end_t = time.time()
        logger.info('status: dl_deal_import_entity - %s dedup DONE, time elapsed: %s s' % (roletype, format(end_t - int_t, '.3f')))
        print('status: dl_deal_import_entity - %s dedup DONE, time elapsed: %s s' % (roletype, format(end_t - int_t, '.3f')))'''

# will commit all investors info from Traverse as free text in company_investor table
def insertFreetextInvestor(db, logger, jobid, roletype, investor):
    cur = db.cursor()
    int_t = time.time()
    try:
        
        for i in range(len(investor)):
            inv_name = investor[i][3]
            try:
                query_update = ur"""update dl_deal_import_entity a
                set a.map_action = 'import_as_new'
                where a.job_id = """ + "%s" % jobid + """ and a.role = '""" + "%s" % str(roletype) + """'
                and a.is_skipped = '0' and a.mapped_profile_id is null and map_action = '' and a.entity_name = \"""" + "%s" % inv_name + """\";"""
                #logger.info('investor free text be inserted : %s ' % investor[i][3])
                cur.execute(query_update)

            except:
                db.rollback()
                raise
            else:
                db.commit()    
    finally:
        cur.close()
        end_t = time.time() 

def manualDeduplication(db, logger, jobid, mapped_target_profile_data, roletype):
    cur = db.cursor()
    logger.info('status: dl_deal_import_entity - %s manually dedup START' % roletype)
    int_t = time.time()
    try:
        for i in range(len(mapped_target_profile_data)):
            
            try:
                if is_number(mapped_target_profile_data[i][1]):
                    mapped_profile_target = list(mapped_target_profile_data[i])
                    if(len(mapped_profile_target[2]) == 0):
                        mapped_profile_target[2] = "Company"
                        print(mapped_profile_target[2])

                    query_update = ur"""update dl_deal_import_entity a 
                    set a.mapped_profile_id = """ + "%s" % mapped_target_profile_data[i][1] + """,
                    a.mapped_profile_type = '""" + "%s" % mapped_profile_target[2] + """',
                    a.map_action = 'map_to_profile'
                    where a.job_id = """ + "%s" % jobid + """ and a.role = '""" + "%s" % str(roletype) + """'
                    and a.is_skipped = '0' and a.mapped_profile_id is null and map_action = '' and a.entity_name = \"""" + "%s" % mapped_target_profile_data[i][0] + """\";"""
                
                else:   
                    query_update = ur"""update dl_deal_import_entity a
                    set a.map_action = 'import_as_new'
                    where a.job_id = """ + "%s" % jobid + """ and a.role = '""" + "%s" % str(roletype) + """'
                    and a.is_skipped = '0' and a.mapped_profile_id is null and map_action = '' and a.entity_name = \"""" + "%s" % mapped_target_profile_data[i][0] + """\";"""
                    #logger.info('new shells to be created : %s ' % mapped_target_profile_data[i][0])
                    print('new shells to be created : %s ' % mapped_target_profile_data[i][0])
               
                cur.execute(query_update)
            except:
                db.rollback()
                print "Unexpected error:", sys.exc_info()[0]
                raise
            else: 
               db.commit()    
        
    finally:
        cur.close()
        end_t = time.time()
        logger.info('status: dl_deal_import_entity - %s manually dedup DONE, time elapsed: %s s' % (roletype, format(end_t - int_t, '.3f')))   
        print('status: dl_deal_import_entity - %s manually dedup DONE, time elapsed: %s s' % (roletype, format(end_t - int_t, '.3f'))) 