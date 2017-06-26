# -*- coding: utf-8 -*-
import pymysql
import logging
import time
import re
#from dedup import deduplication
from dedup import manualDeduplication
from dedup import insertFreetextInvestor


def db_logger(hostname, username, passwdname, filename):
    logging.basicConfig(filename = 'log/{}_{}.log'.format(filename, time.strftime('%Y%m%d')), level = logging.INFO, filemode ='w')
    logger = logging.getLogger()
    try:
        db = pymysql.connect(host=hostname, user=username,
                             passwd=passwdname, db='db_privco', port=3306, charset='utf8')
    except:
        logger.error(
            "ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()
    print("SUCCESS: Connection to RDS mysql instance succeeded")
    return db, logger

def data_loading(db, logger, hostname, username, passwdname, filename, sourcename, row_entry, target, investor, mapped_profile_data, trigger, row_par_val):
  
   
    try:
        cur = db.cursor()

        # insert new job into deal_import_job
        query = ur"""insert into dl_deal_import_job (filename, dtc, source) select '""" + "%s" % str(
            filename) + """', unix_timestamp(now()), '""" + "%s" % str(sourcename) + """';"""
        cur.execute(query)
        db.commit()

        # pick the lastest job_id #####
        query_job_id = ur"""select id from dl_deal_import_job order by id desc limit 1;"""
        cur.execute(query_job_id)
        job_id = cur.fetchone()

        # insert data into deal_import_row
        row_entry = [(job_id,) + i + ('VC Fundings',) +
                     (int(time.time()),) for i in row_entry]
        query = ur"""insert into dl_deal_import_row (job_id, round_id, row_number, raw_data, currency, price, year, month, day, round_type, deal_type, dtc) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cur.executemany(query, row_entry)
        db.commit()

        # insert data into deal_import_entity
        investor = [(job_id,) + i for i in investor]
        target = [(job_id,) + i for i in target]

        logger.info('status: deal_import_entity - insert START')
        int_t = int(time.time())

        query = ur"""insert into dl_deal_import_entity (job_id, round_id, row_number, entity_name, role, country_code, website, import_profile_details, mapped_profile_id, mapped_profile_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cur.executemany(query, investor)
        db.commit()

        cur.executemany(query, target)
        db.commit()

        query = ur"""update dl_deal_import_entity set `entity_clean_name2` = `strip_special_name_words_company`(entity_name) where job_id = """ + "%s" % str(job_id[
            0]) + """;"""
        cur.execute(query)
        db.commit()

        end_t_1 = int(time.time())
        logger.info('status: deal_import_entity - insert DONE, time elapsed: %s s' % (end_t_1 - int_t) )

        # add stop words list for entity name (keep inserting as new ones flow
        # in)

        query = ur"""update dl_deal_import_entity set is_skipped = '1' where (entity_name regexp '^(GV|INVESTORS|INVESTOR|EMPLOYEES|MANAGEMENT|EXECUTIVES|DIRECTORS|ASSOCIATES|ASSOCIATE|FOUNDERS|SHAREHOLDERS|Jr)$' or entity_clean_name2 = '') and role = 'Target' and job_id = """ + "%s" % str(job_id[
            0]) + """;"""
        cur.execute(query)
        db.commit()

        if trigger:
            manualDeduplication(db, logger, job_id[0], mapped_profile_data, 'Target')
            insertFreetextInvestor(db, logger, job_id[0], 'Investor', investor)
            #deduplication(db, logger, job_id[0], 'Investor','company, investor')
           
        #after deduplication
        query = ur"""update dl_deal_import_entity set map_action = 'map_to_profile' where mapped_profile_id <> 0 and mapped_profile_id is not null and job_id = """ + \
            "%s" % str(job_id[0]) + """;"""
        cur.execute(query)
        db.commit()

        end_t_2 = int(time.time())
        logger.info('status: deal_import_entity - update DONE, time elapsed: %s s' % (end_t_2 - end_t_1))


        # the following steps depends on whether the direct name match is
        query_insert_mapped = """insert ignore into db_privco.dl_deal_import_entity_match
        select a.id, """ + "%s" % str(job_id[0]) + """, a.round_id, a.row_number, a.role, b.profile_name, b.profile_type, b.profile_id, weight * match_adjustment adjusted_weight
        from dl_deal_import_entity a join (

        select *, 1.5 match_adjustment
        from name_search_index
        group by profile_id, profile_type
        order by weight * match_adjustment desc, weight asc

        ) b on a.mapped_profile_id = b.profile_id and a.mapped_profile_type = b.profile_type
        where a.map_action = 'map_to_profile' and a.job_id = """ + "%s" % str(job_id[0]) + """ and a.is_skipped = '0';"""
        cur.execute(query_insert_mapped)
        db.commit()

        end_t_3 = int(time.time())
        logger.info('status: deal_import_entity_match - mapped ones insert DONE, time elapsed: %s s' % (end_t_3 - end_t_2))

        # potential deal dupe res based on matched profiles, insert results
        # into dl_deal_import_deal_match

        # 1). Insert mapped results first
        query = """insert into db_privco.dl_deal_import_deal_match (id, job_id, round_id, row_number, role, entity_name, matched_profile_name, matched_profile_type, matched_profile_id)
        select distinct e.id, e.job_id, e.round_id, e.row_number, e.role, e.entity_name, m.matched_profile_name, m.matched_profile_type, m.matched_profile_id
        FROM db_privco.dl_deal_import_entity e
        left join db_privco.dl_deal_import_entity_match m on e.job_id = m.job_id and e.row_number = m.row_number
        and e.role = m.role where e.job_id = """ + "%s" % str(job_id[0]) + """ and e.is_skipped = '0';"""
        cur.execute(query)
        db.commit()

        query = ur"""update dl_deal_import_deal_match set `entity_clean_name2` = `strip_special_name_words_company`(entity_name) where job_id = """ + "%s" % str(job_id[
            0]) + """;"""
        cur.execute(query)
        db.commit()

        end_t_4 = int(time.time())
        logger.info('status: deal_import_deal_match - insert DONE, time elapsed: %s s' % (end_t_4 - end_t_3))

    finally:
        cur.close()

    return job_id
