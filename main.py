from rawdata import raw_data_retrieval_csv
from deal_dupres import dupres_process
from investor_entity_commit_process import investor_commit
from dataloading import db_logger
from dataloading import data_loading
from deal_commit_process import deal_commit
from investor_entity_commit_process import investor_commit

# location of source file
filepath = "20170616_vc - final.csv"

# job name saved in table "dl_deal_import_job"
filename = filepath.rsplit('\\', 1)[-1].rsplit('.', 1)[0]
# usually in "Authority" if source is legit
sourcename = 'Authority'

# it will be the 1st row by default in "" if no specific start row is indicated
startrow = ''
# similarly, this will be the last row by default in ""
endrow = ''


# development database settings
dev_host = 'privcordsdev01.c7kz0293mnmf.us-east-1.rds.amazonaws.com'
dev_user = 'privcoawsuser'
dev_passwd = 'Privco88devdb'

# production database settings
prod_host = 'privcordsmysql.c7kz0293mnmf.us-east-1.rds.amazonaws.com'
prod_user = 'privcoawsuser'
prod_passwd = 'Privco88aws'


# turn on (1)/off (0) potential name mapping process
trigger = 1
# the partition value set for profile id mapping
row_par_val = 100


if __name__ == "__main__":

    # data_retrival & transformation
    ## row_entry, target, buyer, seller = raw_data_retrieval(filepath, tabname, startrow, endrow)
    row_entry, target, investor, lead_investor, non_lead_investor, deal_row, mapped_target_profile_data = raw_data_retrieval_csv(
        filepath, startrow, endrow)
    
    
    
     # set up logger
    # db, logger = db_logger(dev_host, dev_user, dev_passwd, filename)
   ''' db, logger = db_logger(dev_host, dev_user, dev_passwd, filename)

    # data loading
    job_id = data_loading(db, logger, dev_host, dev_user, dev_passwd, filename,
                      sourcename, row_entry, target, investor, mapped_target_profile_data, trigger, row_par_val)
    
    # dupe res process
    # initiation row for each time dupe_res process (in case dupe res breaks,
    # and overwrite previous records again)
    rownum = 1
    dupres_process(db, logger, dev_host, dev_user, dev_passwd, job_id, rownum)


    # target profile commit
    # company_round.source_title, change here if necessary
    last_id, deal_row_log = deal_commit(db, logger, job_id, list(deal_row), sourcename)
    

    # investor commit
    investor_commit(db, logger, lead_investor, non_lead_investor, job_id, last_id, deal_row_log)

