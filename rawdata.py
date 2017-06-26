# -*- coding: utf-8 -*-
import unicodecsv as csv
from itertools import islice
#from xlrd import open_workbook
#from xlrd import xldate_as_tuple
#from phpserialize import *
import re
import json
import pycountry
def to_outline(headings):
  """Converts a list of input headings into nested nodes"""

  # Implement this function. Sample code below builds an 
  # outline of only the first heading

    return Node(Heading(0, ""), [Node(headings[0], [])])


def reading_data(filepath):
    with open(filepath, 'rU') as data:
        reader = csv.DictReader(data)
        for row in reader:
            yield row

def raw_data_retrieval_csv(filepath, startrow, endrow): 
    
    print(to_outline("H1 All About Birds"))
    row_entry = []
    deal_row = set()
    target = []
    investor = []
    deal_title = {}
    lead_investor = []
    non_lead_investor = []
    mapped_target_profile_data = []
   
    if startrow == '' and endrow == '': 
       startrow = 0 
       endrow = len(list(reading_data(filepath)))
    

    for idx, row in islice(enumerate(reading_data(filepath)), startrow, endrow):
        deal_title = row['deal_title']
        deal_title = deal_title.lower()
        
        row_entry.append((row['ID'], idx+1, json.dumps(row), row['currency'], row['deal_amount'], row['year'], row['month'], row['day'], deal_title))
        deal_row.add((row['ID'], row['month'], row['day'], row['year'], deal_title, 
                          row['currency'], row['deal_amount']))
        mapped_target_profile_data.append((row['target'], row['mapped_profile_id'], row['mapped_profile_type'], row['mapped_profile_name']))

        if row['target']:

           if row['target_hq_country'] == u'South Korea':
               row['target_hq_country'] = u'Korea, Republic of'
           
           if len(row['target_hq_country']) == 0:
               country_code = u'' 
           else :    
               country_code = pycountry.countries.get(name=row['target_hq_country']).alpha_2

           target.append((row['ID'], idx+1, row['target'], 'Target', country_code, row['target_url'], json.dumps(row), None, None))
         
        if row['investor']: 
            row['investor'] = re.sub('["]', '', row['investor'])
            investor.append((row['ID'], idx+1, row['investor'], 'Investor', '', '', json.dumps(row), None, None))

            if row['is_lead'] == '1':
                lead_investor.append((row['ID'], idx+1, row['investor'], 'Yes'))
               
            else :
                non_lead_investor.append((row['ID'], idx+1, row['investor'])) 
                
                    

    return  row_entry, target, investor, lead_investor, non_lead_investor, deal_row, mapped_target_profile_data

  