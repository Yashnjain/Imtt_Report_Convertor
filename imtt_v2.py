from bu_alerts import send_mail
from datetime import datetime
from datetime import date
import numpy as np
import pandas as pd
import glob, sys, os, time, logging
import bu_alerts
from zipfile import ZipFile
from tabula import read_pdf
import shutil
from common import send_email_with_attachment as send_mail
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

temp_download = os.getcwd()+"\\temp_download"
receiver_email = 'imam.khan@biourja.com, devina.ligga@biourja.com'
to_mail_list = ["imam.khan@biourja.com", "devina.ligga@biourja.com", "arvind.patidar@biourja.com", "rini.gohil@biourja.com", "amit.bhonsle@biourja.com"]



job_id=np.random.randint(1000000,9999999)
today_date = date.today().strftime("%m-%d-%Y")
file_loc = os.getcwd()+f"\\data"
job_name = "IMTT_REPORT_CONVERTER V2"
# log progress --
# logfile = 'C:\\AJ\\PowerSignals\\paper_position_report_bnp\\bnp_pdf_Logfile.txt'
logfile = os.getcwd()+'\\'+str("itt_Logfile.txt")+'.txt'

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=logfile)


def pdf_page_breaker(today_date):
    try:
        email_df = []
       
        df = read_pdf(file_loc + '\\' + "imtt"+today_date+".pdf", pages = 'all', guess = False, stream = True ,
                    pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,270,330,365,367,417,450,480,520,583,640,740"])
        
        main_df = pd.concat(df[:-1], ignore_index=True)
        
            

        m_df = main_df[[5,7,3,2,1,9,10]]
        # m_df = main_df[[2,3,10,7]]
        m_df.dropna(inplace=True)
        m_df.reset_index(drop=True, inplace=True)
        try:
            if m_df[3].tail(1).str.contains("TOTA").bool():
                m_df.drop(m_df.tail(1).index,inplace=True) # remove total
        except:
            pass
        for i in range(len(m_df)):
            
            if i%2==0 or i == 0:
                print("even ",i)
                m_df[9][i] = m_df[9][i] + m_df[9][i+1]
                m_df[10][i] = m_df[10][i] + m_df[10][i+1]
            else:
                m_df.drop(i, inplace=True)
        
        m_df.columns = ["BOL", "BOL Date", "Carrier Name","Customer", "Destination", "Gross Gallon", "Net Gallon"]
        m_df["BOL"] = pd.to_numeric(m_df["BOL"])
        m_df["Carrier Name"] = pd.to_numeric(m_df["Carrier Name"])
        m_df.insert(0, column="Department", value="Ethanol")
        m_df.insert(1, column="Document Type", value=" ")
        m_df.insert(2, column="File Name", value=" ")
        m_df.insert(9, column="Gross Gallon 2", value=" ")
        m_df["Origin"] = "Montgomery-AL"
        # m_df.columns = ["Department", "Document Type", "File Name", "BOL", "BOL Date", "Carrier Name","Customer", "Destination", "Gross Gallon", "Gross Gallon 2", "Net Gallon", "Origin"]
        m_df.to_excel(file_loc+"\\imttv2"+today_date+".xlsx", sheet_name = today_date,index=False)

        email_df.append(file_loc+"\\imttv2"+today_date+".xlsx")
        
            
        return email_df
    except Exception as e:
        logging.info(f"Exception caught in pdf_page_breaker {e}")
        raise e

def main():
    ############Uncomment for test ###############
    email_date = "10-04-2021"
    pdf_page_breaker(email_date)
    ##############################################





if __name__ == "__main__":
    logging.info('Execution Started')
    rows=0
    time_start = time.time()
    logging.warning('Start work at {} ...'.format(time_start))
    log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
    # bu_alerts.bulog(process_name="RISK:BNP_PDF", database='POWERDB',status='Started',table_name = '', row_count=0, log=log_json, warehouse='',process_owner='MANISH')
    main()
    time_end = time.time()
    logging.warning('It took {} seconds to run.'.format(time_end - time_start))
    print('It took {} seconds to run.'.format(time_end - time_start))