import os
import sys
import time
import glob
import shutil
import logging
import bu_alerts
import numpy as np
import pandas as pd
from datetime import date
from tabula import read_pdf
from bu_config import config
from datetime import datetime, timedelta


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

def pdf_page_breaker():
    try:
        email_df = []
        check = ''
        files_list = glob.glob(file_loc + '\\*pdf')
        logging.info(f"currently forIMTTv2 folder contains {files_list}")
        for file in glob.glob(file_loc + '\\*pdf'):
            file_name = file.split("\\")[-1].replace("imtt","")
            file_name = file_name.replace(".pdf", "")
            df = read_pdf(file, pages = 'all', guess = False, stream = True ,
                        pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,330,367,417,450,482,520,550,583,640,740"])
            
            main_df = pd.concat(df[:-1], ignore_index=True)
            m_df = main_df[[4,5,3,2,1,7,8]]
            m_df = m_df[m_df[3].notna()]
            m_df.reset_index(drop=True, inplace=True)
            try:
                if m_df[3].tail(1).str.contains("TOTA").bool():
                    # remove total
                    m_df.drop(m_df.tail(1).index,inplace=True) 
            except:
                pass
            for i in range(len(m_df)):
                
                if i%2==0 or i == 0:
                    print("even ",i)
                    try:
                        m_df[7][i] = int(m_df[7][i]) + int(m_df[7][i+1])
                        m_df[8][i] = int(m_df[8][i]) + int(m_df[8][i+1])
                    except:
                        # Add nothing
                        m_df[7][i] = int(m_df[7][i])
                        m_df[8][i] = int(m_df[8][i])
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
            m_df['BOL Date'] = pd.to_datetime(m_df['BOL Date'], format='%m/%d/%y').dt.strftime('%m-%d-%Y')
            file_name = file.split("\\")[-1].replace("imtt","")
            file_name = file_name.replace(".pdf", "")
            m_df.to_excel(data_loc+"\\imtt_v2_"+file_name+".xlsx", sheet_name = today_date,index=False)
            email_df.append(data_loc+"\\imtt_v2_"+file_name+".xlsx")
            logging.info(f"currently email_df is {email_df}")
            shutil.move(file, data_loc+"\\"+file.split("\\")[-1])
        return email_df, check
    except Exception as e:
        print(f"Exception caught in pdf_page_breaker {e}")
        logging.info(f"Exception caught in pdf_page_breaker {e}")
        raise e


def imtt_runner():
    try:
        time_start = time.time()
        global jobname,file_loc,data_loc,today_date
        logfile = os.getcwd() + '\\logs\\'+'IMTT_V2_Logfile'+'.txt'
        file_loc = os.getcwd() + "\\forIMTTv2"
        data_loc = os.getcwd()+"\\data"
        today_date = (date.today()-timedelta(days=0)).strftime("%m-%d-%Y")
        logfile = os.getcwd()+'\\logs\\'+str("imtt_v2_Logfile.txt")+'.txt'
        logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] - %(message)s',
        filename=logfile)
        logging.warning('Start work at {} ...'.format(time_start))
        logging.warning('Execution started')
        rows=0
        job_id = np.random.randint(1000000,9999999)
        credential_dict = config.get_config('IMTT_V2', 'N',other_vert= True)
        database = credential_dict['DATABASE'].split(";")[0]
        warehouse=credential_dict['DATABASE'].split(";")[1]
        tablename = credential_dict['TABLE_NAME']
        jobname = credential_dict['PROJECT_NAME']
        owner = credential_dict['IT_OWNER']
        receiveremail = "yashn.jain@biourja.com,imam.khan@biourja.com,yash.gupta@biourja.com,bhavana.kaurav@biourja.com"
        
        #testing environment
        # warehouse = "BUIT_WH"
        # jobname = "BIO-PAD01 IMTT_V2"
        # database = "BUITDB_DEV"
        # receiveremail = "yashn.jain@biourja.com,imam.khan@biourja.com"
        jobname = "BIO-PAD1_" + jobname
        #############################

        log_json = '[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=jobname, database=database,status='STARTED',row_count=rows,table_name = tablename,log=log_json,
        warehouse=warehouse,process_owner=owner)
        logging.info("Entered in pdf_page_breaker")

        email_df, check = pdf_page_breaker()
        if len(email_df)>0:
            logging.info("Sending mail now")
            log_json = '[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            bu_alerts.bulog(process_name=jobname,database=database,status='COMPLETED',row_count=1,table_name=tablename,log=log_json,
            warehouse=warehouse,process_owner=owner)
            bu_alerts.send_mail(receiver_email = receiveremail, mail_subject='JOB SUCCESS - {} {} {}'.format(jobname, today_date, check),
                                 mail_body='{} completed successfully, Attached invoice file'.format(jobname),multiple_attachment_list= email_df)
        else:
            logging.info('No new file found')
            mail_subject = 'JOB SUCCESS - {} No file found'.format(jobname)
            mail_body = '{} completed successfully, Attached logs'.format(jobname)
            bu_alerts.send_mail(receiver_email = receiveremail,mail_subject =mail_subject,mail_body = mail_body,
                attachment_location= logfile)
    except Exception as e:
        logging.exception(e)
        logging.info("Sending Failure mail now")
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.datetime.now())+'"}]'
        bu_alerts.bulog(process_name=jobname, database=database,status='FAILED',table_name = tablename, row_count=0, log=log_json, warehouse=warehouse,process_owner=owner)
        bu_alerts.send_mail(receiver_email = receiveremail,mail_subject ='JOB FAILED - {}'.format(jobname),mail_body = '{} failed, Attached logs'.format(jobname),attachment_location = logfile)
        sys.exit(-1)
    finally:
        time_end = time.time()
        logging.warning('It took {} seconds to run.'.format(time_end - time_start))
        print('It took {} seconds to run.'.format(time_end - time_start))


if __name__ == "__main__":
    imtt_runner()
