import os
import sys
import time
import pytz
import shutil
import logging
import bu_alerts
import numpy as np
import pandas as pd
from datetime import date
from tabula import read_pdf
from bu_config import config
from datetime import datetime


for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

def pdf_page_breaker(email_df):
    try:
        for f in os.listdir(temp_download):
            logging.info(f"current checking {f}")
            if "shipping report" in f.lower() or "shipping.pdf" in f.lower() or "shipping repots" in f.lower() or "transaction reoprt" in f.lower():
                df = read_pdf(temp_download + '\\' + f, pages = 'all', guess = False, stream = True ,
                            pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,330,367,417,450,485,520,550,583,640,740"])
                main_df = pd.concat(df[:-1], ignore_index=True)
                m_df = main_df[[2,3,8,5]]
                m_df = m_df[m_df[3].notna()]
                m_df.reset_index(drop=True, inplace=True)
                try:
                    if m_df[3].tail(1).str.contains("TOTA").bool():
                        # remove total
                        m_df.drop(m_df.tail(1).index,inplace=True) 
                except:
                    pass
                for i in range(len(m_df)):
                    try:
                        m_df[8][i] = int(m_df[8][i])
                    except:
                        m_df[8][i] = 0
                    if i%2==0 or i == 0:
                        m_df[3][i] = m_df[2][i+1]
                    else:
                        m_df[5][i] = m_df[5][i-1]
                        m_df[3][i] = m_df[2][i]
                        m_df[2][i] = m_df[2][i-1]   
                m_df.columns = ["CUSTOMER NAME", "DESTINATION", "NET GALLONS", "DATE"]
                f_name = m_df["DATE"][0]
                f_name = f_name.replace("/", "-")
                f_name = f_name.replace(":", " ")
                m_df.to_excel(data_loc+"\\imtt"+f_name+".xlsx", sheet_name = f_name,index=False)

                email_df.append(data_loc+"\\imtt"+f_name+".xlsx")
                shutil.move(temp_download + '\\' +f, file_loc+"\\imtt"+f_name+".pdf")
            else:
                logging.info(f"removing {f}")
                os.remove(temp_download + '\\' + f)
        return email_df
    except Exception as e:
        print(f"Exception caught in pdf_page_breaker :{e}")
        logging.info(f"Exception caught in pdf_page_breaker :{e}")
        raise e

def main():
    try:
        email_df = []
        email_df = pdf_page_breaker(email_df)
        logging.info(f"currently email_df contains: {email_df}")
        try:
            for f in os.listdir(temp_download):
                            os.remove(os.path.join(temp_download, f))
        except:
                 pass
        if len(email_df)>0:
            logging.info("Sending mail now")
            mail_subject = 'JOB SUCCESS - {}'.format(jobname)
            mail_body = '{} completed successfully, Attached invoice file'.format(jobname) 
        else:
            logging.info('send success e-mail')
            mail_subject = 'JOB SUCCESS - {} No file found'.format(jobname)
            mail_body = '{} completed successfully, Attached logs'.format(jobname)
        return mail_body,mail_subject,email_df
    except Exception as e:
        print("Error in main method: ",str(e))
        logging.info("Error in main method: {}".format(str(e)))
        logging.exception(e)
         
def imtt_report_runner():
    try:
        time_start = time.time()
        global logfile,today,temp_download,data_loc,file_loc,job_id,jobname
        logfile = os.getcwd()+'\\logs\\'+'imtt_report.txt'
        logging.basicConfig(level=logging.INFO,
            force=True,
            format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
            filename=logfile)
        logging.warning('Start work at {} ...'.format(time_start))
        logging.warning('Execution started')
        today = date.today()
        temp_download = os.getcwd()+"\\temp_download"
        data_loc = os.getcwd()+"\\data"
        file_loc = os.getcwd() + "\\forIMTTv2"
        credential_dict = config.get_config('IMTT_REPORT_CONVERTER', 'N',other_vert= True)
        database = credential_dict['DATABASE'].split(";")[0]
        warehouse=credential_dict['DATABASE'].split(";")[1]
        tablename = credential_dict['TABLE_NAME']
        jobname = credential_dict['PROJECT_NAME']
        owner = credential_dict['IT_OWNER']
        job_id=np.random.randint(1000000,9999999)
        receiveremail = credential_dict['EMAIL_LIST'].split(";")[0]
        
        ################# testing environment ############
        # warehouse = "BUIT_WH"
        # jobname = "BIO-PAD01 IMTT_REPORT_CONVERTER"
        # database = "BUITDB_DEV"
        # receiveremail = "yashn.jain@biourja.com,imam.khan@biourja.com,yash.gupta@biourja.com,bhavana.kaurav@biourja.com"
        jobname = "BIO-PAD1_" + jobname
        ###############################################3

        log_json = '[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=jobname, database=database,status='STARTED',table_name = tablename,log=log_json,
        warehouse=warehouse,process_owner=owner)
        logging.info("Entered in main")
        mail_subject,mail_body,email_df = main()
        logging.info("Sending success mail")
        log_json = '[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name=jobname, database=database,status='COMPLETED',table_name = tablename,log=log_json, warehouse=warehouse,process_owner=owner)
        bu_alerts.send_mail(receiver_email = receiveremail,mail_subject =mail_subject,mail_body = mail_body,
        multiple_attachment_list= email_df)
    except Exception as e:
        logging.exception(e)
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.datetime.now())+'"}]'
        bu_alerts.bulog(process_name=jobname, database=database,status='FAILED',table_name = tablename, log=log_json, warehouse=warehouse,process_owner=owner)
        bu_alerts.send_mail(receiver_email = receiveremail,mail_subject =f'JOB FAILED - {jobname}',mail_body = f'{jobname} failed, Attached logs',attachment_location = logfile)
        sys.exit(-1)
    finally:
        time_end = time.time()
        logging.warning('It took {} seconds to run.'.format(time_end - time_start))
        print('It took {} seconds to run.'.format(time_end - time_start))


if __name__ == "__main__":
    imtt_report_runner()
    
    