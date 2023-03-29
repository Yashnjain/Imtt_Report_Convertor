import email
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
from bu_alerts import send_mail
from datetime import datetime, timedelta
from datetime import date
import numpy as np
import pandas as pd
import glob, sys, os, time, logging
import bu_alerts
from zipfile import ZipFile
from tabula import read_pdf
import shutil
import pytz


from common import send_email_with_attachment as send_mail
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

temp_download = os.getcwd()+"\\temp_download"
# receiver_email = 'imam.khan@biourja.com, yashn.jain@biourja.com'
# to_mail_list = ["imam.khan@biourja.com","yashn.jain@biourja.com" ]
receiver_email = 'imam.khan@biourja.com,yashn.jain@biourja.com,mrutunjaya.sahoo@biourja.com,priyanshi.jhawar@biourja.com,ayushi.joshi@biourja.com,itdevsupport@biourja.com'
to_mail_list = ["imam.khan@biourja.com","yashn.jain@biourja.com","mrutunjaya.sahoo@biourja.com","priyanshi.jhawar@biourja.com","ayushi.joshi@biourja.com","jacob.palacios@biourja.com","operations@biourja.com"]

IST = pytz.timezone('Asia/Kolkata')
options = Options()
options.add_argument('--headless')
today = date.today()
job_id=np.random.randint(1000000,9999999)
data_loc = os.getcwd()+"\\data"
file_loc = os.getcwd() + "\\forIMTTv2"
job_name = "IMTT_REPORT_CONVERTER"
# log progress --
# logfile = 'C:\\AJ\\PowerSignals\\paper_position_report_bnp\\bnp_pdf_Logfile.txt'
logfile = os.getcwd()+'\\logs\\'+str(today)+str("imtt_Logfile.txt")+'.txt'

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=logfile)


def pdf_page_breaker(email_df):
    try:
        
        for f in os.listdir(temp_download):
            logging.info(f"current checking {f}")
            if "shipping report" in f.lower() or "shipping.pdf" in f.lower() or "shipping repots" in f.lower() or "transaction reoprt" in f.lower():
                # df = read_pdf(temp_download + '\\' + f, pages = 'all', guess = False, stream = True ,
                #             pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,270,330,365,367,417,450,480,520,583,640,740"])
                df = read_pdf(temp_download + '\\' + f, pages = 'all', guess = False, stream = True ,
                            pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,330,367,417,450,485,520,550,583,640,740"])
                
                main_df = pd.concat(df[:-1], ignore_index=True)
                
                    

                
                # m_df = main_df[[2,3,10,7]]
                m_df = main_df[[2,3,8,5]]
                m_df = m_df[m_df[3].notna()]
                # m_df.dropna(inplace=True)
                m_df.reset_index(drop=True, inplace=True)
                try:
                    if m_df[3].tail(1).str.contains("TOTA").bool():
                        m_df.drop(m_df.tail(1).index,inplace=True) # remove total
                except:
                    pass
                for i in range(len(m_df)):
                    try:
                        # m_df[10][i] = int(m_df[10][i])
                        m_df[8][i] = int(m_df[8][i])
                    except:
                        # m_df[10][i] = 0
                        m_df[8][i] = 0
                    if i%2==0 or i == 0:
                        # print("even ",i)

                        m_df[3][i] = m_df[2][i+1]
                        
                    else:
                        # print("odd ",i)
                        # m_df[7][i] = m_df[7][i-1]
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
        logging.info(f"Exception caught in pdf_page_breaker {e}")
        raise e


def main():
    try:
    ############Uncomment for test ###############
        email_df = []
        email_df = pdf_page_breaker(email_df)

        # logging.info(f"saved latest mail datetime in file as {to_be_saved}")
        # write_file("imtt_prev", to_be_saved)
        logging.info(f"currently email_df contains: {email_df}")
        try:
            for f in os.listdir(temp_download):
                            os.remove(os.path.join(temp_download, f))
        except:
                 pass
        if len(email_df)>0:
            logging.info("Sending mail now")
            send_mail(email_df, subject='JOB SUCCESS - {}'.format(job_name), body='{} completed successfully, Attached invoice file'.format(job_name), to_mail_list=to_mail_list)
            
        else:
            logging.info('send success e-mail')
            log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
            
            bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB SUCCESS - {} No file found'.format(job_name),mail_body = '{} completed successfully, Attached logs'.format(job_name),attachment_location = logfile)
     

                
            
            # break
    except Exception as e:
        # if 'Tried to run command without establishing a connection' not in str(e):
        logging.exception(e)
        logging.info('send failure mail')
        logging.info(str(e))
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {}'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),
        attachment_location = logfile)
        
        sys.exit(-1)

if __name__ == "__main__":
    logging.info('Execution Started')
    rows=0
    time_start = time.time()
    logging.warning('Start work at {} ...'.format(time_start))
    log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
    
    main()
    
    time_end = time.time()
    logging.warning('It took {} seconds to run.'.format(time_end - time_start))
    # print('It took {} seconds to run.'.format(time_end - time_start))