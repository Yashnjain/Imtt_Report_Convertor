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
from common import send_email_with_attachment as send_mail
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

temp_download = os.getcwd()+"\\temp_download"
receiver_email = 'imam.khan@biourja.com, devina.ligga@biourja.com'
to_mail_list = ["imam.khan@biourja.com", "devina.ligga@biourja.com", "arvind.patidar@biourja.com", "rini.gohil@biourja.com", "amit.bhonsle@biourja.com"]



job_id=np.random.randint(1000000,9999999)
today_date = (date.today()-timedelta(days=0)).strftime("%m-%d-%Y") #Change 1 to 0 for regular run
# today_date = "11-16-2021"
data_loc = os.getcwd()+"\\data"
file_loc = os.getcwd() + "\\forIMTTv2"
job_name = "IMTT_REPORT_CONVERTER V2"
# log progress --
# logfile = 'C:\\AJ\\PowerSignals\\paper_position_report_bnp\\bnp_pdf_Logfile.txt'
logfile = os.getcwd()+'\\logs\\'+str("imtt_v2_Logfile.txt")+'.txt'

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=logfile)


def pdf_page_breaker():
    try:
        email_df = []
        for file in glob.glob(file_loc + '\\*pdf'):
            # df = read_pdf(file_loc + '\\' + "imtt"+today_date+".pdf", pages = 'all', guess = False, stream = True ,
            #             pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,270,330,365,367,417,450,480,520,583,640,740"])
            df = read_pdf(file, pages = 'all', guess = False, stream = True ,
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
                    try:
                        m_df[9][i] = int(m_df[9][i]) + int(m_df[9][i+1])
                        m_df[10][i] = int(m_df[10][i]) + int(m_df[10][i+1])
                    except:
                        m_df[9][i] = int(m_df[9][i]) # Add nothing
                        m_df[10][i] = int(m_df[10][i])
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
            # m_df.columns = ["Department", "Document Type", "File Name", "BOL", "BOL Date", "Carrier Name","Customer", "Destination", "Gross Gallon", "Gross Gallon 2", "Net Gallon", "Origin"]
            # file_date = m_df["DATE"][0]
            # file_date = file_date.replace("/", "-")
            file_name = file.split("\\")[-1].replace("imtt","")
            file_name = file_name.replace(".pdf", "")
            m_df.to_excel(data_loc+"\\imtt_v2_"+file_name+".xlsx", sheet_name = today_date,index=False)
            file_name = file.split("\\")[-1].replace("imtt","")
            email_df.append(data_loc+"\\imtt_v2_"+file_name+".xlsx")
            shutil.move(file, data_loc+"\\"+file.split("\\")[-1])
        
            
        return email_df
    except Exception as e:
        logging.info(f"Exception caught in pdf_page_breaker {e}")
        raise e

def main():
    try:
    ############Uncomment for test ###############
        # email_date = "10-08-2021"
        # email_df = pdf_page_breaker(email_date)
        ##############################################
        email_df = pdf_page_breaker()
        if len(email_df)>0:
            logging.info("Sending mail now")
            send_mail(email_df, subject='JOB SUCCESS - {} {}'.format(job_name, today_date), body='{} completed successfully, Attached invoice file'.format(job_name), to_mail_list=to_mail_list)
                
        else:
            logging.info('send success e-mail')
            bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB SUCCESS - {} No file found'.format(job_name),mail_body = '{} completed successfully, Attached logs'.format(job_name),attachment_location = logfile)
    except Exception as e:
        logging.exception(e)
        logging.info("Sending mail now")
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {}'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),attachment_location = logfile)



if __name__ == "__main__":
    logging.info('Execution Started')
    rows=0
    time_start = time.time()
    logging.warning('Start work at {} ...'.format(time_start))
    log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
    
    main()
    time_end = time.time()
    logging.warning('It took {} seconds to run.'.format(time_end - time_start))
    print('It took {} seconds to run.'.format(time_end - time_start))