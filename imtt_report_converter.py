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

temp_download = r"C:\Users\Yashn.jain\Documents\power_automate\imtt_report_converter"+"\\temp_download"
receiver_email = 'imam.khan@biourja.com, yashn.jain@biourja.com'
# mrutunjaya.sahoo@biourja.com, devina.ligga@biourja.com, priyanshi.jhawar@biourja.com, ayushi.joshi@biourja.com'
to_mail_list = ["imam.khan@biourja.com","yashn.jain@biourja.com" ]
# "mrutunjaya.sahoo@biourja.com", "devina.ligga@biourja.com", "priyanshi.jhawar@biourja.com", "ayushi.joshi@biourja.com","jacob.palacios@biourja.com", "operations@biourja.com"]

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



def read_file(filename:str):
    date_file = open(os.getcwd()+"\\"+filename+".txt")
    prev_email = date_file.read()
    date_file.close()
    return prev_email
       
def write_file(filename:str,data:str):
    date_file = open(os.getcwd()+"\\"+filename+".txt","w")
    date_file.write(str(data))
    date_file.close()

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
                # shutil.move(temp_download + '\\' +f, file_loc+"\\imtt"+email_date_time+".pdf")
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
    # email_date = "05-09-2022"
    # email_df = pdf_page_breaker(email_date,email_df)
    # browser = None
    # check = False
    # try:
    ##############################################
    # 
    # remove files present in temp folder before starting main process
    # retry = 0
    # while retry < 2:
    #     if retry > 0:
    #             logging.info("Retrying code now")
       
    #     try:
    #         browser = None
    #         check = False
    #         to_be_saved = None
    #         email_df = []
    #         # raise Exception("test")
    #         #getting the current email date to set download folder
    #         logging.info('get the folder name as email date')
    #         mime_types=['application/pdf'
    #                         ,'text/plain',
    #                         'application/vnd.ms-excel',
    #                         'test/csv',
    #                         'application/zip',
    #                         'application/csv',
    #                         'text/comma-separated-values','application/download','application/octet-stream'
    #                         ,'binary/octet-stream'
    #                         ,'application/binary'
    #                         ,'application/x-unknown']
                            
    #         # path=os.getcwd()+'\\'
    #         # download_path = path
    #         profile = webdriver.FirefoxProfile()
    #         profile.set_preference('browser.download.folderList', 2)
    #         profile.set_preference('browser.download.manager.showWhenStarting', False)
    #         profile.set_preference('browser.download.dir', temp_download)
    #         profile.set_preference('pdfjs.disabled', True)
    #         profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types))
    #         profile.set_preference('browser.helperApps.neverAsk.openFile',','.join(mime_types))
    #         # browser = webdriver.Firefox(executable_path='C:\\AJ\\PowerSignals\\paper_position_report_bnp\\geckodriver.exe', firefox_profile=profile)
    #         browser = webdriver.Firefox(executable_path=os.getcwd()+'\\geckodriver.exe', firefox_profile=profile)
    #         x_path_i = 2
    #         #getting the current email date to set download folder
    #         prev_email = read_file("imtt_prev")
    #         prev_email = datetime.strptime(prev_email, "%m/%d/%Y %I:%M %p")
    #         logging.info(f"prev_mail date is {prev_email}")
    #         i=0
    #         while True:
    #             try:
    #                 for f in os.listdir(temp_download):
    #                     os.remove(os.path.join(temp_download, f))
    #             except:
    #                 pass
    #             email_date, email_date2, x_path_i = get_email_date(browser, i, x_path_i)
    #             if email_date2 == "5/24/2022 9:02 PM":
    #                 i+=1
    #                 continue
    #             email_date_time = datetime.strptime(email_date2, "%m/%d/%Y %I:%M %p")
    #             if to_be_saved is None:
    #                 to_be_saved = email_date2
    #                 logging.info(f"Email datetime to be saved is {to_be_saved}")
    #             logging.info(f'Email date is {email_date}')
    #             # print(f"Email reception date is {email_date}")
                
    #             email_date = datetime.strptime(email_date, "%m-%d-%Y")
    #             date_today = datetime.today() - timedelta(hours=24) #Change 1 to 0 for regular run
    #             ist_today = date_today.astimezone(IST)
    #             logging.info(f"current ist datetime is {ist_today}")
                # ist_today = ist_today.date()
                # logging.info(f"current ist date is {ist_today}")
                # date_today = str((datetime.today() - timedelta(days=0)).strftime("%m-%d-%Y")) #Change 1 to 0 for regular run
                # print(f"Today\'s date is {date_today}")
                # logging.info(f'Today date is {date_today}')
                # check = False
                # logging.info(f"Email reception date is {email_date}")
                # logging.info(f"Email reception datetime is {email_date2}")
                # logging.info(f"email_date time is {email_date_time} and prev_email date time is {prev_email}")
                # ##################For retrial################################

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
        
        # print(e)
        
        logging.info(str(e))
        # try:
        #     if browser is not None:
        #         logging.info("quitting browser")
        #         browser.quit()
        #         browser = None
        # except Exception as e:
        #     # print(e)
        #     logging.exception(e)
        #     pass
        logging.info("retry again")
        # retry +=1 
        # if retry != 2:
        #     time.sleep(60)
        # if retry == 2:
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        # if check:
        #     logging.info("sending file not received failure mail")
        #     bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {} TILL NOW FILE NOT RECEIVED'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),attachment_location = logfile)
        # else:
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