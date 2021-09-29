from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
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
to_mail_list = ["imam.khan@biourja.com", "devina.ligga@biourja.com", "jacob.palacios@biourja.com", "operations@biourja.com"]

headers = {
    "User-Agent": 'Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'}
options = Options()
options.add_argument('--headless')

job_id=np.random.randint(1000000,9999999)
file_loc = os.getcwd()+"\\data"
job_name = "IMTT_REPORT_CONVERTER"
# log progress --
# logfile = 'C:\\AJ\\PowerSignals\\paper_position_report_bnp\\bnp_pdf_Logfile.txt'
logfile = os.getcwd()+'\\'+str("itt_Logfile.txt")+'.txt'

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=logfile)

def pdf_page_breaker(email_date):
    try:
        email_df = []
        for f in os.listdir(temp_download):
            if "shipping report" in f.lower():
                df = read_pdf(temp_download + '\\' + f, pages = 'all', guess = False, stream = True ,
                            pandas_options={'header':None}, area = ["150,50,566,750"], columns = ["90,140,238,270,330,365,367,417,450,480,520,583,640,740"])
                
                main_df = pd.concat(df[:-1], ignore_index=True)
                
                    

                
                m_df = main_df[[2,3,10,7]]
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
                        m_df[3][i] = m_df[2][i+1]
                        
                    else:
                        print("odd ",i)
                        m_df[7][i] = m_df[7][i-1]
                        m_df[3][i] = m_df[2][i]
                        m_df[2][i] = m_df[2][i-1]   
                m_df.columns = ["CUSTOMER NAME", "DESTINATION", "NET GALLONS", "DATE"]
                m_df.to_excel(file_loc+"\\imtt"+email_date+".xlsx", sheet_name = email_date,index=False)

                email_df.append(file_loc+"\\imtt"+email_date+".xlsx")
                shutil.move(temp_download + '\\' +f, file_loc+"\\imtt"+email_date+".pdf")
            else:
                os.remove(temp_download + '\\' + f)
        return email_df
    except Exception as e:
        logging.info(f"Exception caught in pdf_page_breaker {e}")
        raise e

def unzip_downloaded_files(download_path:str):
    print('unzip the downloaded file in folder')
    print(download_path)
    for root, dirs, files in os.walk(download_path):
        if len(files) > 0:
            for file_name in files:
                if '.zip' in file_name: 
                    # dir_name = file_name.replace('.zip','')
                    # if os.path.exists(download_path+dir_name):
                    #     print('directory already exist')
                    # else:
                    #     os.mkdir(download_path+dir_name)
                    # if '.zip' in file_name:
                    with ZipFile(download_path+file_name, 'r') as zipObj:
                        # Extract all the contents of zip file in current directory
                        zipObj.extractall(download_path)
                else:
                    pass
                #delete the folder after this
                if os.path.exists(download_path+file_name) and '.zip' in file_name:
                    os.remove(download_path+file_name)             
        else:
            print('No files avialble')

def download_wait(directory, nfiles = None):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 90:
        time.sleep(1)
       
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True

        for fname in files:
            print(fname)
            if fname.endswith('.crdownload'):
                dl_wait = True
            elif fname.endswith('.tmp'):
                dl_wait = True
            elif fname.endswith('.part'):
                dl_wait = True
            else:
                dl_wait = False

        seconds += 1
    return seconds


def get_email_date(browser):
    try:
    
        logging.info('open outllok in firefox')
        browser.get("https://outlook.office365.com/owa/biourja.com/")
        logging.info('pass user name')
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.ID, "i0116"))).send_keys("ITDevSupport@biourja.com")
        logging.info('pass password')
        time.sleep(1)
        browser.find_element_by_id("idSIButton9").click()
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.ID, "i0118"))).send_keys("Z@^>Nzh'x85]@dL?")
        logging.info('click on search box')
        time.sleep(1)
        browser.find_element_by_id("idSIButton9").click()
        time.sleep(1)
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.ID, "idBtn_Back"))).click()
        logging.info('point cursor on search box')
        time.sleep(5)
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.ID, "searchBoxId-Mail"))).click()
        logging.info('IMTT')
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/div/div[2]/div/input"))).send_keys("IMTT")
        logging.info('Click on search button')
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/button"))).click()
        # browser.find_element_by_xpath("/html/body/div[2]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/button").click()
        time.sleep(10)
        #Selects first mail from all results
        
        download_xpath = '/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[1]/div[2]/div/div/div/div/div/div[6]/div/div'
        WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,download_xpath))).click()
        time.sleep(4)
        # logging.info("getting file date")
        # try:
        #     date_path=WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div/div[1]/div/div/div[2]/div/div[1]")))
        # except:
        #     date_path=WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div[1]/div/div[2]/div/div/div/div/div/div[2]/div/div[1]")))
                                                                                                        
        # file_date = str(date_path.text)
        # if '.PDF' in file_date:
        #     file_date = str(date_path.text).replace('.PDF','')[-8:]
        # elif '.pdf' in file_date:
        #     file_date = str(date_path.text).replace('.pdf','')[-8:]

        # f_month = file_date[:2]
        # f_day =  file_date[2:4]
        # file_date = f_month + f_day + file_date[4:]
        logging.info('get email date and time')
        time.sleep(4)
        try:
            temp_dt = browser.find_element_by_class_name('DWrY3hKxZTZNTwt3mx095')
        except:
            temp_dt = browser.find_element_by_class_name('_24i22iNhbLz_Hc8BeXBUwc')
        time.sleep(4)
        lst_date = temp_dt.text.split()[1].split('/')
        if len(lst_date[0])==1:
            mth = '0'+str(lst_date[0])
        else:
            mth = str(lst_date[0])
        if len(lst_date[1])==1:
            day = '0'+str(lst_date[1])
        else:
            day = str(lst_date[1])
        year = str(lst_date[2])
        email_date = [mth, day, year]
        email_date = ("-").join(email_date)
        
        return email_date
    except Exception as e:
        raise e

def login_and_download(browser, download_path):
    result = False
    try:
        # click on show all 3 attachements
        # /html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div/div/div[2]/div/span/span[1]/button/span/span/span
        try:
            WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/span/button/span/span"))).click()
            
                                                                                                     
        except:
            # try:
            WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div[1]/div/div[2]/div/span/button"))).click()
            # except:
            #     WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div/div/div/div/div[3]/button/span/i"))).click()
            #     time.sleep(5)
            #     WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[10]/div/div/div/div/div/div/ul/li[3]/button/div/span"))).click()
            
        download_time = download_wait(download_path)
        logging.info(f"download_time is {download_time}")
        time.sleep(10)
        # logout --
        logging.info('OPEN MAIN LINK')
        browser.find_element_by_id("O365_MainLink_Me").click()
        logging.info('CLICK ON SIGN OUT BUTTON')
        time.sleep(5)
        browser.find_element_by_xpath('//*[@id="mectrl_body_signOut"]').click()
        time.sleep(4)
        result = True
    except (NoSuchElementException, Exception) as e:
        print(e)
        logging.info(str(e))
        if browser is not None:
            browser.close()
        raise e
    finally:
        if browser is not None:
            browser.close()
        return result

def main():
    ############Uncomment for test ###############
    # email_date = "09-27-2021"
    # pdf_page_breaker(email_date)
    ##############################################
    browser = None
    # remove files present in temp folder before starting main process
    try:
        for f in os.listdir(temp_download):
            os.remove(os.path.join(temp_download, f))
    except:
        pass
    try:
        #getting the current email date to set download folder
        logging.info('get the folder name as email date')
        mime_types=['application/pdf'
                        ,'text/plain',
                        'application/vnd.ms-excel',
                        'test/csv',
                        'application/zip',
                        'application/csv',
                        'text/comma-separated-values','application/download','application/octet-stream'
                        ,'binary/octet-stream'
                        ,'application/binary'
                        ,'application/x-unknown']
                        
        # path=os.getcwd()+'\\'
        # download_path = path
        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.manager.showWhenStarting', False)
        profile.set_preference('browser.download.dir', temp_download)
        profile.set_preference('pdfjs.disabled', True)
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types))
        profile.set_preference('browser.helperApps.neverAsk.openFile',','.join(mime_types))
        # browser = webdriver.Firefox(executable_path='C:\\AJ\\PowerSignals\\paper_position_report_bnp\\geckodriver.exe', firefox_profile=profile)
        browser = webdriver.Firefox(executable_path=os.getcwd()+'\\geckodriver.exe', firefox_profile=profile)
        #getting the current email date to set download folder
        email_date = get_email_date(browser)
        
        logging.info(f'Email date is {email_date}')
        print(f"Email reception date is {email_date}")
        
        date_today = str(datetime.today().strftime("%m%d%Y"))
        print(f"Today\'s date is {date_today}")
        logging.info(f'Today date is {date_today}')
       
        logging.info(f"Email reception date is {email_date}")
        
        print(temp_download)
        
        logging.info('login and download the zip file')
        status = login_and_download(browser, temp_download)
        if status:
            logging.info("download successful")
            logging.info('unzip downloaded file')
            unzip_downloaded_files(temp_download+"\\")
            logging.info("delete duplicate files")
            email_df = pdf_page_breaker(email_date)
            if len(email_df)>0:
                logging.info("Sending mail now")
                send_mail(email_df, subject='JOB SUCCESS - {} {}'.format(job_name, email_date), body='{} completed successfully, Attached invoice file'.format(job_name), to_mail_list=to_mail_list)
            
            else:
                logging.info('send success e-mail')
                log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
                bu_alerts.bulog(process_name="RISK:BNP_PDF", database='POWERDB',status='Completed',table_name = '', row_count=0, log=log_json, warehouse='',process_owner='MANISH')
                bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB SUCCESS - {} No file found'.format(job_name),mail_body = '{} completed successfully, Attached logs'.format(job_name),attachment_location = logfile)

            # else:
            #     logging.info("Files not match")
            #     raise Exception("Files not match")
            # else:
        #         logging.info("Bno folder not found")
        #         # raise Exception
        else:
            logging.info("download failed, aborting process...")
            raise Exception("download failed, aborting process...")
    except Exception as e:
        # if 'Tried to run command without establishing a connection' not in str(e):
        logging.exception(e)
        logging.info('send failure mail')
        logging.info(f'In exception and browser not none: {browser is not None}')
        print(e)
        # if browser is not None:
        #     browser.close()
        logging.info(str(e))
        log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        # bu_alerts.bulog(process_name="RISK:BNP_PDF", database='POWERDB',status='Failed',table_name = '', row_count=0, log=log_json, warehouse='',process_owner='IMAM')
        bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {}'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),
        attachment_location = logfile)
        sys.exit(-1)

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