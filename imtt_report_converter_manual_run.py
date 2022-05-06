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
receiver_email = 'imam.khan@biourja.com, devina.ligga@biourja.com'
to_mail_list = ["imam.khan@biourja.com", "devina.ligga@biourja.com", "jacob.palacios@biourja.com", "operations@biourja.com"]

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

def pdf_page_breaker(email_date_time, email_df):
    try:
        
        for f in os.listdir(temp_download):
            logging.info(f"current checking {f}")
            if "shipping report" in f.lower() or "shipping.pdf" in f.lower() or "shipping repots" in f.lower():
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
                    try:
                        m_df[10][i] = int(m_df[10][i])
                    except:
                        m_df[10][i] = 0
                    if i%2==0 or i == 0:
                        print("even ",i)
                        m_df[3][i] = m_df[2][i+1]
                        
                    else:
                        print("odd ",i)
                        m_df[7][i] = m_df[7][i-1]
                        m_df[3][i] = m_df[2][i]
                        m_df[2][i] = m_df[2][i-1]   
                m_df.columns = ["CUSTOMER NAME", "DESTINATION", "NET GALLONS", "DATE"]
                email_date_time = email_date_time.replace("/", "-")
                email_date_time = email_date_time.replace(":", " ")


                m_df.to_excel(data_loc+"\\imtt"+email_date_time.replace("/", "-")+".xlsx", sheet_name = email_date_time,index=False)

                email_df.append(data_loc+"\\imtt"+email_date_time.replace("/", "-")+".xlsx")
                shutil.move(temp_download + '\\' +f, file_loc+"\\imtt"+email_date_time+".pdf")
            else:
                logging.info(f"removing {f}")
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
        if len(files)==1:
            dl_wait = False
        for fname in files:
            print(fname)
            if fname.endswith('.crdownload'):
                dl_wait = True
            elif fname.endswith('.tmp'):
                dl_wait = True
            elif fname.endswith('.part'):
                dl_wait = True
            
                

        seconds += 1
    return seconds


def get_email_date(browser, i, x_path_i):
    try:
        if i == 0:
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
            try:
                WebDriverWait(browser, 30, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, f"/html/body/div[{x_path_i}]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/div/div[2]/div/input"))).send_keys("IMTT")
            except:
                try:
                    x_path_i += 1
                    WebDriverWait(browser, 10, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, f"/html/body/div[{x_path_i}]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/div/div[2]/div/input"))).send_keys("IMTT")
                except Exception as e:
                    raise e

            logging.info('Click on search button')
            WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH, f"/html/body/div[{x_path_i}]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/button"))).click()
            # browser.find_element_by_xpath("/html/body/div[2]/div/div[1]/div/div[1]/div[2]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[1]/button").click()
            time.sleep(10)
        #Selects first mail from all results
        # download_xpath = '/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[1]/div[2]/div/div/div/div/div/div[7]/div/div' #select 2nd mail instead of 1st
        try:
            download_xpath = f'/html/body/div[{x_path_i}]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[1]/div[2]/div/div/div/div/div/div[{6+i}]/div/div' #change 7 to6  for normal run
            WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,download_xpath))).click()
        except:
            try:
                download_xpath = f'/html/body/div[{x_path_i}]/div/div[2]/div[2]/div[2]/div/div/div/div[2]/div/div[1]/div[2]/div/div/div/div/div/div[{6+i}]/div/div'               
                WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,download_xpath))).click()
            except:
                try:
                    download_xpath = f'/html/body/div[2]/div/div[2]/div[1]/div/div/div/div[3]/div[2]/div/div[1]/div[2]/div/div/div/div/div/div[{6+i}]/div/div'
                    WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,download_xpath))).click()
            
                except Exception as e:
                    raise e
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
        email_date_2 = [temp_dt.text.split()[1], temp_dt.text.split()[2], temp_dt.text.split()[3]]
        email_date_2 = (" ").join(email_date_2)
        email_date_2 = email_date_2.upper()
        
        return email_date, email_date_2, x_path_i
    except Exception as e:
        raise e

def login_and_download(browser, download_path, x_path_i):
    result = False
    not_zip = False
    try:
        # # click on show all 3 attachements
        # WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div/div/div[2]/div/span/button[1]/span/i"))).click()
        # #click on down arrow to expand shipping report options
        # WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[2]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div/div/div[2]/div/div/div[2]/div/div/div/div[3]/button/span/i"))).click()
        # #click on doenload
        # WebDriverWait(browser, 90, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[10]/div/div/div/div/div/div/ul/li[3]/button/div/span"))).click()
        try:                                                                                          
            WebDriverWait(browser, 30, poll_frequency=1).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"button.ms-Button--action:nth-child(3)"))).click()
        except:
            try:
                WebDriverWait(browser, 10, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f"/html/body/div[{x_path_i}]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div/div/div[2]/div/span/button[2]/span/span/span"))).click()
            except:
                try:                                                                                
                    WebDriverWait(browser, 10, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f"/html/body/div[{x_path_i}]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/span/button/span/span"))).click()
                                                                                                                    
                except:
                    try:
                        WebDriverWait(browser, 10, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f"/html/body/div[{x_path_i}]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div[1]/div/div[2]/div/span/button"))).click()
                    except:
                        try:
                            WebDriverWait(browser, 10, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f"/html/body/div[{x_path_i}]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div[1]/div/div/div[1]/div/div[2]/div/div/div/div/div/div[3]/button/span/i"))).click()
                        except:
                            try:
                                logging.info("downloading single available file")
                                WebDriverWait(browser, 30, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f"/html/body/div[{x_path_i}]/div/div[2]/div[2]/div/div/div/div[3]/div[2]/div/div[3]/div/div/div/div/div[2]/div/div/div[1]/div/div/div/div[2]/div/div/div/div/div/div/div[3]/button/span/i"))).click()
                                time.sleep(5)                                                                             
                                WebDriverWait(browser, 30, poll_frequency=1).until(EC.element_to_be_clickable((By.XPATH,f"/html/body/div[{x_path_i+8}]/div/div/div/div/div/div/ul/li[3]/button/div/span"))).click()
                                                                                                                        
                                not_zip = True
                            except Exception as e:
                                raise e
                
            
        download_time = download_wait(download_path)
        logging.info(f"download_time is {download_time}")
        time.sleep(60)
        # logout --
        # logging.info('OPEN MAIN LINK')
        # browser.find_element_by_id("O365_MainLink_Me").click()
        # logging.info('CLICK ON SIGN OUT BUTTON')
        # time.sleep(5)
        # browser.find_element_by_xpath('//*[@id="mectrl_body_signOut"]').click()
        # time.sleep(4)
        result = True
    except (NoSuchElementException, Exception) as e:
        print(e)
        logging.exception(e)
        # if browser is not None:
        #     logging.info("quitting browser")
        #     browser.quit()
        raise e
    finally:
        # if browser is not None:
        #     logging.info("quitting browser")
        #     browser.quit()
        return result, x_path_i, not_zip

def main():
    ############Uncomment for test ###############
    # email_date = "11-18-2021"
    # email_df = pdf_page_breaker(email_date)
    # browser = None
    # check = False
    # try:
    ##############################################
    # 
    # remove files present in temp folder before starting main process
    retry = 0
    while retry < 2:
        if retry > 0:
                logging.info("Retrying code now")
       
        try:
            browser = None
            check = False
            to_be_saved = None
            email_df = []
            # raise Exception("test")
            #getting the current email date to set download folder
            # logging.info('get the folder name as email date')
            # mime_types=['application/pdf'
            #                 ,'text/plain',
            #                 'application/vnd.ms-excel',
            #                 'test/csv',
            #                 'application/zip',
            #                 'application/csv',
            #                 'text/comma-separated-values','application/download','application/octet-stream'
            #                 ,'binary/octet-stream'
            #                 ,'application/binary'
            #                 ,'application/x-unknown']
                            
            # # path=os.getcwd()+'\\'
            # # download_path = path
            # profile = webdriver.FirefoxProfile()
            # profile.set_preference('browser.download.folderList', 2)
            # profile.set_preference('browser.download.manager.showWhenStarting', False)
            # profile.set_preference('browser.download.dir', temp_download)
            # profile.set_preference('pdfjs.disabled', True)
            # profile.set_preference('browser.helperApps.neverAsk.saveToDisk', ','.join(mime_types))
            # profile.set_preference('browser.helperApps.neverAsk.openFile',','.join(mime_types))
            # # browser = webdriver.Firefox(executable_path='C:\\AJ\\PowerSignals\\paper_position_report_bnp\\geckodriver.exe', firefox_profile=profile)
            # browser = webdriver.Firefox(executable_path=os.getcwd()+'\\geckodriver.exe', firefox_profile=profile)
            x_path_i = 2
            #getting the current email date to set download folder
            # prev_email = read_file("imtt_prev")
            # prev_email = datetime.strptime(prev_email, "%m/%d/%Y %I:%M %p")
            i=0
            while True:
                # try:
                #     for f in os.listdir(temp_download):
                #         os.remove(os.path.join(temp_download, f))
                # except:
                #     pass
                # email_date, email_date2, x_path_i = get_email_date(browser, i, x_path_i)
                # email_date_time = datetime.strptime(email_date2, "%m/%d/%Y %I:%M %p")
                # if to_be_saved is None:
                #     to_be_saved = email_date2
                #     logging.info(f"Email datetime to be saved is {to_be_saved}")
                # logging.info(f'Email date is {email_date}')
                # print(f"Email reception date is {email_date}")
                
                # email_date = datetime.strptime(email_date, "%m-%d-%Y")
                # date_today = datetime.today() - timedelta(hours=12) #Change 1 to 0 for regular run
                # ist_today = date_today.astimezone(IST)
                # logging.info(f"current ist datetime is {ist_today}")
                # # ist_today = ist_today.date()
                # # logging.info(f"current ist date is {ist_today}")
                # # date_today = str((datetime.today() - timedelta(days=0)).strftime("%m-%d-%Y")) #Change 1 to 0 for regular run
                # print(f"Today\'s date is {date_today}")
                # logging.info(f'Today date is {date_today}')
                # check = False
                # logging.info(f"Email reception date is {email_date}")
                # logging.info(f"Email reception datetime is {email_date2}")
                
                # # ##################For retrial################################
                # if email_date_time == prev_email and len(email_df)==0:
                #     check = True
                #     if ist_today.replace(tzinfo=None) > email_date_time:  #date_today != email_date: 12 hour old mail will be considered as no new mail recived for today 
                #         raise Exception("File not received till now")
                #     else:
                #         logging.info("File for today already downloaded")
                #         try:
                #             if browser is not None:
                #                 browser.quit()
                #                 browser = None
                #         except Exception as e:
                #             print(e)
                #             logging.exception(e)
                #         sys.exit(0)
                #     # logging.info("sending file not received failure mail")
                #     # bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {} TILL NOW FILE NOT RECEIVED'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),attachment_location = logfile)
                #     # sys.exit(-1)
                    
                    
                # # ########################################################
                
                # elif email_date_time != prev_email:
                #     logging.info('login and download the zip file')
                #     status, x_path_i, not_zip = login_and_download(browser, temp_download, x_path_i)
                #     if status:
                #         logging.info("download successful")
                #         logging.info('unzip downloaded file')
                        # if not not_zip:
                
                # unzip_downloaded_files(temp_download+"\\")
                email_date2 = "2-9-2022 1 24 AM"            
                email_df = pdf_page_breaker(email_date2, email_df)
                print("Done")
                    ##################################################################
                    ###########################################################
                    # else:
                    #     logging.info("download failed, aborting process...")
                    #     raise Exception("download failed, aborting process...")
                    ###########################################################
                # else:
                #     break
                i+=1
            logging.info(f"saved latest mail datetime in file as {to_be_saved}")
            write_file("imtt_prev", to_be_saved)
            logging.info(f"currently email_df contains: {email_df}")
            if len(email_df)>0:
                logging.info("Sending mail now")
                send_mail(email_df, subject='JOB SUCCESS - {} {}'.format(job_name, email_date), body='{} completed successfully, Attached invoice file'.format(job_name), to_mail_list=to_mail_list)
                
            else:
                logging.info('send success e-mail')
                log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
                
                bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB SUCCESS - {} No file found'.format(job_name),mail_body = '{} completed successfully, Attached logs'.format(job_name),attachment_location = logfile)
                    

                
            
            break
        except Exception as e:
            # if 'Tried to run command without establishing a connection' not in str(e):
            logging.exception(e)
            logging.info('send failure mail')
            
            print(e)
            
            logging.info(str(e))
            try:
                if browser is not None:
                    logging.info("quitting browser")
                    browser.quit()
                    browser = None
            except Exception as e:
                print(e)
                logging.exception(e)
                pass
            logging.info("retry again")
            retry +=1 
            if retry != 2:
                time.sleep(60)
            if retry == 2:
                log_json='[{"JOB_ID": "'+str(job_id)+'","CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
                if check:
                    logging.info("sending file not received failure mail")
                    bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {} TILL NOW FILE NOT RECEIVED'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),attachment_location = logfile)
                else:
                    bu_alerts.send_mail(receiver_email = receiver_email,mail_subject ='JOB FAILED - {}'.format(job_name),mail_body = '{} failed, Attached logs'.format(job_name),
                attachment_location = logfile)
                
                sys.exit(-1)
        finally:
            if browser is not None:
                logging.info("quitting browser")
                browser.quit()
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