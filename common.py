import email
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText  # Added
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
import datetime
from email import encoders
import os

smtp_user_name = 'biourjapowerdata@biourja.com'
smtp_password = r'bY3mLSQ-\Q!9QmXJ'

def send_email_with_attachment(file_location:list, subject:str, body: str, to_mail_list:list):
    """
    Sends an email with pdf attachment

    Args:
        file_location - File location from where we need to attach file
    """
    try:
        #to_mail_list = ['manish.gupta@biourja.com']
        # Get file name from file locaiton
        # path, file_name = os.path.split(file_location)
        # Siging into  email 
        smtp = smtplib.SMTP(host='us-smtp-outbound-1.mimecast.com', port=587)
        smtp.starttls()
        smtp.login(smtp_user_name, smtp_password)
        # Define the mime part
        msg = MIMEMultipart()
        # Email Details
        msg["To"] = ", ".join(to_mail_list)
        msg["From"] = smtp_user_name
        msg["Subject"] = subject
        msg["Body"] = body
        body_mimed = MIMEText(body, 'plain')
        # Attached desiredpdf file

        for f in file_location:
            path, file_name = os.path.split(f)
            binary_file = open(f, 'rb')
            # if "csv" in file_name:
            #     payload = MIMEBase('application', 'vnd.ms-excel', Name=file_name)
            # else:
            try:
                payload = MIMEBase('application', 'octate-stream', Name=file_name)
            except:
                payload = MIMEBase('application', 'octet-stream', Name=file_name)
            payload.set_payload((binary_file).read())
            #enconding the binary into base64
            encoders.encode_base64(payload)
            payload.add_header('Content-Decomposition', 'attachment', filename=file_name)
            msg.attach(payload)
        msg.attach(body_mimed)
        smtp.sendmail(smtp_user_name, to_mail_list, msg.as_string())
    except Exception as ex:
        print('Error in sending mail details {}'.format(str(ex)))
    finally:
        smtp.quit()
