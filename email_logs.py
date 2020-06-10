from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import date
import smtplib
import config
import os

today = date.today()

msg = MIMEMultipart()
msg['From'] = config.email_user
msg['To'] = config.email_send
msg['Subject'] = 'JitBit Logs - ' + str(today)


body = 'JitBit logs'
msg.attach(MIMEText(body,'plain'))

today_file = "logfile-{0}.txt".format(str(today))
orig_name = os.path.join('log_file', today_file)
mgmt_name = os.path.join('log_file_mgmt', today_file)
filename_list = [orig_name, mgmt_name]

for filename in filename_list:
    attachment = open(filename,'rb')

    part = MIMEBase('application','octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',"attachment; filename= "+filename)

    msg.attach(part)
    text = msg.as_string()
    server = smtplib.SMTP('smtp.office365.com',587)
    server.starttls()
    server.login(config.email_user,config.email_password)


server.sendmail(config.email_user,config.email_send,text)
server.quit()