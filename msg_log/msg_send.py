import smtplib
from email.mime.text import MIMEText
from email.header import Header

send_acount = [
    {
        'acount': '2285687467@qq.com',
        'password': 'wzekzueiuxpndida'
    },
    {
        'acount': '2291933453@qq.com',
        'password': 'awairimahepgdija'
    }
]

recevier_test = [
    '2291933453@qq.com',
    '2285687467@qq.com'
]
recevier = ['2285687467@qq.com', '3145971793@qq.com']
def send_email(subject, content, test=True):

    acount = send_acount[0]['acount']
    password = send_acount[0]['password']
    mailhost = "smtp.qq.com"

    message = MIMEText(content, 'plain', 'utf-8')
    message['Subject'] = subject
    message['From'] = acount
    if test:
        to_list = recevier_test
    else:
        to_list = recevier
    if test:
        message['To'] = ','.join(to_list)
    else:
        message['To'] = ','.join(to_list)

    try:
        with smtplib.SMTP_SSL(mailhost, 465) as smtp:
            smtp.login(acount, password)
            smtp.sendmail(acount, to_list, message.as_string())
        print('send email success')
    except smtplib.SMTPException as e:
        print(e)

if __name__ == '__main__':
    send_email('test', 'test')