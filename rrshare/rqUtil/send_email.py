#!/usr/bin/python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


# OK !!!
#sender_qq为发件人的qq号码
sender_qq = 'romepeng'
#pwd为qq邮箱的授权码
pwd = 'fcthsbywwwyjbhjd'
#收件人邮箱receiver
receivers = ['rbobowang@hotmail.com','romepeng@outlook.com']  
#msg['To'] = ','.join(receiver) # 这里必须要把多个邮箱按照逗号拼接为字符串
#邮件的正文内容
mail_content = '你好，我是来自QQ ，romepeng,现在在进行一项用python登录qq邮箱发邮件的测试'
#邮件标题
mail_title = 'qq_romepeng 的邮件'

def send_mail(sender_qq='',pwd='',receiver='',mail_title='',mail_content=''):
	# qq邮箱smtp服务器
	host_server = 'smtp.qq.com'
	sender_qq_mail = 'romepeng@qq.com' 
	
	#ssl登录
	smtp = smtplib.SMTP_SSL(host_server, 465)
	#set_debuglevel()是用来调试的。参数值为1表示开启调试模式，参数值为0关闭调试模式
	smtp.set_debuglevel(1)
	smtp.ehlo(host_server)
	smtp.login(sender_qq, pwd)

	msg = MIMEText(mail_content, "plain", 'utf-8')
	msg["Subject"] = Header(mail_title, 'utf-8')
	msg["From"] = sender_qq_mail
	msg["To"] = ''.join(receivers)

	try:
		smtp.sendmail(sender_qq_mail, receivers, msg.as_string())
		print('sendmail success !')
	except smtplib.SMTPException as e:
		print(e)
	finally:
		smtp.quit()

if  __name__ == '__main__':
	send_mail(sender_qq=sender_qq,pwd=pwd,receiver=receivers,mail_title=mail_title,mail_content=mail_content)
	#注意MIMEText函数中的第二个参数为“plain”时，发送的是text文本。如果为“html”，则能发送网页格式文本邮件。

