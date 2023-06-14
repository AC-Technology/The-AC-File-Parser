import smtplib

email = 'theacnotifications@gmail.com'
password = 'acsetup!1'
contact = 'lbartlett@jointheac.com'

server = smtplib.SMTP("smtp.gmail.com",587)

server.starttls()

server.login(email,password)

server.sendmail(email,contact,"The records have been uploaded.")
print("mail sent")

server.quit()
