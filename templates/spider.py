import smtplib
import ssl
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict


class Spider(ABC):
    url: str
    mail: str
    headers: Dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"
    }

    def __init__(self, url, mail) -> None:
        self.url = url
        self.mail = mail

    def send_mail(self, auth, message, subject=""):
        smtp_server = "smtp.gmail.com"
        port = 587

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = auth[0]
        msg["To"] = self.mail
        body_text = MIMEText(message, "plain")
        msg.attach(body_text)

        context = ssl.create_default_context()

        try:
            server = smtplib.SMTP(smtp_server, port)
            server.ehlo()  # check connection
            server.starttls(context=context)  # Secure the connection
            server.ehlo()  # check connection
            server.login(*auth)

            server.sendmail(auth[0], self.mail, msg.as_string())

        except Exception as e:
            print(e)
        finally:
            server.quit()

    @abstractmethod
    def refresh(self) -> None:
        pass
