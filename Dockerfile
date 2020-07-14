FROM python:3
RUN pip3 install firebase-admin
RUN pip3 install python-dateutil
RUN pip3 install fpdf
RUN pip3 install pytz
RUN pip3 install APScheduler
CMD cd /app
# docker run --rm -v %cd%:/app -w /app yt_download python ytDownload.py