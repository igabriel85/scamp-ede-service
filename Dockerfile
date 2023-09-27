FROM python:3.8

EXPOSE 5001

RUN mkdir /app
WORKDIR /app
RUN mkdir /app/data
RUN mkdir /app/etc
RUN mkdir /app/logs
RUN mkdir /app/models

RUN apt-get update

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install git+https://github.com/wannesm/dtaidistance.git#egg=dtaidistance

COPY . /app

CMD ["python", "run.py"]