FROM python:3.8

EXPOSE 5001

RUN mkdir /app
WORKDIR /app
RUN mkdir /app/data
RUN mkdir /app/etc
RUN mkdir /app/logs

RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6  -y

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY . /app

#CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
CMD ["python", "run.py"]