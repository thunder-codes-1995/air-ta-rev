# syntax=docker/dockerfile:1
FROM python:3.8
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
EXPOSE 8080
CMD ["gunicorn","-w","4","app:run_app()","-b","0.0.0.0:8080"]
