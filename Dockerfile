# Python base image
FROM python:3.9-slim

WORKDIR /app

# installing the requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# copying the python file
COPY app.py app.py

# setting the command to run the python script
CMD ["python", "app.py"]