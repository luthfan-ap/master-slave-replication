FROM postgres:14

# install python
RUN apt-get update && apt-get install -y python3 python3-pip

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY app.py .
COPY setup-slave.sh /setup-slave.sh

RUN chmod +x /setup-slave.sh

CMD ["/bin/bash", "/setup-slave.sh"]
