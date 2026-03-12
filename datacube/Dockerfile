FROM cubejs/cube:latest

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /cube/conf/requirements.txt
RUN pip3 install --break-system-packages -r /cube/conf/requirements.txt
