FROM ubuntu
COPY . /app
WORKDIR /app
RUN apt-get update && apt-get install -y \
    python3-pip
RUN pip3 install geos
RUN pip3 install -r requirements.txt
