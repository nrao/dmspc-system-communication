#==========================
# etransfer builder stage
#==========================
FROM debian:bookworm-slim AS etransfer-builder 

RUN apt-get update && apt-get install -y gcc g++ make git 

WORKDIR /build 

RUN git clone --branch v2.0 https://github.com/jive-vlbi/etransfer.git

RUN sed -i 's/MACHINE),arm64)/MACHINE),aarch64)/' /build/etransfer/libudt5ab/Makefile

RUN sed -i 's/MACHINE),arm64)/MACHINE),aarch64)/' /build/etransfer/libsrt5ab/Makefile

RUN cd etransfer && make


#==========================
# main application image
#==========================
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN mkdir /service
WORKDIR /service

# Switching to Debian dependencies instead of Alpine ones
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    gcc \
    g++ \
    make \
    git \
    libpq-dev \
    librdkafka-dev \
    libffi-dev \
    libssl-dev \
    libsasl2-dev \
    lsof \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


#================
# app
#================
FROM base AS app

COPY . .

RUN python manage.py collectstatic --noinput

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]


#================
# etransfer etd
#================
FROM base AS etd

COPY --from=etransfer-builder /build/etransfer/*-native-opt/etd /usr/local/bin/


#================
# etransfer etc
#================
FROM base AS etc

COPY --from=etransfer-builder /build/etransfer/*-native-opt/etc /usr/local/bin/


#================
# load-staging-data
#================
FROM postgres:18-alpine AS load-staging-data

WORKDIR /scripts

COPY load-staging-data.sh .

RUN chmod +x load-staging-data.sh

CMD ["./load-staging-data.sh"]


#================
# seaweedfs
#================
FROM chrislusf/seaweedfs:4.40 AS seaweedfs

RUN apk add --no-cache gettext

COPY s3.json.template /s3.json.template
COPY filer.toml.template /filer.toml.template

COPY seaweedfs.sh /seaweedfs.sh
RUN chmod +x /seaweedfs.sh

# Expose ports: 9333 (Master), 8888 (Filer), 8333 (S3)
EXPOSE 9333 8888 8333

ENTRYPOINT ["/seaweedfs.sh"]