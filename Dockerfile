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

# CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

CMD ["uvicorn", "asgi:application", "--host", "0.0.0.0", "--port", "8000"]


#===============
# expedat server
#===============
FROM base AS expedat-server

COPY /expedat/servedat /usr/local/bin/


#===============
# expedat client
#===============
FROM base AS expedat-client

COPY /expedat/movedat /usr/local/bin/

COPY /expedat/mtping /usr/local/bin/



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