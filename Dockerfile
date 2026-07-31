FROM python:3.11-slim

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
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python manage.py collectstatic --noinput

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]

#Desmond's Docker Commands for ETransfer - work in progress
#Docker Commands for ETransfer
FROM alphine:3.21 AS builder

#adds the alpine linux package manager (APK) and common C++ tools
RUN apk add --no-cache build base
WORKDIR /eTransferContainer
#will copy the etransfer codebase into the target folder for the container
COPY ["/eTransfer_Codebase/","/eTransferContainer/"]
RUN make
