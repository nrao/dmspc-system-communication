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


#================
# etransfer
#================
# Putting this here means that every additional simulator image will contain both executables.
# Only the VLBA simulator ever calls etc.
# Only the ETD container ever runs etd.
COPY eTransfer /tmp/etransfer

RUN cd /tmp/etransfer \
 && make \
 && find . -path "*/etc" -type f -executable -exec cp {} /usr/local/bin/etc \; \
 && find . -path "*/etd" -type f -executable -exec cp {} /usr/local/bin/etd \;

COPY . .

RUN python manage.py collectstatic --noinput

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
