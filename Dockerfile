FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libc-dev \
        libffi-dev \
        libpq-dev \
        build-essential \
    rm -rf /var/lib/apt/lists/*


WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY main.py .

CMD ["faststream", "run", "main:app"]
