FROM python:3.9

WORKDIR /app

COPY . /app

RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/app"

CMD ["bash", "-c", "ray start --head --num-cpus 1 --dashboard-host 0.0.0.0 --include-dashboard true --block"]
