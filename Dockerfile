FROM python:3
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app
COPY parser_app.py /app
WORKDIR /app
ENTRYPOINT python3 parser_app.py
