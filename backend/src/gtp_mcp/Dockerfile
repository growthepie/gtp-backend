FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd --system --uid 10001 mcp && mkdir /data && chown mcp /data
USER mcp

ENV PORT=8080
ENV MCP_USAGE_DB=/data/usage.sqlite
VOLUME /data

CMD ["python", "server.py"]
