# ─── server ─────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS server

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY build/prog /usr/local/bin/prog
COPY build/dbms_grpc_server /usr/local/bin/dbms_grpc_server

VOLUME ["/data"]
EXPOSE 50051

ENV DBMS_DATA_ROOT=/data

ENTRYPOINT ["/usr/local/bin/dbms_grpc_server"]
CMD ["0.0.0.0:50051", "/data"]

# ─── client ─────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS client

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY build/dbms_grpc_client /usr/local/bin/dbms_grpc_client

ENTRYPOINT ["/usr/local/bin/dbms_grpc_client"]
