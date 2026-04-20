# Coursework DBMS

Skeleton project for the coursework DBMS.

## Build

```bash
cmake -S . -B build
cmake --build build -j 4
```

## Run

```bash
./build/prog
```

## Tests

```bash
# build test binaries
cmake --build build -j 4

# run all tests (single entrypoint)
cmake --build build --target run_all_tests
```

Test binaries are placed in:

```text
build/tests/bin
```

## gRPC

```bash
# start gRPC server
./build/dbms_grpc_server 0.0.0.0:50051

# run gRPC client (interactive)
./build/dbms_grpc_client 127.0.0.1:50051

# run gRPC client with script
./build/dbms_grpc_client 127.0.0.1:50051 /path/to/script.sql
```
