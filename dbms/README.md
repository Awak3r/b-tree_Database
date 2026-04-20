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
