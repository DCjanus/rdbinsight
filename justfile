# This is a justfile for the rdbinsight project.
# Similiar to Makefile, but in [just](https://github.com/casey/just)

default: prepare

check:
	cargo +nightly fmt --all -- --check
	cargo machete
	cargo clippy --all -- -D warnings

test:
    # cargo install cargo-nextest --locked
    # Run all tests with optional parser-trace feature enabled to validate trace instrumentation.
    cargo nextest run --all --status-level=all

fmt:
	cargo sort --workspace
	cargo +nightly fmt --all

fix: fmt
	cargo fix --allow-dirty --allow-staged

prepare:
    just fix
    just check
    just test

clean:
	rm -rf tests/dumps/*.rdb