# This is a justfile for the rdbinsight project.
# Similiar to Makefile, but in [just](https://github.com/casey/just)

default: prepare

check:
	cargo +nightly fmt --all -- --check
	cargo machete
	cargo clippy --all -- -D warnings

test:
    cargo test --all -- --nocapture

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