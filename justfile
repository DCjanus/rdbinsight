# This is a justfile for the rdbinsight project.
# Similiar to Makefile, but in [just](https://github.com/casey/just)

default: prepare

check:
	cargo +nightly fmt --all -- --check
	cargo machete
	cargo clippy --all -- -D warnings
	cargo test --all -- --nocapture

fmt:
	cargo sort --workspace
	cargo +nightly fmt --all

fix: fmt
	cargo fix --allow-dirty --allow-staged

prepare:
    just fix
    just check
    just parser_test # TODO: remove this after active development

clean:
	rm -rf tests/dumps/*.rdb

parser_test:
	cargo test --test parser_test -- --nocapture