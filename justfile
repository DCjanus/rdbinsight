# This is a justfile for the rdbinsight project.
# Similiar to Makefile, but in [just](https://github.com/casey/just)

default: before_commit

check:
	cargo +nightly fmt --all -- --check
	cargo machete
	cargo clippy --all -- -D warnings

test: init_test
    cargo nextest run --all --status-level=all

fmt:
	cargo sort --workspace
	cargo +nightly fmt --all

fix: fmt
	cargo fix --allow-dirty --allow-staged

before_commit:
    just fix
    just check
    just test

clean:
	rm -rf tests/dumps/*.rdb

init_test:
    if ! cargo nextest --version > /dev/null 2>&1; then cargo install cargo-nextest --locked; fi
    if ! grcov --version > /dev/null 2>&1; then cargo install grcov --locked; fi
    if ! cargo llvm-cov --version > /dev/null 2>&1; then cargo install cargo-llvm-cov --locked; fi

coverage: init_test
    mkdir -p target/coverage
    CARGO_LLVM_COV_SETUP=yes cargo +nightly llvm-cov nextest --lcov --branch --output-path target/coverage/lcov.info --status-level=all
    grcov target/coverage/lcov.info --output-types html --source-dir . --branch --output-path target/coverage
    @echo "Report ready: target/coverage/html/index.html"