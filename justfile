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
	cargo +nightly fix --allow-dirty --allow-staged

before_commit:
    just fix
    just check
    just test

clean:
	rm -rf tests/dumps/*.rdb
	rm -f ./rdb_report_*.html

init_test:
    @ if ! cargo nextest --version > /dev/null 2>&1; then cargo install cargo-nextest --locked; fi
    @ if ! grcov --version > /dev/null 2>&1; then cargo install grcov --locked; fi
    @ if ! cargo llvm-cov --version > /dev/null 2>&1; then cargo install cargo-llvm-cov --locked; fi

coverage: init_test
    mkdir -p target/coverage
    CARGO_LLVM_COV_SETUP=yes cargo +nightly llvm-cov nextest --lcov --branch --output-path target/coverage/lcov.info --status-level=all
    grcov target/coverage/lcov.info --output-types html --source-dir . --branch --output-path target/coverage
    @echo "Report ready: target/coverage/html/index.html"

up_dev:
    docker-compose -f dev/docker-compose.yml up -d --force-recreate --renew-anon-volumes --wait

down_dev:
    docker-compose -f dev/docker-compose.yml down

demo with_data='true': up_dev
    if {{with_data}} == 'true'; then cargo run --release --bin fill_redis_memory -- 'redis://127.0.0.1:6380' '512M'; fi
    cargo run --release --bin rdbinsight -- dump from-standalone --addr '127.0.0.1:6380' --cluster 'dev-test-cluster' into-clickhouse --url 'http://rdbinsight:rdbinsight@127.0.0.1:8124?database=rdbinsight' --auto-create-tables
    cargo run --release --bin rdbinsight -- report from-clickhouse --cluster 'dev-test-cluster' --url 'http://rdbinsight:rdbinsight@127.0.0.1:8124?database=rdbinsight' --output ./rdb_report_from_clickhouse.html
    cargo run --release --bin rdbinsight -- dump from-standalone --addr '127.0.0.1:6380' --cluster 'dev-test-cluster' into-parquet --dir ./tmp/parquet_output
    cargo run --release --bin rdbinsight -- report from-parquet --cluster 'dev-test-cluster' --dir ./tmp/parquet_output --output ./rdb_report_from_parquet.html

verify_version tag='':
    #!/usr/bin/env bash
    TAG="{{tag}}"; TAG="${TAG#tag=}";
    if [ -z "$TAG" ]; then
        TAG="$(git describe --tags --exact-match 2>/dev/null || true)"
    fi
    if [ -z "$TAG" ]; then
        echo "HEAD commit has no tag; specify via 'just verify_version tag=vX.Y.Z'." >&2
        exit 1
    fi
    node .github/actions/verify-version-consistency/index.js "$TAG"