.PHONY: clean clean_all function_catalog rdm rpc_smoke rpc_smoke_r

rpc_smoke: check_configure
	$(TEST_RUNNER_RELEASE)

rpc_smoke_r:
	@if command -v Rscript >/dev/null 2>&1; then \
		Rscript test/rpc_smoke.R; \
	else \
		echo "Rscript not found; skipping optional rpc_smoke_r"; \
	fi

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=ducknng
USE_UNSTABLE_C_API=1
TARGET_DUCKDB_VERSION=v1.5.2

all: configure release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean clean_configure

function_catalog:
	python3 function_catalog/generate_function_catalog.py

rdm: function_catalog
	R -e "rmarkdown::render('README.Rmd')"
