# Default target
all: gen

# Run generation targets and then build
gen: gen-proto gen-submods clean-proto

# List of files to keep
KEEP_FILES := grpc/proto/cosmos \
              grpc/proto/cosmos/base \
              grpc/proto/cosmos/base/query \
              grpc/proto/cosmos/base/query/v1beta1 \
              grpc/proto/cosmos/base/query/mod.rs \
              grpc/proto/cosmos/base/v1beta1 \
              grpc/proto/cosmos/base/v1beta1/mod.rs \
              grpc/proto/cosmos/base/mod.rs \
              grpc/proto/cosmos/distribution \
              grpc/proto/cosmos/distribution/v1beta1 \
              grpc/proto/cosmos/distribution/v1beta1/mod.rs \
              grpc/proto/cosmos/distribution/v1beta1/tonic.rs \
              grpc/proto/cosmos/distribution/mod.rs \
              grpc/proto/cosmos/query \
              grpc/proto/cosmos/query/v1 \
              grpc/proto/cosmos/query/v1/mod.rs \
              grpc/proto/cosmos/query/mod.rs \
              grpc/proto/cosmos/mod.rs \
              grpc/proto/injective \
              grpc/proto/injective/exchange \
              grpc/proto/injective/exchange/v1beta1 \
              grpc/proto/injective/exchange/v1beta1/mod.rs \
              grpc/proto/injective/exchange/v1beta1/tonic.rs \
              grpc/proto/injective/exchange/mod.rs \
              grpc/proto/injective/oracle \
              grpc/proto/injective/oracle/v1beta1 \
              grpc/proto/injective/oracle/v1beta1/mod.rs \
              grpc/proto/injective/oracle/v1beta1/tonic.rs \
              grpc/proto/injective/oracle/mod.rs \
              grpc/proto/injective/stream \
              grpc/proto/injective/stream/v1beta1 \
              grpc/proto/injective/stream/v1beta1/mod.rs \
              grpc/proto/injective/stream/v1beta1/tonic.rs \
              grpc/proto/injective/stream/mod.rs \
              grpc/proto/injective/mod.rs \
              grpc/proto/cosmos.bank.v1beta1.rs \
              grpc/proto/cosmos.bank.v1beta1.tonic.rs \
              grpc/proto/cosmos.base.query.v1beta1.rs \
              grpc/proto/cosmos.base.v1beta1.rs \
              grpc/proto/cosmos.distribution.v1beta1.rs \
              grpc/proto/cosmos.distribution.v1beta1.tonic.rs \
              grpc/proto/cosmos.query.v1.rs \
              grpc/proto/injective.exchange.v1beta1.rs \
              grpc/proto/injective.exchange.v1beta1.tonic.rs \
              grpc/proto/injective.oracle.v1beta1.rs \
              grpc/proto/injective.oracle.v1beta1.tonic.rs \
              grpc/proto/injective.stream.v1beta1.rs \
              grpc/proto/injective.stream.v1beta1.tonic.rs \
              grpc/proto/mod.rs

clean-proto:
	@echo "Cleaning proto directory..."
	@find grpc/proto -type f -not -path "*/\.*" | while read file; do \
		keep=false; \
		for pattern in $(KEEP_FILES); do \
			if [ "$$file" = "$$pattern" ] || [ -d "$$pattern" -a "$$(echo $$file | grep -c ^$$pattern/)" -gt 0 ]; then \
				keep=true; \
				break; \
			fi; \
		done; \
		if [ "$$keep" = "false" ]; then \
			echo "Removing $$file"; \
			rm -f "$$file"; \
		fi; \
	done
	@echo "Proto cleanup complete"


# Generate proto code and reorganize it into module directories
gen-proto:
	@echo "Creating output directory grpc/proto..."
	mkdir -p grpc/proto
	@echo "Running buf generate..."
	buf generate
	@echo "Reorganizing generated proto files based on file names..."
	@for file in grpc/proto/*.rs; do \
	  base=$$(basename $$file); \
	  if [ "$$base" = "mod.rs" ]; then continue; fi; \
	  if echo "$$base" | grep -q "\.tonic\.rs$$"; then \
	    pkg=$$(echo "$$base" | sed 's/\.tonic\.rs//'); \
	    submod="tonic"; \
	  else \
	    pkg=$$(echo "$$base" | sed 's/\.rs//'); \
	    submod=""; \
	  fi; \
	  dir_path=grpc/proto/$$(echo $$pkg | tr '.' '/'); \
	  echo "Creating directory $$dir_path for package $$pkg..."; \
	  mkdir -p $$dir_path; \
	  if [ "$$submod" = "tonic" ]; then \
	    echo "Creating $$dir_path/tonic.rs to include $$base"; \
	    echo "include!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/grpc/proto/$$base\"));" > $$dir_path/tonic.rs; \
	  else \
	    echo "Creating $$dir_path/mod.rs to include $$base"; \
	    echo "include!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/grpc/proto/$$base\"));" > $$dir_path/mod.rs; \
	  fi; \
	done

# Update mod.rs files with nested submodule re-exports
gen-submods:
	@echo "Updating mod.rs files with nested submodule re-exports..."
	@find grpc/proto -type d | while read d; do \
	  for sub in $$(find "$$d" -mindepth 1 -maxdepth 1 -type d); do \
	    subname=$$(basename $$sub); \
	    if [ -f "$$d/mod.rs" ]; then \
	      if ! grep -q "pub mod $$subname;" "$$d/mod.rs"; then \
	        echo "Adding \"pub mod $$subname;\" to $$d/mod.rs"; \
	        echo "pub mod $$subname;" >> "$$d/mod.rs"; \
	      fi; \
	    else \
	      echo "pub mod $$subname;" > "$$d/mod.rs"; \
	    fi; \
	  done; \
	done
	@echo "Nested mod.rs files updated."

build:
	cargo build

clean-all:
	rm -rf cosmos-sdk ibc-go cometbft wasmd injective-core grpc/proto

.PHONY: all gen gen-proto gen-submods fix-tonic-types build clean-all