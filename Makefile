# Default target
all: gen

# Run generation targets and then build
gen: gen-proto clean-proto gen-submods

# List of files to keep
KEEP_FILES := grpc/src/proto/cosmos \
              grpc/src/proto/cosmos/base \
              grpc/src/proto/cosmos/base/query \
              grpc/src/proto/cosmos/base/query/v1beta1 \
              grpc/src/proto/cosmos/base/query/mod.rs \
              grpc/src/proto/cosmos/base/v1beta1 \
              grpc/src/proto/cosmos/base/v1beta1/mod.rs \
              grpc/src/proto/cosmos/base/mod.rs \
              grpc/src/proto/cosmos/distribution \
              grpc/src/proto/cosmos/distribution/v1beta1 \
              grpc/src/proto/cosmos/distribution/v1beta1/mod.rs \
              grpc/src/proto/cosmos/distribution/v1beta1/tonic.rs \
              grpc/src/proto/cosmos/distribution/mod.rs \
              grpc/src/proto/cosmos/query \
              grpc/src/proto/cosmos/query/v1 \
              grpc/src/proto/cosmos/query/v1/mod.rs \
              grpc/src/proto/cosmos/query/mod.rs \
              grpc/src/proto/cosmos/mod.rs \
              grpc/src/proto/injective \
              grpc/src/proto/injective/exchange \
              grpc/src/proto/injective/exchange/v1beta1 \
              grpc/src/proto/injective/exchange/v1beta1/mod.rs \
              grpc/src/proto/injective/exchange/v1beta1/tonic.rs \
              grpc/src/proto/injective/exchange/mod.rs \
              grpc/src/proto/injective/oracle \
              grpc/src/proto/injective/oracle/v1beta1 \
              grpc/src/proto/injective/oracle/v1beta1/mod.rs \
              grpc/src/proto/injective/oracle/v1beta1/tonic.rs \
              grpc/src/proto/injective/oracle/mod.rs \
              grpc/src/proto/injective/stream \
              grpc/src/proto/injective/stream/v1beta1 \
              grpc/src/proto/injective/stream/v1beta1/mod.rs \
              grpc/src/proto/injective/stream/v1beta1/tonic.rs \
              grpc/src/proto/injective/stream/mod.rs \
              grpc/src/proto/injective/mod.rs \
              grpc/src/proto/cosmos.bank.v1beta1.rs \
              grpc/src/proto/cosmos.bank.v1beta1.tonic.rs \
              grpc/src/proto/cosmos.base.query.v1beta1.rs \
              grpc/src/proto/cosmos.base.v1beta1.rs \
              grpc/src/proto/cosmos.distribution.v1beta1.rs \
              grpc/src/proto/cosmos.distribution.v1beta1.tonic.rs \
              grpc/src/proto/cosmos.query.v1.rs \
              grpc/src/proto/injective.exchange.v1beta1.rs \
              grpc/src/proto/injective.exchange.v1beta1.tonic.rs \
              grpc/src/proto/injective.oracle.v1beta1.rs \
              grpc/src/proto/injective.oracle.v1beta1.tonic.rs \
              grpc/src/proto/injective.stream.v1beta1.rs \
              grpc/src/proto/injective.stream.v1beta1.tonic.rs \
              grpc/src/proto/mod.rs
clean-proto:
	@echo "Cleaning proto directory..."
	@# Write the list of paths to keep into a temporary file, one per line
	@echo "$(KEEP_FILES)" | tr ' ' '\n' > .keep_paths.txt
	@# Remove files that are not in the keep list (or not in a kept directory)
	@find grpc/src/proto -type f | while read file; do \
	  if grep -Fxq "$$file" .keep_paths.txt; then \
	    echo "Keeping $$file"; \
	  else \
	    dir=$$(dirname "$$file"); \
	    if grep -Fxq "$$dir" .keep_paths.txt; then \
	      echo "Keeping $$file (inside kept dir $$dir)"; \
	    else \
	      echo "Removing $$file"; \
	      rm -f "$$file"; \
	    fi; \
	  fi; \
	done
	@rm -f .keep_paths.txt
	@echo "Removing empty directories..."
	@# Remove directories that are empty – these are considered "useless"
	@find grpc/src/proto -type d -empty -delete
	@echo "Proto cleanup complete"

# Generate proto code and reorganize it into module directories
gen-proto:
	@echo "Creating output directory grpc/src/proto..."
	mkdir -p grpc/src/proto
	@echo "Running buf generate..."
	buf generate
	@echo "Reorganizing generated proto files based on file names..."
	@for file in grpc/src/proto/*.rs; do \
	  base=$$(basename $$file); \
	  if [ "$$base" = "mod.rs" ]; then continue; fi; \
	  if echo "$$base" | grep -q "\.tonic\.rs$$"; then \
	    pkg=$$(echo "$$base" | sed 's/\.tonic\.rs//'); \
	    submod="tonic"; \
	  else \
	    pkg=$$(echo "$$base" | sed 's/\.rs//'); \
	    submod=""; \
	  fi; \
	  dir_path=grpc/src/proto/$$(echo $$pkg | tr '.' '/'); \
	  echo "Creating directory $$dir_path for package $$pkg..."; \
	  mkdir -p $$dir_path; \
	  if [ "$$submod" = "tonic" ]; then \
	    echo "Creating $$dir_path/tonic.rs to include $$base"; \
	    echo "include!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/src/proto/$$base\"));" > $$dir_path/tonic.rs; \
	  else \
	    echo "Creating $$dir_path/mod.rs to include $$base"; \
	    echo "include!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/src/proto/$$base\"));" > $$dir_path/mod.rs; \
	  fi; \
	done

# Update mod.rs files with nested submodule re-exports
gen-submods:
	@echo "Updating mod.rs files with nested submodule re-exports..."
	@find grpc/src/proto -type d | while read d; do \
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
	rm -rf cosmos-sdk ibc-go cometbft wasmd injective-core grpc/src/proto

.PHONY: all gen gen-proto clean-proto gen-submods build clean-all