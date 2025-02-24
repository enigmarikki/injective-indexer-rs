# Default target
all: gen

# Run generation targets and then build
gen: gen-proto gen-submods #fix-tonic-types

# Generate proto code and reorganize it into module directories
gen-proto:
	@echo "Creating output directory src/proto..."
	mkdir -p src/proto
	@echo "Running buf generate..."
	buf generate
	@echo "Reorganizing generated proto files based on file names..."
	@for file in src/proto/*.rs; do \
	  base=$$(basename $$file); \
	  if [ "$$base" = "mod.rs" ]; then continue; fi; \
	  if echo "$$base" | grep -q "\.tonic\.rs$$"; then \
	    pkg=$$(echo "$$base" | sed 's/\.tonic\.rs//'); \
	    submod="tonic"; \
	  else \
	    pkg=$$(echo "$$base" | sed 's/\.rs//'); \
	    submod=""; \
	  fi; \
	  dir_path=src/proto/$$(echo $$pkg | tr '.' '/'); \
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
	@find src/proto -type d | while read d; do \
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

# Patch generated Tonic code to fix type resolution issues
fix-tonic-types:
	@echo "Patching generated code for Tonic compatibility..."
	# Replace EnabledCompressionEncodings with tonic::codec::CompressionEncoding
	find ./src/proto -type f -name "*.rs" -exec sed -i 's/EnabledCompressionEncodings/tonic::codec::CompressionEncoding/g' {} \;
	# Replace bare CompressionEncoding with tonic::codec::CompressionEncoding (using word boundaries)
	find ./src/proto -type f -name "*.rs" -exec sed -i 's/\bCompressionEncoding\b/tonic::codec::CompressionEncoding/g' {} \;
	# Replace NamedService in tonic::server with tonic::transport::server::NamedService
	find ./src/proto -type f -name "*.rs" -exec sed -i 's/tonic::server::NamedService/tonic::transport::server::NamedService/g' {} \;
	@echo "Patching complete."

build:
	cargo build

clean-all:
	rm -rf cosmos-sdk ibc-go cometbft wasmd injective-core src/proto

.PHONY: all gen gen-proto gen-submods fix-tonic-types build clean-all