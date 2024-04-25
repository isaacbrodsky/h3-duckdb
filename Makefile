PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=h3ext
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override for linking to H3:
WASM_LINK_TIME_FLAGS=h3/lib/libh3.a
WASM_COMPILE_TIME_COMMON_FLAGS=-DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -DSKIP_EXTENSIONS="parquet;json"
WASM_CXX_MVP_FLAGS=
WASM_CXX_EH_FLAGS=$(WASM_CXX_MVP_FLAGS) -fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1
WASM_CXX_THREADS_FLAGS=$(WASM_COMPILE_TIME_EH_FLAGS) -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -DUSE_PTHREADS=1 -pthread
WASM_LINK_TIME_FLAGS=-O3 -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="_${EXT_NAME}_version,_${EXT_NAME}_init" h3/lib/libh3.a

wasm_mvp:
	mkdir -p build/wasm_mvp
	emcmake cmake $(GENERATOR) $(EXTENSION_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) -Bbuild/wasm_mvp -DCMAKE_CXX_FLAGS="$(WASM_CXX_MVP_FLAGS) -DDUCKDB_CUSTOM_PLATFORM=wasm_mvp" -S duckdb -DDUCKDB_EXPLICIT_PLATFORM="wasm_mvp"
	emmake make -j8 -Cbuild/wasm_mvp
	cd build/wasm_mvp/extension/${EXT_NAME} && emcc $f -o ../../${EXT_NAME}.duckdb_extension.wasm ${EXT_NAME}.duckdb_extension.wasm.lib $(WASM_LINK_TIME_FLAGS)

wasm_eh:
	mkdir -p build/wasm_eh
	emcmake cmake $(GENERATOR) $(EXTENSION_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) -Bbuild/wasm_eh -DCMAKE_CXX_FLAGS="$(WASM_CXX_EH_FLAGS) -DDUCKDB_CUSTOM_PLATFORM=wasm_eh" -S duckdb -DDUCKDB_EXPLICIT_PLATFORM="wasm_eh"
	emmake make -j8 -Cbuild/wasm_eh
	cd build/wasm_eh/extension/${EXT_NAME} && emcc $f -o ../../${EXT_NAME}.duckdb_extension.wasm ${EXT_NAME}.duckdb_extension.wasm.lib $(WASM_LINK_TIME_FLAGS)

wasm_threads:
	mkdir -p ./build/wasm_threads
	emcmake cmake $(GENERATOR) $(EXTENSION_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) -Bbuild/wasm_threads -DCMAKE_CXX_FLAGS="$(WASM_CXX_THREADS_FLAGS) -DDUCKDB_CUSTOM_PLATFORM=wasm_threads" -DCMAKE_C_FLAGS="-pthread" -S duckdb -DDUCKDB_EXPLICIT_PLATFORM="wasm_threads"
	emmake make -j8 -Cbuild/wasm_threads
	cd build/wasm_threads/extension/${EXT_NAME} && emcc $f -o ../../${EXT_NAME}.duckdb_extension.wasm ${EXT_NAME}.duckdb_extension.wasm.lib $(WASM_LINK_TIME_FLAGS) -sSHARED_MEMORY=1 -pthread
