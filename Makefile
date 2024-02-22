.PHONY: all clean format debug release duckdb_debug duckdb_release pull update wasm_mvp wasm_eh wasm_threads

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

TEST_PATH="/test/unittest"
DUCKDB_PATH="/duckdb"

# For non-MinGW windows the path is slightly different
ifeq ($(OS),Windows_NT)
# TODO: Doesn't match what should actually happen
# ifneq ($(CXX),g++)
	TEST_PATH="/test/Release/unittest.exe"
	DUCKDB_PATH="/Release/duckdb.exe"
# endif
endif

#### OSX config
OSX_BUILD_FLAG=
ifneq (${OSX_BUILD_ARCH}, "")
	OSX_BUILD_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

#### VCPKG config
VCPKG_TOOLCHAIN_PATH?=
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}' -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif

#### Enable Ninja as generator
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja" -DFORCE_COLORED_OUTPUT=1
endif

EXT_NAME=h3ext

#### Configuration for this extension
EXTENSION_NAME=H3EXT
EXTENSION_FLAGS=\
-DDUCKDB_EXTENSION_NAMES="h3ext" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_LOAD_TESTS=1 \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_INCLUDE_PATH="$(PROJ_DIR)src/include" \
-DDUCKDB_EXTENSION_${EXTENSION_NAME}_TEST_PATH="$(PROJ_DIR)test/sql"

#### Add more of the DuckDB in-tree extensions here that you need (also feel free to remove them when not needed)
EXTRA_EXTENSIONS_FLAG=-DBUILD_EXTENSIONS="tpch"

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 $(EXTENSION_FLAGS) ${EXTRA_EXTENSIONS_FLAG} $(OSX_BUILD_FLAG) $(TOOLCHAIN_FLAGS) -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'
CLIENT_FLAGS:=

#### Main build
# For regular CLI build, we link the quack extension directly into the DuckDB executable
# H3 specific: must be static build
CLIENT_FLAGS=-DDUCKDB_EXTENSION_${EXTENSION_NAME}_SHOULD_LINK=1

debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(BUILD_FLAGS) $(CLIENT_FLAGS) -DCMAKE_BUILD_TYPE=Debug -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(BUILD_FLAGS)  $(CLIENT_FLAGS)  -DCMAKE_BUILD_TYPE=Release -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release

##### Client build
JS_BUILD_FLAGS=-DBUILD_NODE=1 -DDUCKDB_EXTENSION_${EXTENSION_NAME}_SHOULD_LINK=0
PY_BUILD_FLAGS=-DBUILD_PYTHON=1 -DDUCKDB_EXTENSION_${EXTENSION_NAME}_SHOULD_LINK=0

debug_js: CLIENT_FLAGS=$(JS_BUILD_FLAGS)
debug_js: debug
debug_python: CLIENT_FLAGS=$(PY_BUILD_FLAGS)
debug_python: debug
release_js: CLIENT_FLAGS=$(JS_BUILD_FLAGS)
release_js: release
release_python: CLIENT_FLAGS=$(PY_BUILD_FLAGS)
release_python: release

# Main tests
test: test_release
test_release: release
	./build/release/$(TEST_PATH) "$(PROJ_DIR)test/*"
test_debug: debug
	./build/debug/$(TEST_PATH) "$(PROJ_DIR)test/*"

#### Client tests
DEBUG_EXT_PATH='$(PROJ_DIR)build/debug/extension/h3ext/h3ext.duckdb_extension'
RELEASE_EXT_PATH='$(PROJ_DIR)build/release/extension/h3ext/h3ext.duckdb_extension'
test_js: test_debug_js
test_debug_js: debug_js
	cd duckdb/tools/nodejs && ${EXTENSION_NAME}_EXTENSION_BINARY_PATH=$(DEBUG_EXT_PATH) npm run test-path -- "../../../test/nodejs/**/*.js"
test_release_js: release_js
	cd duckdb/tools/nodejs && ${EXTENSION_NAME}_EXTENSION_BINARY_PATH=$(RELEASE_EXT_PATH) npm run test-path -- "../../../test/nodejs/**/*.js"
test_python: test_debug_python
test_debug_python: debug_python
	cd test/python && ${EXTENSION_NAME}_EXTENSION_BINARY_PATH=$(DEBUG_EXT_PATH) python3 -m pytest
test_release_python: release_python
	cd test/python && ${EXTENSION_NAME}_EXTENSION_BINARY_PATH=$(RELEASE_EXT_PATH) python3 -m pytest

#### Misc
format:
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
update:
	git submodule update --remote --merge
pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	cd duckdb && make clean
	cd duckdb && make clean-python

WASM_LINK_TIME_FLAGS=

wasm_mvp:
	mkdir -p build/wasm_mvp
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_mvp -DCMAKE_CXX_FLAGS="-DDUCKDB_CUSTOM_PLATFORM=wasm_mvp" -DSKIP_EXTENSIONS="parquet" -S duckdb $(TOOLCHAIN_FLAGS) $(EXTENSION_FLAGS) -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'
	emmake make -j8 -Cbuild/wasm_mvp
	cd build/wasm_mvp/extension/${EXT_NAME} && emcc $f -sSIDE_MODULE=1 -o ../../${EXT_NAME}.duckdb_extension.wasm -O3 ${EXT_NAME}.duckdb_extension.wasm $(WASM_LINK_TIME_FLAGS)

wasm_eh:
	mkdir -p build/wasm_eh
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_eh -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_eh" -DSKIP_EXTENSIONS="parquet" -S duckdb $(TOOLCHAIN_FLAGS) $(EXTENSION_FLAGS) -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'
	emmake make -j8 -Cbuild/wasm_eh
	cd build/wasm_eh/extension/${EXT_NAME} && emcc $f -sSIDE_MODULE=1 -o ../../${EXT_NAME}.duckdb_extension.wasm -O3 ${EXT_NAME}.duckdb_extension.wasm $(WASM_LINK_TIME_FLAGS)

wasm_threads:
	mkdir -p ./build/wasm_threads
	emcmake cmake $(GENERATOR) -DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 -Bbuild/wasm_threads -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_threads" -DSKIP_EXTENSIONS="parquet" -S duckdb $(TOOLCHAIN_FLAGS) $(EXTENSION_FLAGS) -DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}'
	emmake make -j8 -Cbuild/wasm_threads
	cd build/wasm_threads/extension/${EXT_NAME} && emcc $f -sSIDE_MODULE=1 -o ../../${EXT_NAME}.duckdb_extension.wasm -O3 ${EXT_NAME}.duckdb_extension.wasm $(WASM_LINK_TIME_FLAGS)
