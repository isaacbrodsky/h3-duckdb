.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

OSX_ARCH_FLAG=
ifneq (${OSX_BUILD_ARCH}, "")
	OSX_ARCH_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_EXTENSIONS="tpch" ${OSX_ARCH_FLAG}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS= \
-DDUCKDB_EXTENSION_NAMES="h3ext" \
-DDUCKDB_EXTENSION_H3EXT_PATH="$(PROJ_DIR)" \
-DDUCKDB_EXTENSION_H3EXT_SHOULD_LINK=0 \
-DDUCKDB_EXTENSION_H3EXT_LOAD_TESTS=1 \
-DDUCKDB_EXTENSION_H3EXT_TEST_PATH=$(PROJ_DIR)test \
-DDUCKDB_EXTENSION_H3EXT_INCLUDE_PATH="$(PROJ_DIR)include" \

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	cd duckdb && make clean
	rm -rf h3/build

# Main build
debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DEXTENSION_STATIC_BUILD=1 -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb/ -B build/release && \
	cmake --build build/release --config Release

test: test_release

test_release: release
	./build/release/test/unittest "$(PROJ_DIR)test/*"

test_debug: debug
	./build/release/test/unittest "$(PROJ_DIR)test/*"

#format:
#	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
#	cmake-format -i CMakeLists.txt

#update:
#	git submodule update --remote --merge

#
#.PHONY: all clean format debug release duckdb_debug duckdb_release pull update_deps
#
#all: release
#
#OSX_BUILD_UNIVERSAL_FLAG=
#ifeq (${OSX_BUILD_UNIVERSAL}, 1)
#	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
#endif
#
#pull:
#	git submodule init
#	git submodule update --recursive
#
update_deps: pull
	git submodule update --remote --checkout
#
#clean:
#	rm -rf build
#	rm -rf duckdb/build
#	rm -rf h3/build
#
#duckdb_debug:
#	cd duckdb && \
#	BUILD_TPCH=1 make debug
#
#duckdb_release:
#	cd duckdb && \
#	BUILD_TPCH=1 make release
#
#debug: pull
#	mkdir -p build/debug && \
#	cd build/debug && \
#	cmake -DCMAKE_BUILD_TYPE=Debug -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/debug/src ${OSX_BUILD_UNIVERSAL_FLAG}  ../.. && \
#	cmake --build .
#
#release: pull
#	mkdir -p build/release && \
#	cd build/release && \
#	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/release/src ${OSX_BUILD_UNIVERSAL_FLAG} ../.. && \
#	cmake --build .
#
#test: release duckdb_release
#	./duckdb/build/release/test/unittest --test-dir . "[h3]"
#
