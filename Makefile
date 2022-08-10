.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif

pull:
	git submodule init
	git submodule update --recursive

update_deps: pull
	git submodule update --reucrsive --upstream

clean:
	rm -rf build
	rm -rf duckdb/build
	rm -rf h3/build

duckdb_debug:
	cd duckdb && \
	BUILD_TPCH=1 make debug

duckdb_release:
	cd duckdb && \
	BUILD_TPCH=1 make release

debug: pull
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/debug/src ${OSX_BUILD_UNIVERSAL_FLAG}  ../.. && \
	cmake --build .

release: pull
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/release/src ${OSX_BUILD_UNIVERSAL_FLAG} ../.. && \
	cmake --build .

test: release duckdb_release
	../duckdb/build/release/test/unittest --test-dir . "[h3]"

format:
	cp duckdb/.clang-format .
	clang-format --sort-includes=0 --style=file -i h3-extension.cpp h3_common.cpp h3_functions/h3_valid.cpp h3_functions/h3_cell_to_parent.cpp h3_functions/h3_latlng_to_cell.cpp
	# cmake-format -i CMakeLists.txt
	rm .clang-format

update:
	git submodule update --remote --merge
