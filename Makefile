# Convenience wrapper around CMake.
#   make build             configure (if needed) and build everything into build/
#   make test              build and run every test via ctest
#   make test T=<regex>    run only tests whose name matches, e.g. make test T=ORSet
#   make clean             delete the build directory

.PHONY: build configure test clean

configure:
	cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER=address -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++

build: configure
	cmake --build build -j $(shell nproc)

test: configure
	cmake --build build -j $(shell nproc) --target build-tests
	ctest --test-dir build --output-on-failure $(if $(T),-R '$(T)')

clean:
	rm -rf build
