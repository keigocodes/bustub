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
