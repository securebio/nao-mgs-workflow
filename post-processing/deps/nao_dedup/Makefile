CC = gcc
CFLAGS = -Wall -Wextra -O3 -std=c99 -march=native -fPIC
LDFLAGS = -shared

SRC = nao_dedup.c
HEADER = nao_dedup.h

TEST_LIB = tests/libnaodedup_test.so

.PHONY: all test-lib clean test

all:
	@echo "This is a library. Use 'make test-lib' to build test library."

# Build shared library for testing
test-lib: $(TEST_LIB)

$(TEST_LIB): $(SRC) $(HEADER)
	@mkdir -p tests
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SRC)
	@echo "Test library built: $(TEST_LIB)"

# Run tests
test: test-lib
	cd tests && pytest -v

clean:
	rm -f $(TEST_LIB)

.PHONY: all test-lib test clean
