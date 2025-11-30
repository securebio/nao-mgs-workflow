CC = gcc
CFLAGS = -Wall -Wextra -O3 -std=c99 -march=native
LDFLAGS = -lz

DEDUP_DIR = ../deps/nao_dedup
INCLUDES = -I$(DEDUP_DIR)
OBJ_DIR = ../obj

SRC = similarity_duplicate_marking.c $(DEDUP_DIR)/nao_dedup.c
OBJ = $(OBJ_DIR)/similarity_duplicate_marking.o $(OBJ_DIR)/nao_dedup.o
TARGET = ../bin/similarity_duplicate_marking

all: $(TARGET)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(OBJ_DIR)/similarity_duplicate_marking.o: similarity_duplicate_marking.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

$(OBJ_DIR)/nao_dedup.o: $(DEDUP_DIR)/nao_dedup.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c -o $@ $<

$(TARGET): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET) $(OBJ)
	rm -rf $(OBJ_DIR)

.PHONY: all clean
