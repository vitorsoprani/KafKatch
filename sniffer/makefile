C_SRC=$(wildcard src/*.c)
H_SRC=$(wildcard src/*.h)

OBJ=$(patsubst src/%.c, obj/%.o, $(C_SRC))

CC=gcc
CC_FLAGS=-c -std=gnu99 -Wall -Wextra -pedantic

# adicionar as bibliotecas necessárias (-lpcap -lrdkafka -lpthread)
LD_FLAGS=-lpcap -lrdkafka

all: sniffer

sniffer: $(OBJ)
	$(CC) $^ -o $@ $(LD_FLAGS)

obj/%.o: src/%.c $(H_SRC) | obj
	$(CC) $(CC_FLAGS) $< -o $@

obj:
	mkdir -p obj

clean:
	rm -rf obj sniffer

debug: CC_FLAGS += -g -O0
debug: clean sniffer

.PHONY: all clean debug
