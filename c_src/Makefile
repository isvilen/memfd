OUT_DIR := $(abspath $(CURDIR)/../priv)

ERTS_DIR := $(shell erl -noshell -s init stop -eval "io:format(\"~s/erts-~s/\", [code:root_dir(), erlang:system_info(version)]).")

CFLAGS := -O3 -fPIC -I $(ERTS_DIR)/include -std=c99 -finline-functions -Wall -Wmissing-prototypes

LDFLAGS += -shared

INFO = echo -e "\033[0;32m===> $1\033[0m"

$(OUT_DIR)/memfd.so: memfd.o
	@$(call INFO,Compiling memfd NIF...)
	mkdir -p $(OUT_DIR)
	$(CC) $^ $(LDFLAGS) -o $@

.PHONY: clean
clean:
	rm -f $(OUT_DIR)/memfd.so memfd.o

