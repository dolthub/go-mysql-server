#
# Darwin: brew install mysql-connector-c
# Linux:  apt-get install libmysqlclient-dev
#
CFLAGS=-Wall `mysql_config --cflags --libs`
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
		CFLAGS += mysqlclient
endif

%.c:
	@echo CFLAGS: $(CFLAGS)
	$(CC) *.c $(CFLAGS)

test:	%.c
	./a.out

clean:
	@rm -f *.o a.out

.PHONY: test clean