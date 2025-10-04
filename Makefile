APP_NAME=redis-go
SRC=app/*.go
BIN=$(PWD)/$(APP_NAME)
SYMLINK=/usr/local/bin/$(APP_NAME)

.PHONY: all build run install clean

all: build

build:
	go build -o $(APP_NAME) $(SRC)

run: build
	./$(APP_NAME)

install: build
	ln -sf $(BIN) $(SYMLINK)

clean:
	rm -f $(APP_NAME)
