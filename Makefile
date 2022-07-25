build:
	go build -o bin/fraudSync

clean:
ifneq ($(wildcard bin/fraudSync),)
	rm bin/fraudSync
endif

run: build
	./bin/fraudSync

.PHONY: build clean run
