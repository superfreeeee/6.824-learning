# parallel version
master: build
	go run mrcoordinator.go pg-*.txt

master-race: build
	go run -race mrcoordinator.go pg-*.txt

worker:
	go run mrworker.go wc.so

build:
	go build -buildmode=plugin ../mrapps/wc.go
	rm mr-out* || true

# test
test:
	bash test-mr.sh

test-more:
	bash test-mr-many.sh 10

# sequential version
seq: build
	go run mrsequential.go wc.so pg*.txt
	more mr-out-0

# clean output
clean:
	rm -rf mr-tmp
	rm -f mr-*
	mkdir mr-tmp
