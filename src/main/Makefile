run_coordinator:
	rm -rf mr-*
	go build mrcoordinator.go
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrcoordinator.go pg-dorian_gray.txt pg-huckleberry_finn.txt

run_worker:
	go build mrworker.go
	go run mrworker.go wc.so

# go run mrworker.go wc.so
# go run mrworker.go wc.so