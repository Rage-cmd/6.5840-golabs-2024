run_coordinator:
	rm -rf mr-*
	go build mrcoordinator.go
	go run mrcoordinator.go pg-being_ernest.txt pg-huckleberry_finn.txt pg-dorian_gray.txt

run_worker:
	go build -buildmode=plugin ../mrapps/wc.go
	codesign --force --deep --sign - wc.so
	go build mrworker.go
	go run mrworker.go wc.so

# go run mrworker.go wc.so
# go run mrworker.go wc.so