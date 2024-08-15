rm -rf mr-0*
rm -rf mr-1*
rm -rf mr-2*
rm -rf mr-3*
rm -rf mr-4*
rm -rf mr-5*
rm -rf mr-6*
rm -rf mr-7*
rm -rf mr-out-*
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrcoordinator.go pg-*.txt
