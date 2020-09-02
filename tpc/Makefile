prepare:
	@go build -o ./tpc  .
	@cd examples/client && go build


run-example-coordinator:
	@./tpc  -role=coordinator -nodeaddr=localhost:3000 -follower=localhost:3001 -committype=three-phase -timeout=1000

run-example-follower:
	@./tpc -role=follower -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000

run-example-client:
	@examples/client/client

unit-tests:
	@cd server && go test

functional-tests:
	@go test