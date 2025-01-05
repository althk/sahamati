# Allows starting/stopping a raft cluster with dynamic size of nodes for dev env.
# `make start CLUSTER_SIZE=5` will start 5 nodes on localhost from ports 6001 to 6005
# `make stop` will send SIGINT to all nodes and allows them to shutdown gracefully.

CLUSTER_SIZE=3
BASE_PORT=6001
LAST_PORT=$(shell expr $(BASE_PORT) + $(CLUSTER_SIZE) - 1)

# Generate sequential port numbers (6001 6002...)
PORTS=$(shell seq $(BASE_PORT) $(LAST_PORT))
# Generate host list (localhost:6001 localhost:6002...)
HOSTS=$(addprefix localhost:,${PORTS})
# Generate a CSV for the cluster combining all hosts (localhost:6001,localhost:6002,...)
CLUSTER="$(shell echo $(HOSTS) | tr ' ' ',')"

.PHONY: proto run_example stop_example

proto:
	@buf lint
	@buf generate

run_example:
	mkdir /tmp/sahamati
	@echo "starting $(CLUSTER_SIZE) nodes"
	@echo "cluster = $(CLUSTER)"
	@for i in $(HOSTS); do \
  		echo "starting node $$i"; \
  		KVS_PORT=`echo $$i | sed 's/localhost:[0-9]/localhost:8/'`; \
  		go run ./example/kvstore --kvstore_addr $$KVS_PORT --raft_addr $$i --nodes $(CLUSTER) & \
  	done

# `go run` spawns a child process for each invocation.
# We get the PIDs of the `go run` cmds and kill all child processes to stop the run.
stop_example:
	@echo "stopping all nodes"
	@pkill -SIGINT -P $(shell pgrep -f "go run ./example/kvstore" | tr \\n , | sed 's/,$$//')