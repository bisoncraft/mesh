#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ROOT_DIR=~/.tatanka-test
TATANKA_DIR="$SCRIPT_DIR/../cmd/tatanka"
TATANKA_BIN=$TATANKA_DIR/tatanka
TESTCLIENT_DIR="$SCRIPT_DIR/../cmd/testclient"
TESTCLIENT_BIN=$TESTCLIENT_DIR/testclient
TESTCLIENT_UI_DIR="$SCRIPT_DIR/client/ui"
MAKEPRIVKEY_DIR="$SCRIPT_DIR/makeprivkey"
MAKEPRIVKEY_BIN=$ROOT_DIR/makeprivkey
WHITELIST_FILE=$ROOT_DIR/whitelist.json

build_tatanka() {
  cd "$TATANKA_DIR"
  go build -o tatanka
  cd - > /dev/null
}

build_makeprivkey() {
  go build -o "$MAKEPRIVKEY_BIN" "$MAKEPRIVKEY_DIR/main.go"
}

build_testclient() {
  cd "$TESTCLIENT_DIR"
  go build -o testclient
  cd - > /dev/null
}

build_testclient_ui() {
  cd "$TESTCLIENT_UI_DIR"
  if [ ! -d node_modules ]; then
    npm install
  fi
  npm run build
  cd - > /dev/null
}

generate_privkey() {
  local node_dir=$1
  local privkey_path=$node_dir/p.key
  local peer_id=$("$MAKEPRIVKEY_BIN" "$privkey_path")
  echo $peer_id
}

create_config() {
  local node_dir=$1
  local whitelist_path=$2
  local listen_port=$3
  local metrics_port=$4
  local admin_port=$5
  local config_path=$node_dir/tatanka.conf

  cat <<EOF > $config_path
appdata=$node_dir
whitelistpath=$whitelist_path
listenport=$listen_port
metricsport=$metrics_port
adminport=$admin_port
EOF

  echo $config_path
}

create_client_config() {
  local client_dir=$1
  local node_addr=$2
  local client_port=$3
  local web_port=$4
  local config_path=$client_dir/testclient.conf

  cat <<EOF > $config_path
appdata=$client_dir
loglevel=debug
nodeaddr=$node_addr
clientport=$client_port
webport=$web_port
EOF

  echo $config_path
}

start_harness() {
  local num_nodes=$1
  local num_clients=$2

  mkdir -p $ROOT_DIR

  build_tatanka
  build_makeprivkey
  build_testclient
  build_testclient_ui

  # Store bootstrap peers for the whitelist file
  whitelist_peers=()
  node_peer_ids=()
  node_listen_ports=()

  # Generate private keys and create config files for each node
  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    mkdir -p $node_dir

    peer_id=$(generate_privkey $node_dir)
    node_peer_ids+=("$peer_id")

    listen_port=$((12345 + i))
    metrics_port=$((12355 + i))
    admin_port=$((12365 + i))
    node_listen_ports+=("$listen_port")
    config_path=$(create_config $node_dir $WHITELIST_FILE $listen_port $metrics_port $admin_port)

    addr="/ip4/127.0.0.1/tcp/$listen_port"
    whitelist_peers+=("{\"id\": \"$peer_id\", \"address\": \"$addr\"}")
  done

  # Create whitelist file
  whitelist_json=$(printf ",%s" "${whitelist_peers[@]}")
  whitelist_json="${whitelist_json:1}"  # Remove leading comma
  whitelist="{\"peers\": [$whitelist_json]}"
  echo $whitelist > $WHITELIST_FILE

  # Start tmux session and start nodes
  session_name="tatanka-test"
  tmux new-session -d -s $session_name
  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    config_path=$node_dir/tatanka.conf
    tmux new-window -t $session_name:$i -n node-$i
    tmux send-keys -t $session_name:$i "$TATANKA_BIN -C $config_path" C-m
  done

  sleep 2 # wait for nodes to start before starting clients

  # Start test clients and evenly distribute them across the nodes.
  if [ "$num_clients" -gt 0 ]; then
    for i in $(seq 1 $num_clients); do
      client_dir=$ROOT_DIR/client-$i
      mkdir -p $client_dir

      node_idx=$(( (i - 1) % num_nodes ))
      node_peer_id=${node_peer_ids[$node_idx]}
      node_listen_port=${node_listen_ports[$node_idx]}
      node_addr="/ip4/127.0.0.1/tcp/$node_listen_port/p2p/$node_peer_id"

      client_port=$((12455 + i))
      web_port=$((12465 + i))
      client_cfg=$(create_client_config $client_dir "$node_addr" $client_port $web_port)

      tmux new-window -t $session_name -n client-$i
      tmux send-keys -t $session_name "ROOT_DIR=$ROOT_DIR $TESTCLIENT_BIN -C $client_cfg" C-m
    done
  fi

  echo "Started $num_nodes nodes and $num_clients clients in tmux session '$session_name'"
}

# Check if number of nodes and clients are provided
if [ $# -lt 2 ]; then
  echo "Usage: $0 <num_nodes> <num_clients>"
  echo "  num_nodes:   Number of tatanka nodes to start"
  echo "  num_clients: Number of test clients to start"
  exit 1
fi

num_nodes=$1
num_clients=$2

# Validate that the argument is a positive integer
if ! [[ "$num_nodes" =~ ^[0-9]+$ ]] || [ "$num_nodes" -eq 0 ]; then
  echo "Error: num_nodes must be a positive integer"
  exit 1
fi

if ! [[ "$num_clients" =~ ^[0-9]+$ ]] || [ "$num_clients" -lt 0 ]; then
  echo "Error: num_clients must be a non-negative integer"
  exit 1
fi

start_harness $num_nodes $num_clients

tmux attach-session -t $session_name