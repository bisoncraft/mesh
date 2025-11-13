#!/bin/bash

ROOT_DIR=~/.tatanka-test
TATANKA_DIR=../cmd/tatanka
TATANKA_BIN=$TATANKA_DIR/tatanka
MAKEPRIVKEY_DIR=./makeprivkey
MAKEPRIVKEY_BIN=$ROOT_DIR/makeprivkey
MANIFEST_FILE=$ROOT_DIR/manifest.json

build_tatanka() {
  cd $TATANKA_DIR
  go build -o tatanka
  cd - > /dev/null
}

build_makeprivkey() {
  go build -o $MAKEPRIVKEY_BIN $MAKEPRIVKEY_DIR/main.go
}

generate_privkey() {
  local node_dir=$1
  local privkey_path=$node_dir/p.key
  local peer_id=$($MAKEPRIVKEY_BIN $privkey_path)
  echo $peer_id
}

create_config() {
  local node_dir=$1
  local manifest_path=$2
  local listen_port=$3
  local config_path=$node_dir/tatanka.conf

  cat <<EOF > $config_path
appdata=$node_dir
manifestpath=$manifest_path
listenport=$listen_port
EOF

  echo $config_path
}

start_harness() {
  local num_nodes=$1

  mkdir -p $ROOT_DIR

  build_tatanka
  build_makeprivkey

  # Store bootstrap peers for the manifest file
  manifest_bootstrap=()

  # Generate private keys and create config files for each node
  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    mkdir -p $node_dir

    peer_id=$(generate_privkey $node_dir)

    listen_port=$((12345 + i))
    config_path=$(create_config $node_dir $MANIFEST_FILE $listen_port)

    addr="/ip4/127.0.0.1/tcp/$listen_port"
    manifest_bootstrap+=("{\"id\": \"$peer_id\", \"addresses\": [\"$addr\"]}")
  done

  # Create manifest file
  bootstrap_json=$(printf ",%s" "${manifest_bootstrap[@]}")
  bootstrap_json="${bootstrap_json:1}"  # Remove leading comma
  manifest="{\"bootstrap_peers\": [$bootstrap_json], \"non_bootstrap_peers\": []}"
  echo $manifest > $MANIFEST_FILE

  # Start tmux session and start nodes
  session_name="tatanka-test"
  tmux new-session -d -s $session_name
  for i in $(seq 1 $num_nodes); do
    node_dir=$ROOT_DIR/tatanka-$i
    config_path=$node_dir/tatanka.conf
    tmux new-window -t $session_name:$i -n node-$i
    tmux send-keys -t $session_name:$i "$TATANKA_BIN -C $config_path" C-m
  done

  echo "Started $num_nodes nodes in tmux session '$session_name'"
}

# Check if number of nodes is provided
if [ $# -eq 0 ]; then
  echo "Usage: $0 <num_nodes>"
  echo "  num_nodes: Number of tatanka nodes to start"
  exit 1
fi

num_nodes=$1

# Validate that the argument is a positive integer
if ! [[ "$num_nodes" =~ ^[0-9]+$ ]] || [ "$num_nodes" -eq 0 ]; then
  echo "Error: num_nodes must be a positive integer"
  exit 1
fi

start_harness $num_nodes

tmux attach-session -t $session_name