package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jessevdk/go-flags"
	"github.com/bisoncraft/mesh/tatanka/admin"
)

// Command definitions
type command struct {
	description string
	usage       string
	help        string
	run         func(args []string) error
}

var commands = map[string]*command{}

const globalOptions = `Global options:
  -a, --address=  Admin server address (default: localhost:12366)`

func init() {
	commands["conns"] = &command{
		description: "Display current node connections",
		usage:       "tatankactl conns",
		help: `Options:
  (none)`,
		run: runConns,
	}
	commands["watchconns"] = &command{
		description: "Watch node connections in real-time",
		usage:       "tatankactl watchconns",
		help: `Options:
  (none)

Press Ctrl+C to stop watching.`,
		run: runWatchConns,
	}
	commands["diff"] = &command{
		description: "Show whitelist diff for a node with whitelist mismatch",
		usage:       "tatankactl diff <peer_id>",
		help: `Arguments:
  peer_id         Peer ID (or prefix) to show diff for

Options:
  (none)

The peer must be in whitelist_mismatch state to show the diff.`,
		run: runDiff,
	}
	commands["help"] = &command{
		description: "Show help for commands",
		usage:       "tatankactl help [command]",
		help: `Arguments:
  command         Command to show help for (optional)`,
		run: runHelp,
	}
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmdName := os.Args[1]

	// Handle --help or -h at top level
	if cmdName == "--help" || cmdName == "-h" {
		printUsage()
		os.Exit(0)
	}

	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmdName)
		printUsage()
		os.Exit(1)
	}

	// Pass remaining args to the command
	if err := cmd.run(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("tatankactl - Tatanka node administration tool")
	fmt.Println()
	fmt.Println("Usage: tatankactl <command> [options]")
	fmt.Println()
	fmt.Println(globalOptions)
	fmt.Println()
	fmt.Println("Commands:")
	for name := range commands {
		fmt.Printf("  %-12s %s\n", name, commands[name].description)
	}
	fmt.Println()
	fmt.Println("Use \"tatankactl help <command>\" for more information about a command.")
}

func printCommandUsage(cmd *command) {
	fmt.Println(cmd.usage)
	fmt.Println()
	fmt.Println(cmd.description)
	fmt.Println()
	fmt.Println(cmd.help)
	fmt.Println()
	fmt.Println(globalOptions)
}

// Common options for connection commands
type connOptions struct {
	Address string `short:"a" long:"address" description:"Admin server address" default:"localhost:12366"`
}

func parseConnOptions(args []string) (*connOptions, []string, error) {
	var opts connOptions
	parser := flags.NewParser(&opts, flags.Default&^flags.PrintErrors)
	remaining, err := parser.ParseArgs(args)
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			return nil, nil, err
		}
		return nil, nil, err
	}
	return &opts, remaining, nil
}

func normalizeAddress(addr string) string {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return "http://" + addr
	}
	return addr
}

// conns command
func runConns(args []string) error {
	opts, remaining, err := parseConnOptions(args)
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			printCommandUsage(commands["conns"])
			return nil
		}
		return err
	}

	if len(remaining) > 0 {
		return fmt.Errorf("conns does not accept additional arguments: %v", remaining)
	}

	address := normalizeAddress(opts.Address)
	state, err := fetchState(address)
	if err != nil {
		return err
	}

	printState(state)
	return nil
}

// watchconns command
func runWatchConns(args []string) error {
	opts, remaining, err := parseConnOptions(args)
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			printCommandUsage(commands["watchconns"])
			return nil
		}
		return err
	}

	if len(remaining) > 0 {
		return fmt.Errorf("watchconns does not accept additional arguments: %v", remaining)
	}

	address := normalizeAddress(opts.Address)
	watchState(address)
	return nil
}

// diff command
func runDiff(args []string) error {
	opts, remaining, err := parseConnOptions(args)
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			printCommandUsage(commands["diff"])
			return nil
		}
		return err
	}

	if len(remaining) == 0 {
		return fmt.Errorf("diff requires a peer ID argument")
	}
	if len(remaining) > 1 {
		return fmt.Errorf("diff accepts only one peer ID argument, got: %v", remaining)
	}

	peerID := remaining[0]
	address := normalizeAddress(opts.Address)

	state, err := fetchState(address)
	if err != nil {
		return err
	}

	return showDiff(state, peerID)
}

// help command
func runHelp(args []string) error {
	if len(args) == 0 {
		printUsage()
		return nil
	}

	if len(args) > 1 {
		return fmt.Errorf("help accepts at most one argument")
	}

	cmdName := args[0]
	cmd, ok := commands[cmdName]
	if !ok {
		return fmt.Errorf("unknown command: %s", cmdName)
	}

	printCommandUsage(cmd)

	return nil
}

func fetchState(address string) (*admin.AdminState, error) {
	resp, err := http.Get(address + "/admin/state")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	var state admin.AdminState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &state, nil
}

func watchState(address string) {
	// Convert HTTP URL to WebSocket URL
	wsURL, err := url.Parse(address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid address: %v\n", err)
		os.Exit(1)
	}

	if wsURL.Scheme == "https" {
		wsURL.Scheme = "wss"
	} else {
		wsURL.Scheme = "ws"
	}
	wsURL.Path = "/admin/ws"

	// Handle interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("Connecting to %s...\n", wsURL.String())

	conn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected. Watching for updates (Ctrl+C to exit)...")

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fmt.Fprintf(os.Stderr, "Connection error: %v\n", err)
				}
				return
			}

			var state admin.AdminState
			if err := json.Unmarshal(message, &state); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to decode message: %v\n", err)
				continue
			}

			// Clear screen and print new state
			fmt.Print("\033[H\033[2J")
			fmt.Printf("Tatanka Admin - %s\n", time.Now().Format("15:04:05"))
			fmt.Println(strings.Repeat("=", 60))
			printState(&state)
		}
	}()

	select {
	case <-done:
	case <-interrupt:
		fmt.Println("\nDisconnecting...")
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
}

func printState(state *admin.AdminState) {
	nodes := make([]admin.NodeInfo, 0, len(state.Nodes))
	for _, node := range state.Nodes {
		nodes = append(nodes, node)
	}

	// Sort nodes by state: connected first, then whitelist_mismatch, then disconnected
	sort.Slice(nodes, func(i, j int) bool {
		stateOrder := map[admin.NodeConnectionState]int{
			admin.StateConnected:         0,
			admin.StateWhitelistMismatch: 1,
			admin.StateDisconnected:      2,
		}
		return stateOrder[nodes[i].State] < stateOrder[nodes[j].State]
	})

	// Count by state
	counts := make(map[admin.NodeConnectionState]int)
	for _, node := range nodes {
		counts[node.State]++
	}

	fmt.Printf("Node Connections (%d total)\n", len(nodes))
	fmt.Printf("  Connected: %d | Whitelist Mismatch: %d | Disconnected: %d\n\n",
		counts[admin.StateConnected], counts[admin.StateWhitelistMismatch], counts[admin.StateDisconnected])

	if len(nodes) == 0 {
		fmt.Println("  No nodes in whitelist")
		return
	}

	for _, node := range nodes {
		icon := getStateIcon(node.State)
		stateStr := getStateString(node.State)
		fmt.Printf("  %s %-20s %s\n", icon, stateStr, node.PeerID)

		// Print addresses
		if len(node.Addresses) > 0 {
			for _, addr := range node.Addresses {
				fmt.Printf("     │  %s\n", addr)
			}
		}

		if node.State == admin.StateWhitelistMismatch && len(node.PeerWhitelist) > 0 {
			fmt.Printf("     └─ Use \"tatankactl diff %s\" to see whitelist differences\n", node.PeerID[:12])
		}
	}
}

func showDiff(state *admin.AdminState, peerID string) error {
	var targetNode *admin.NodeInfo
	for id, node := range state.Nodes {
		if strings.HasPrefix(id, peerID) {
			nodeCopy := node
			targetNode = &nodeCopy
			break
		}
	}

	if targetNode == nil {
		return fmt.Errorf("node not found: %s", peerID)
	}

	if targetNode.State != admin.StateWhitelistMismatch {
		return fmt.Errorf("node %s is not in whitelist mismatch state", peerID)
	}

	if len(targetNode.PeerWhitelist) == 0 {
		return fmt.Errorf("no peer whitelist data available for %s", peerID)
	}

	ourSet := make(map[string]bool)
	for _, id := range state.OurWhitelist {
		ourSet[id] = true
	}

	peerSet := make(map[string]bool)
	for _, id := range targetNode.PeerWhitelist {
		peerSet[id] = true
	}

	var onlyInOurs, onlyInPeers, inBoth []string

	for _, id := range state.OurWhitelist {
		if peerSet[id] {
			inBoth = append(inBoth, id)
		} else {
			onlyInOurs = append(onlyInOurs, id)
		}
	}

	for _, id := range targetNode.PeerWhitelist {
		if !ourSet[id] {
			onlyInPeers = append(onlyInPeers, id)
		}
	}

	fmt.Printf("Whitelist Diff for %s\n", targetNode.PeerID)
	fmt.Println(strings.Repeat("=", 60))

	if len(inBoth) > 0 {
		fmt.Printf("\n✓ In Both Whitelists (%d):\n", len(inBoth))
		for _, id := range inBoth {
			fmt.Printf("  %s\n", id)
		}
	}

	if len(onlyInOurs) > 0 {
		fmt.Printf("\n+ Only in Our Whitelist (%d):\n", len(onlyInOurs))
		for _, id := range onlyInOurs {
			fmt.Printf("  %s\n", id)
		}
	}

	if len(onlyInPeers) > 0 {
		fmt.Printf("\n- Only in Peer's Whitelist (%d):\n", len(onlyInPeers))
		for _, id := range onlyInPeers {
			fmt.Printf("  %s\n", id)
		}
	}

	fmt.Println()
	return nil
}

func getStateIcon(state admin.NodeConnectionState) string {
	switch state {
	case admin.StateConnected:
		return "🟢"
	case admin.StateWhitelistMismatch:
		return "🟡"
	case admin.StateDisconnected:
		return "🔴"
	default:
		return "⚪"
	}
}

func getStateString(state admin.NodeConnectionState) string {
	switch state {
	case admin.StateConnected:
		return "Connected"
	case admin.StateWhitelistMismatch:
		return "Whitelist Mismatch"
	case admin.StateDisconnected:
		return "Disconnected"
	default:
		return string(state)
	}
}
