package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/decred/slog"
	"github.com/jessevdk/go-flags"
	"github.com/jrick/logrotate/rotator"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/martonp/tatanka-mesh/bond"
	"github.com/martonp/tatanka-mesh/testing/client"
)

// bondParamsFlag wraps bond.BondParams to implement the flag.Value interface.
type bondParamsFlag bond.BondParams

// Set sets the provided value as bond params.
func (p *bondParamsFlag) Set(value string) error {
	if err := json.Unmarshal([]byte(value), p); err != nil {
		return fmt.Errorf("failed to unmarshal bond params: %w", err)
	}

	return nil
}

// String outputs stringified bond params.
func (p *bondParamsFlag) String() string {
	data, err := json.Marshal(p)
	if err != nil {
		return ""
	}

	return string(data)
}

// Config represents the configuration options for the test client.
type Config struct {
	AppDataDir string            `short:"A" long:"appdata" description:"Path to application home directory."`
	ConfigFile string            `short:"C" long:"configfile" description:"Path to configuration file."`
	LogLevel   string            `short:"l" long:"loglevel" description:"Logging level {trace, debug, info, warn, error, critical}."`
	NodeAddr   string            `long:"nodeaddr" description:"The address of the mesh node to connect to."`
	BondParams []*bondParamsFlag `long:"bondparams" description:"The test client bond params."`
	ClientPort int               `long:"clientport" description:"The port to listen on for client connections"`
	WebPort    int               `long:"webport" description:"The web interface port."`
}

// defaultAppDataDir returns the default application data directory.
func defaultAppDataDir() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".testclient")
}

// defaultConfigFile returns the default configuration file path.
func defaultConfigFile() string {
	return filepath.Join(defaultAppDataDir(), "testclient.conf")
}

// loadConfigFile loads values from the config file into cfg.
func loadConfigFile(parser *flags.Parser, cfg *Config) error {
	filePath := cfg.ConfigFile
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	return flags.NewIniParser(parser).ParseFile(filePath)
}

// initLogRotator initializes the logging rotator to write logs to logFile and
// create roll files in the same directory.  It must be called before the
// package-global log rotator variables are used.
func initLogRotator(dir string) (*rotator.Rotator, error) {
	logDir := filepath.Join(dir, "logs")
	logFile := filepath.Join(logDir, "testclient.log")
	err := os.MkdirAll(logDir, 0700)
	if err != nil {
		return nil, err
	}
	const maxLogRolls = 5
	return rotator.New(logFile, 32*1024, false, maxLogRolls)
}

// generateIdentity generates or loads the client's identity private key.
func generateIdentity() (crypto.PrivKey, error) {
	pkPath := filepath.Join(defaultAppDataDir(), "priv.pem")

	var priv crypto.PrivKey
	data, err := os.ReadFile(pkPath)
	if err != nil {
		if os.IsNotExist(err) {
			// If no private key file exists for the client's identity, generate one for the session.
			priv, _, err = crypto.GenerateKeyPair(crypto.Ed25519, -1)
			if err != nil {
				return nil, fmt.Errorf("failed to generate key pair: %w", err)
			}

			return priv, nil
		}
	}

	priv, err = crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	return priv, nil
}

func main() {
	cfg := Config{
		AppDataDir: defaultAppDataDir(),
		ConfigFile: defaultConfigFile(),
		LogLevel:   "debug",
		NodeAddr:   "",
		ClientPort: client.DefaultClientPort,
		WebPort:    client.DefaultWebPort,
	}

	// Parse command-line flags (overrides file values)
	parser := flags.NewParser(&cfg, flags.Default)
	if _, err := parser.Parse(); err != nil {
		if e, ok := err.(*flags.Error); ok && e.Type == flags.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Failed to parse flags: %v\n", err)
		os.Exit(1)
	}

	// Load config file if exists (base values, overridden by flags)
	if err := loadConfigFile(parser, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config file: %v\n", err)
		os.Exit(1)
	}

	logRotator, err := initLogRotator(cfg.AppDataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize log rotator: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logRotator.Close() }()

	// Setup logger to write to both stdout and log file
	logStream := make(chan string, 100)
	logWriter := io.MultiWriter(os.Stdout, client.NewLogStreamBackend(logStream), logRotator)
	backend := slog.NewBackend(logWriter)
	log := backend.Logger("testclient")
	level, ok := slog.LevelFromString(cfg.LogLevel)
	if !ok {
		fmt.Fprintf(os.Stderr, "Invalid debug level: %s\n", cfg.LogLevel)
		os.Exit(1)
	}
	log.SetLevel(level)

	log.Infof("Using app data directory: %s", cfg.AppDataDir)

	priv, err := generateIdentity()
	if err != nil {
		log.Errorf("Failed to generate identity: %v", err)
		os.Exit(1)
	}

	tcCfg := &client.Config{
		NodeAddr:   cfg.NodeAddr,
		PrivateKey: priv,
		ClientPort: cfg.ClientPort,
		WebPort:    cfg.WebPort,
		Logger:     log,
		LogStream:  logStream,
	}

	tc, err := client.NewClient(tcCfg)
	if err != nil {
		log.Errorf("Failed to create test client: %v", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Infof("Received shutdown signal")
		cancel()
	}()

	bonds := make([]*bond.BondParams, len(cfg.BondParams))
	for i, bp := range cfg.BondParams {
		bonds[i] = (*bond.BondParams)(bp)
	}

	tc.Run(ctx, bonds)
}
