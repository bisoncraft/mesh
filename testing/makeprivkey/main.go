// makeprivkey generates a new Ed25519 private key for a libp2p node,
// writes it to the specified file path, and outputs the corresponding
// peer ID to stdout. This is used to create node identities for the
// tatanka test harness.
package main

import (
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <private-key-file-path>\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]

	// Generate a new Ed25519 private key
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating key pair: %v\n", err)
		os.Exit(1)
	}

	// Marshal the private key to bytes
	bytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling private key: %v\n", err)
		os.Exit(1)
	}

	// Write the private key to file
	if err := os.WriteFile(filePath, bytes, 0600); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing private key to file: %v\n", err)
		os.Exit(1)
	}

	// Get the peer ID from the public key
	peerID, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error deriving peer ID: %v\n", err)
		os.Exit(1)
	}

	// Print the peer ID to stdout
	fmt.Println(peerID.String())
}
