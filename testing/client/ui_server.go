package client

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

func uiDistDir() string {
	// Try environment variable.
	if env := os.Getenv("TATANKA_TESTCLIENT_UI_DIST"); env != "" {
		if fileExists(filepath.Join(env, "index.html")) {
			return env
		}
	}

	// Try relative to the executable location.
	if exe, err := os.Executable(); err == nil && exe != "" {
		if dir := findUIDistDir(filepath.Dir(exe)); dir != "" {
			return dir
		}
	}

	// Try relative to current working directory.
	if wd, err := os.Getwd(); err == nil && wd != "" {
		if dir := findUIDistDir(wd); dir != "" {
			return dir
		}
	}

	// Fallback: relative path from cwd.
	return filepath.Join("testing", "client", "ui", "dist")
}

// findUIDistDir walks up a few levels looking for index.html.
func findUIDistDir(startDir string) string {
	const maxUp = 8
	dir := startDir
	for range maxUp {
		candidate := filepath.Join(dir, "testing", "client", "ui", "dist")
		if fileExists(filepath.Join(candidate, "index.html")) {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func (c *Client) serveUIDistAsset(w http.ResponseWriter, r *http.Request) {
	dist := uiDistDir()
	fs := http.FileServer(http.Dir(dist))
	http.StripPrefix("/", fs).ServeHTTP(w, r)
}

// serveUIIndexFromDist serves the UI if index.html is found,
// otherwise it serves an error page.
func (c *Client) serveUIIndexFromDist(w http.ResponseWriter, r *http.Request) {
	dist := uiDistDir()
	indexPath := filepath.Join(dist, "index.html")

	b, err := os.ReadFile(indexPath)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `<!doctype html>
<html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Tatanka Test Client UI</title>
<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;margin:24px;line-height:1.4}code{background:#eee;padding:2px 6px;border-radius:6px}</style>
</head><body>
<h2>UI not built</h2>
<p>The test client serves the UI from <code>%s</code>, but it does not exist yet.</p>
<p>Run:</p>
<pre><code>cd testing/client/ui
npm install
npm run build</code></pre>
<p>Or for continuous rebuilds:</p>
<pre><code>npm run watch</code></pre>
<p>If you built the UI elsewhere, set <code>TATANKA_TESTCLIENT_UI_DIST</code> to the dist directory.</p>
</body></html>`, indexPath)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}
