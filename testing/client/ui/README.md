## Test client UI

### One-time setup

```bash
cd testing/client/ui
npm install
```

### Build (required before running the Go UI server)

```bash
npm run build
```

This writes `testing/client/ui/dist/` (not committed).

### Watch mode (auto rebuild on changes)

```bash
npm run watch
```

### How it is served
The Go test client serves the built output from `testing/client/ui/dist/` at:

- `http://localhost:<webport>/`

If you start the test client without a `dist/` folder, it will show an instruction page telling you to run `npm install` and `npm run build`.


