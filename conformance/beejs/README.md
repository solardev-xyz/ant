# bee-js drop-in pack

Runs **unmodified bee-js** (the canonical Swarm JS client) against two
backends and compares:

- **ant** — the production gateway served over the in-memory MemNode
  (`cargo run -p ant-conformance --bin memgateway`),
- **bee** — bee's real API layer over in-memory mocks
  (`conformance/beemock`).

Every operation must succeed on both, and content-addressed references
(bytes, files, collections, SOCs, feed updates, feed manifests) must be
**byte-identical** across backends. This is the executable form of
PLAN.md's drop-in-replacement claim, at the client-library level rather
than raw HTTP.

```sh
# once: build the two servers
(cd ../beemock && go build -o beemock .)
cargo build -p ant-conformance --bin memgateway

# then
npm install
npm test
```

The runner spawns both servers itself on ephemeral ports; override with
`BEEMOCK_BIN` / `MEMGATEWAY_BIN` / `BATCH_ID`.

Scenarios: uploadData/downloadData, uploadFile round-trip with metadata,
uploadCollection with index document + nested path, tags lifecycle,
client-signed SOC write/read, two feed updates with latest resolution,
feed-manifest creation + `/bzz` dereference, `fetchLatestFeedUpdate`.
