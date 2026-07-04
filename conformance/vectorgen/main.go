// Command vectorgen emits cross-implementation test vectors for ant by
// calling the canonical bee implementation (the sibling ../../../bee
// checkout, wired in via a go.mod replace directive).
//
// Everything that bee exports is produced by bee's own code: CAC
// construction (pkg/cac), SOC signing/addressing (pkg/soc), feed ids
// (pkg/feeds), postage stamp digests + signatures (pkg/postage), and the
// erasure-code parity tables (pkg/file/redundancy). The only mirrored
// byte layouts are the private feed index marshallers:
//
//   - sequence index -> 8-byte big-endian uint64
//     (pkg/feeds/sequence/sequence.go:47)
//   - epoch index    -> keccak256(BE8(start) || level)
//     (pkg/feeds/epochs/epoch.go:35)
//
// Regenerate with:
//
//	cd conformance/vectorgen && go run . -out ../vectors
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/postage"
	"github.com/ethersphere/bee/v2/pkg/replicas"
	"github.com/ethersphere/bee/v2/pkg/soc"
	"github.com/ethersphere/bee/v2/pkg/swarm"
)

func main() {
	out := flag.String("out", "../vectors", "output directory for vector JSON files")
	flag.Parse()
	if err := run(*out); err != nil {
		fmt.Fprintf(os.Stderr, "vectorgen: %v\n", err)
		os.Exit(1)
	}
}

func run(outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	files := map[string]any{
		"cac.json":           cacVectors(),
		"soc.json":           socVectors(),
		"feed_sequence.json": feedSequenceVectors(),
		"feed_epoch.json":    feedEpochVectors(),
		"feed_updates.json":  feedUpdateVectors(),
		"stamps.json":        stampVectors(),
		"replicas.json":      replicaVectors(),
		"redundancy.json":    redundancyVectors(),
		"rs_files.json":      redundantFileVectors(),
	}
	for name, v := range files {
		blob, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		blob = append(blob, '\n')
		if err := os.WriteFile(filepath.Join(outDir, name), blob, 0o644); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Printf("wrote %s\n", filepath.Join(outDir, name))
	}
	return nil
}

// --- deterministic inputs -------------------------------------------------

// det derives n deterministic bytes from a seed label via a keccak stream.
func det(seed string, n int) []byte {
	cur := mustKeccak([]byte(seed))
	out := make([]byte, 0, n+32)
	for len(out) < n {
		out = append(out, cur...)
		cur = mustKeccak(cur)
	}
	return out[:n]
}

func mustKeccak(parts ...[]byte) []byte {
	var all []byte
	for _, p := range parts {
		all = append(all, p...)
	}
	h, err := crypto.LegacyKeccak256(all)
	if err != nil {
		panic(err)
	}
	return h
}

func hexs(b []byte) string { return hex.EncodeToString(b) }

func be8(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

type keyed struct {
	secret []byte
	signer crypto.Signer
	owner  []byte
}

func testKey(label string) keyed {
	secret := det(label, 32)
	pk, err := crypto.DecodeSecp256k1PrivateKey(secret)
	if err != nil {
		panic(err)
	}
	signer := crypto.NewDefaultSigner(pk)
	owner, err := signer.EthereumAddress()
	if err != nil {
		panic(err)
	}
	return keyed{secret: secret, signer: signer, owner: owner.Bytes()}
}

// --- feed index marshallers (mirrors of bee's private types) --------------

type seqIndex uint64

func (i seqIndex) MarshalBinary() ([]byte, error) { return be8(uint64(i)), nil }
func (i seqIndex) Next(int64, uint64) feeds.Index { return i + 1 }
func (i seqIndex) String() string                 { return fmt.Sprintf("%d", uint64(i)) }

type epochIndex struct {
	start uint64
	level uint8
}

func (e epochIndex) MarshalBinary() ([]byte, error) {
	return crypto.LegacyKeccak256(append(be8(e.start), e.level))
}
func (e epochIndex) Next(int64, uint64) feeds.Index { return e }
func (e epochIndex) String() string                 { return fmt.Sprintf("%d/%d", e.start, e.level) }

// --- cac -------------------------------------------------------------------

type cacCase struct {
	Name         string `json:"name"`
	PayloadHex   string `json:"payloadHex"`
	SpanHex      string `json:"spanHex"`
	AddressHex   string `json:"addressHex"`
	ChunkDataHex string `json:"chunkDataHex"`
}

func cacVectors() any {
	sizes := []int{0, 1, 31, 32, 33, 64, 100, 1000, 4095, 4096}
	cases := make([]cacCase, 0, len(sizes))
	for _, n := range sizes {
		payload := det(fmt.Sprintf("cac/payload/%d", n), n)
		ch, err := cac.New(payload)
		if err != nil {
			panic(fmt.Sprintf("cac.New(len=%d): %v", n, err))
		}
		cases = append(cases, cacCase{
			Name:         fmt.Sprintf("len-%d", n),
			PayloadHex:   hexs(payload),
			SpanHex:      hexs(ch.Data()[:swarm.SpanSize]),
			AddressHex:   ch.Address().String(),
			ChunkDataHex: hexs(ch.Data()),
		})
	}
	return map[string]any{
		"description": "content-addressed chunks built by bee pkg/cac: address = BMT(span, payload), span = little-endian payload length",
		"cases":       cases,
	}
}

// --- soc -------------------------------------------------------------------

type socCase struct {
	Name          string `json:"name"`
	SecretHex     string `json:"secretHex"`
	OwnerHex      string `json:"ownerHex"`
	IDHex         string `json:"idHex"`
	PayloadHex    string `json:"payloadHex"`
	CacAddressHex string `json:"cacAddressHex"`
	DigestHex     string `json:"digestHex"`
	SignatureHex  string `json:"signatureHex"`
	SocAddressHex string `json:"socAddressHex"`
	ChunkDataHex  string `json:"chunkDataHex"`
}

func makeSoc(name string, k keyed, id, payload []byte) socCase {
	ch, err := cac.New(payload)
	if err != nil {
		panic(err)
	}
	signed, err := soc.New(id, ch).Sign(k.signer)
	if err != nil {
		panic(err)
	}
	parsed, err := soc.FromChunk(signed)
	if err != nil {
		panic(err)
	}
	// digest actually signed (before the Ethereum message prefix bee's
	// crypto.Signer.Sign adds): keccak256(id || wrapped cac address) —
	// bee pkg/soc/soc.go:124.
	digest := mustKeccak(id, ch.Address().Bytes())
	return socCase{
		Name:          name,
		SecretHex:     hexs(k.secret),
		OwnerHex:      hexs(k.owner),
		IDHex:         hexs(id),
		PayloadHex:    hexs(payload),
		CacAddressHex: ch.Address().String(),
		DigestHex:     hexs(digest),
		SignatureHex:  hexs(parsed.Signature()),
		SocAddressHex: signed.Address().String(),
		ChunkDataHex:  hexs(signed.Data()),
	}
}

func socVectors() any {
	k := testKey("ant-conformance/key/1")
	zeroID := make([]byte, 32)
	cases := []socCase{
		makeSoc("zero-id-small-payload", k, zeroID, det("soc/payload/1", 27)),
		makeSoc("random-id-small-payload", k, det("soc/id/1", 32), det("soc/payload/2", 64)),
		makeSoc("random-id-empty-payload", k, det("soc/id/2", 32), nil),
		makeSoc("random-id-full-payload", k, det("soc/id/3", 32), det("soc/payload/3", 4096)),
	}
	return map[string]any{
		"description": "single-owner chunks signed by bee pkg/soc: address = keccak256(id || owner), signature over keccak256(id || cacAddr) with Ethereum message prefix",
		"cases":       cases,
	}
}

// --- feed ids ---------------------------------------------------------------

type feedIDCase struct {
	TopicHex      string `json:"topicHex"`
	Index         string `json:"index"`
	IDHex         string `json:"idHex"`
	OwnerHex      string `json:"ownerHex"`
	SocAddressHex string `json:"socAddressHex"`
}

func feedSequenceVectors() any {
	k := testKey("ant-conformance/key/1")
	topics := [][]byte{det("feed/topic/1", 32), det("feed/topic/2", 32)}
	indices := []uint64{0, 1, 2, 7, 8, 255, 256, 65535, 1 << 20, ^uint64(0)}
	var cases []feedIDCase
	for _, topic := range topics {
		for _, idx := range indices {
			id, err := feeds.Id(topic, seqIndex(idx))
			if err != nil {
				panic(err)
			}
			addr, err := soc.CreateAddress(id, k.owner)
			if err != nil {
				panic(err)
			}
			cases = append(cases, feedIDCase{
				TopicHex:      hexs(topic),
				Index:         fmt.Sprintf("%d", idx),
				IDHex:         hexs(id),
				OwnerHex:      hexs(k.owner),
				SocAddressHex: addr.String(),
			})
		}
	}
	return map[string]any{
		"description": "sequence feed ids from bee pkg/feeds: id = keccak256(topic || BE8(index)), soc address = keccak256(id || owner)",
		"cases":       cases,
	}
}

type epochIDCase struct {
	Start         string `json:"start"`
	Level         uint8  `json:"level"`
	IndexBytesHex string `json:"indexBytesHex"`
	TopicHex      string `json:"topicHex"`
	IDHex         string `json:"idHex"`
	OwnerHex      string `json:"ownerHex"`
	SocAddressHex string `json:"socAddressHex"`
}

func feedEpochVectors() any {
	k := testKey("ant-conformance/key/1")
	topic := det("feed/topic/1", 32)
	pairs := []epochIndex{
		{0, 0}, {0, 25}, {0, 32},
		{1, 0}, {1145373120, 0}, {1145373120, 25},
		{^uint64(0) >> 1, 31},
	}
	var cases []epochIDCase
	for _, e := range pairs {
		indexBytes, err := e.MarshalBinary()
		if err != nil {
			panic(err)
		}
		id, err := feeds.Id(topic, e)
		if err != nil {
			panic(err)
		}
		addr, err := soc.CreateAddress(id, k.owner)
		if err != nil {
			panic(err)
		}
		cases = append(cases, epochIDCase{
			Start:         fmt.Sprintf("%d", e.start),
			Level:         e.level,
			IndexBytesHex: hexs(indexBytes),
			TopicHex:      hexs(topic),
			IDHex:         hexs(id),
			OwnerHex:      hexs(k.owner),
			SocAddressHex: addr.String(),
		})
	}
	return map[string]any{
		"description": "epoch feed ids: index bytes = keccak256(BE8(start) || level) (bee pkg/feeds/epochs/epoch.go:35), id = keccak256(topic || indexBytes)",
		"cases":       cases,
	}
}

// --- feed update chunks -----------------------------------------------------

type feedUpdateCase struct {
	Kind          string `json:"kind"`
	SecretHex     string `json:"secretHex"`
	OwnerHex      string `json:"ownerHex"`
	TopicHex      string `json:"topicHex"`
	Index         string `json:"index"`
	PayloadHex    string `json:"payloadHex"`
	IDHex         string `json:"idHex"`
	SignatureHex  string `json:"signatureHex"`
	SocAddressHex string `json:"socAddressHex"`
	ChunkDataHex  string `json:"chunkDataHex"`
}

func makeFeedUpdate(kind string, k keyed, topic []byte, idx uint64, payload []byte) feedUpdateCase {
	id, err := feeds.Id(topic, seqIndex(idx))
	if err != nil {
		panic(err)
	}
	ch, err := cac.New(payload)
	if err != nil {
		panic(err)
	}
	signed, err := soc.New(id, ch).Sign(k.signer)
	if err != nil {
		panic(err)
	}
	parsed, err := soc.FromChunk(signed)
	if err != nil {
		panic(err)
	}
	return feedUpdateCase{
		Kind:          kind,
		SecretHex:     hexs(k.secret),
		OwnerHex:      hexs(k.owner),
		TopicHex:      hexs(topic),
		Index:         fmt.Sprintf("%d", idx),
		PayloadHex:    hexs(payload),
		IDHex:         hexs(id),
		SignatureHex:  hexs(parsed.Signature()),
		SocAddressHex: signed.Address().String(),
		ChunkDataHex:  hexs(signed.Data()),
	}
}

func feedUpdateVectors() any {
	k := testKey("ant-conformance/key/1")
	topic := det("feed/topic/1", 32)

	// v2 ("wrapped chunk"): the soc-wrapped cac IS the content.
	v2 := makeFeedUpdate("v2", k, topic, 0, det("feed/payload/v2", 64))

	// v1 (legacy): wrapped cac data = BE8(timestamp) || 32-byte reference,
	// total wrapped chunk data = 8 (span) + 8 + 32 = 48 (bee getter.go:110).
	v1Payload := append(be8(1720000000), det("feed/ref/1", 32)...)
	v1 := makeFeedUpdate("v1", k, topic, 1, v1Payload)

	// v1 with a 64-byte (encrypted) reference: wrapped data length 80.
	v1EncPayload := append(be8(1720000000), det("feed/ref/2", 64)...)
	v1Enc := makeFeedUpdate("v1-enc", k, topic, 2, v1EncPayload)

	return map[string]any{
		"description": "full sequence-feed update chunks (soc-wrapped cac) signed by bee; v1 payload = BE8(ts)||ref, v2 payload = content",
		"cases":       []feedUpdateCase{v2, v1, v1Enc},
	}
}

// --- postage stamps -----------------------------------------------------------

type stampCase struct {
	Name         string `json:"name"`
	SecretHex    string `json:"secretHex"`
	OwnerHex     string `json:"ownerHex"`
	ChunkAddrHex string `json:"chunkAddrHex"`
	BatchIDHex   string `json:"batchIdHex"`
	IndexHex     string `json:"indexHex"`
	TimestampHex string `json:"timestampHex"`
	DigestHex    string `json:"digestHex"`
	SignatureHex string `json:"signatureHex"`
	StampHex     string `json:"stampHex"`
}

func stampVectors() any {
	k := testKey("ant-conformance/key/1")
	batchID := det("stamp/batch/1", 32)
	var cases []stampCase
	for i, payloadLen := range []int{11, 4096} {
		ch, err := cac.New(det(fmt.Sprintf("stamp/chunk/%d", i), payloadLen))
		if err != nil {
			panic(err)
		}
		addr := ch.Address().Bytes()
		// index = BE4(collision bucket) || BE4(index within bucket); the
		// bucket is the top bucketDepth (16) bits of the chunk address
		// (bee pkg/postage/stampissuer.go toBucket).
		bucket := binary.BigEndian.Uint32(addr[:4]) >> 16
		index := make([]byte, 8)
		binary.BigEndian.PutUint32(index[:4], bucket)
		binary.BigEndian.PutUint32(index[4:], uint32(i)+3)
		timestamp := be8(1720000000000000000 + uint64(i))

		digest, err := postage.ToSignDigest(addr, batchID, index, timestamp)
		if err != nil {
			panic(err)
		}
		sig, err := k.signer.Sign(digest)
		if err != nil {
			panic(err)
		}
		marshaled, err := postage.NewStamp(batchID, index, timestamp, sig).MarshalBinary()
		if err != nil {
			panic(err)
		}
		cases = append(cases, stampCase{
			Name:         fmt.Sprintf("chunk-%d", i),
			SecretHex:    hexs(k.secret),
			OwnerHex:     hexs(k.owner),
			ChunkAddrHex: hexs(addr),
			BatchIDHex:   hexs(batchID),
			IndexHex:     hexs(index),
			TimestampHex: hexs(timestamp),
			DigestHex:    hexs(digest),
			SignatureHex: hexs(sig),
			StampHex:     hexs(marshaled),
		})
	}
	return map[string]any{
		"description": "postage stamps: digest = keccak256(chunkAddr || batchID || index || ts) via bee postage.ToSignDigest, signed with Ethereum message prefix; stamp wire = batchID || index || ts || sig (113 bytes)",
		"cases":       cases,
	}
}

// --- dispersed replicas -----------------------------------------------------

type replicaCase struct {
	Entropy       uint8  `json:"entropy"`
	IDHex         string `json:"idHex"`
	SocAddressHex string `json:"socAddressHex"`
}

func replicaVectors() any {
	ch, err := cac.New(det("replica/chunk/1", 500))
	if err != nil {
		panic(err)
	}
	addr := ch.Address().Bytes()
	var cases []replicaCase
	for i := 0; i < 32; i++ {
		// bee pkg/replicas/replicas.go:56 replicate(): id = addr with
		// id[0] overwritten by the entropy byte.
		id := make([]byte, 32)
		copy(id, addr)
		id[0] = byte(i)
		socAddr, err := soc.CreateAddress(id, swarm.ReplicasOwner)
		if err != nil {
			panic(err)
		}
		cases = append(cases, replicaCase{
			Entropy:       uint8(i),
			IDHex:         hexs(id),
			SocAddressHex: socAddr.String(),
		})
	}
	return map[string]any{
		"description":      "dispersed replica addresses: id = chunk address with id[0] = entropy, soc address = keccak256(id || ReplicasOwner)",
		"baseAddressHex":   hexs(addr),
		"replicasOwnerHex": hexs(swarm.ReplicasOwner),
		"replicaCounts":    redundancy.GetReplicaCounts(),
		"cases":            cases,
	}
}

// --- redundancy / erasure tables ---------------------------------------------

type redundancyLevel struct {
	Level        int    `json:"level"`
	Name         string `json:"name"`
	ReplicaCount int    `json:"replicaCount"`
	MaxShards    int    `json:"maxShards"`
	MaxEncShards int    `json:"maxEncShards"`
	// Parities[i] = GetParities(i+1), i.e. parity count when i+1 chunk
	// slots are used in an intermediate node.
	Parities    []int `json:"parities"`
	EncParities []int `json:"encParities"`
}

func redundancyVectors() any {
	names := []string{"NONE", "MEDIUM", "STRONG", "INSANE", "PARANOID"}
	var levels []redundancyLevel
	for l := redundancy.NONE; l <= redundancy.PARANOID; l++ {
		parities := make([]int, swarm.Branches)
		for n := 1; n <= swarm.Branches; n++ {
			parities[n-1] = l.GetParities(n)
		}
		encParities := make([]int, swarm.EncryptedBranches)
		for n := 1; n <= swarm.EncryptedBranches; n++ {
			encParities[n-1] = l.GetEncParities(n)
		}
		levels = append(levels, redundancyLevel{
			Level:        int(l),
			Name:         names[l],
			ReplicaCount: l.GetReplicaCount(),
			MaxShards:    l.GetMaxShards(),
			MaxEncShards: l.GetMaxEncShards(),
			Parities:     parities,
			EncParities:  encParities,
		})
	}
	return map[string]any{
		"description": "bee pkg/file/redundancy parity tables: parities[n-1] = GetParities(n) for n used slots per intermediate node",
		"levels":      levels,
	}
}

// --- redundancy-encoded files (Reed-Solomon fixtures) -------------------------

type rsChunk struct {
	AddressHex string `json:"addressHex"`
	DataHex    string `json:"dataHex"`
}

type rsCase struct {
	Name     string `json:"name"`
	Level    int    `json:"level"`
	FileSize int    `json:"fileSize"`
	RootHex  string `json:"rootHex"`
	// Every chunk bee's pipeline emitted (data + intermediate + parity),
	// keyed by address; a decoder test deletes some data chunks and must
	// recover them from the parities.
	Chunks []rsChunk `json:"chunks"`
	// Dispersed replicas of the root chunk (soc wires), as bee's
	// replicas.NewPutter would upload for this level.
	RootReplicas []rsChunk `json:"rootReplicas"`
}

type collectPutter struct {
	mu     sync.Mutex
	chunks map[string][]byte
}

func (c *collectPutter) Put(_ context.Context, ch swarm.Chunk) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chunks[ch.Address().String()] = append([]byte(nil), ch.Data()...)
	return nil
}

func rsFileCase(name string, level int, size int) rsCase {
	payload := det(fmt.Sprintf("rs/%s", name), size)
	put := &collectPutter{chunks: make(map[string][]byte)}
	p := builder.NewPipelineBuilder(context.Background(), put, false, redundancy.Level(level))
	root, err := builder.FeedPipeline(context.Background(), p, bytes.NewReader(payload))
	if err != nil {
		panic(err)
	}

	// replicate the root like bee's upload path does for redundant files
	repPut := &collectPutter{chunks: make(map[string][]byte)}
	rp := replicas.NewPutter(repPut, redundancy.Level(level))
	rootData, ok := put.chunks[root.String()]
	if !ok {
		panic("root chunk missing from collected set")
	}
	if err := rp.Put(context.Background(), swarm.NewChunk(root, rootData)); err != nil {
		panic(err)
	}

	toSorted := func(m map[string][]byte) []rsChunk {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]rsChunk, 0, len(keys))
		for _, k := range keys {
			out = append(out, rsChunk{AddressHex: k, DataHex: hexs(m[k])})
		}
		return out
	}
	// replicas putter also re-puts the root itself; drop it from the
	// replica list so the field holds only the SOC replicas
	delete(repPut.chunks, root.String())

	return rsCase{
		Name:         name,
		Level:        level,
		FileSize:     size,
		RootHex:      root.String(),
		Chunks:       toSorted(put.chunks),
		RootReplicas: toSorted(repPut.chunks),
	}
}

func redundantFileVectors() any {
	cases := []rsCase{
		// single intermediate level (< maxShards data chunks per level)
		rsFileCase("medium-150k", 1, 150*1024),
		rsFileCase("strong-150k", 2, 150*1024),
		rsFileCase("insane-150k", 3, 150*1024),
		rsFileCase("paranoid-50k", 4, 50*1024),
		// two intermediate nodes at level 1 (147 data chunks > 119 max shards)
		rsFileCase("medium-600k", 1, 600*1024),
	}
	return map[string]any{
		"description": "files encoded by bee's real pipeline (builder.NewPipelineBuilder) with swarm-redundancy levels; chunks = every emitted chunk (data+intermediate+parity); span top byte encodes level|0x80 on intermediate nodes; parity count per node from the level's erasure table; rootReplicas = dispersed replica SOCs from replicas.NewPutter",
		"cases":       cases,
	}
}
