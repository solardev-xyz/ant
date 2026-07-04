// Command beemock serves the canonical bee Swarm HTTP API over in-memory mock
// backends. It is the oracle for differential conformance testing of the ant
// client: the wiring below deliberately mirrors bee's own API test harness
// (newTestServer in pkg/api/api_test.go) so responses match real bee behavior.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	mockac "github.com/ethersphere/bee/v2/pkg/accesscontrol/mock"
	accountingmock "github.com/ethersphere/bee/v2/pkg/accounting/mock"
	"github.com/ethersphere/bee/v2/pkg/api"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/feeds/factory"
	"github.com/ethersphere/bee/v2/pkg/log"
	p2pmock "github.com/ethersphere/bee/v2/pkg/p2p/mock"
	"github.com/ethersphere/bee/v2/pkg/postage"
	mockbatchstore "github.com/ethersphere/bee/v2/pkg/postage/batchstore/mock"
	mockpost "github.com/ethersphere/bee/v2/pkg/postage/mock"
	"github.com/ethersphere/bee/v2/pkg/pusher"
	resolverMock "github.com/ethersphere/bee/v2/pkg/resolver/mock"
	"github.com/ethersphere/bee/v2/pkg/settlement/pseudosettle"
	chequebookmock "github.com/ethersphere/bee/v2/pkg/settlement/swap/chequebook/mock"
	erc20mock "github.com/ethersphere/bee/v2/pkg/settlement/swap/erc20/mock"
	swapmock "github.com/ethersphere/bee/v2/pkg/settlement/swap/mock"
	statestore "github.com/ethersphere/bee/v2/pkg/statestore/mock"
	"github.com/ethersphere/bee/v2/pkg/steward"
	"github.com/ethersphere/bee/v2/pkg/storage"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemstore"
	mockstorer "github.com/ethersphere/bee/v2/pkg/storer/mock"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/ethersphere/bee/v2/pkg/topology/lightnode"
	topologymock "github.com/ethersphere/bee/v2/pkg/topology/mock"
	"github.com/ethersphere/bee/v2/pkg/tracing"
	"github.com/ethersphere/bee/v2/pkg/transaction/backendmock"
	transactionmock "github.com/ethersphere/bee/v2/pkg/transaction/mock"
)

// Fixed 32-byte secrets so the node identity (and therefore the printed owner
// address) is deterministic across runs. Any scalar below the secp256k1 curve
// order works; 0xbe/0xcd... are comfortably below it.
var (
	nodeKeySecret = bytes.Repeat([]byte{0xbe}, 32)
	pssKeySecret  = bytes.Repeat([]byte{0xcd}, 32)
	// The postage service below is created WithAcceptAll, which returns a fresh
	// usable stamp issuer for ANY batch ID passed in swarm-postage-batch-id
	// (see pkg/postage/mock GetStampIssuer). We advertise this fixed ID so
	// clients have a canonical value to use.
	batchID = bytes.Repeat([]byte{0xbb}, 32)
)

// localRetriever adapts the mock storer's chunk store to retrieval.Interface
// so the REAL steward can be constructed without networking (same trick as
// bee's pkg/api/stewardship_test.go).
type localRetriever struct {
	getter storage.Getter
}

func (lr *localRetriever) RetrieveChunk(ctx context.Context, addr, _ swarm.Address) (swarm.Chunk, error) {
	ch, err := lr.getter.Get(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("retrieve chunk %s: %w", addr, err)
	}
	return ch, nil
}

// drainPusherFeed reimplements the protocol of api_test.go's chanStorer:
// mockstorer's DirectUpload() blocks sending each chunk on PusherFeed(), so
// without a consumer any request with Swarm-Deferred-Upload: false would hang.
// Acking is `op.Err <- nil` (Err is created with buffer 1 by mockstorer).
//
// Unlike api_test's chanStorer (which only records addresses), we persist each
// pushed chunk into the shared chunk store: on a real node a successful direct
// upload makes the chunk retrievable from the network, and the chunk store is
// this oracle's stand-in for the network. Without this, e.g. feed updates
// posted via /soc (which defaults to direct upload) would never be found by
// GET /feeds.
func drainPusherFeed(cc <-chan *pusher.Op, store storage.Putter, quit <-chan struct{}) {
	for {
		select {
		case op := <-cc:
			op.Err <- store.Put(context.Background(), op.Chunk)
		case <-quit:
			return
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:1635", "TCP address to listen on")
	flag.Parse()

	logger := log.Noop

	// Deterministic node identity.
	pk, err := crypto.DecodeSecp256k1PrivateKey(nodeKeySecret)
	if err != nil {
		fatal("decode node key: %v", err)
	}
	signer := crypto.NewDefaultSigner(pk)
	pssPk, err := crypto.DecodeSecp256k1PrivateKey(pssKeySecret)
	if err != nil {
		fatal("decode pss key: %v", err)
	}
	ethAddrBytes, err := crypto.NewEthereumAddress(pk.PublicKey)
	if err != nil {
		fatal("derive ethereum address: %v", err)
	}
	ethereumAddress := common.BytesToAddress(ethAddrBytes)
	overlay, err := crypto.NewOverlayAddress(pk.PublicKey, 1, common.HexToHash("0x01").Bytes())
	if err != nil {
		fatal("derive overlay address: %v", err)
	}

	// REAL in-memory subsystems where bee's real code can run without a network.
	// The chunk store is created explicitly so the pusher-feed drain below can
	// write direct uploads into it.
	chunkStore := inmemchunkstore.New()
	storer := mockstorer.NewWithChunkStore(chunkStore)
	// Real feed factory over the mock storer's chunk store, exactly as bee's
	// node.go wires it: factory.New(localStore.Download(true)).
	feedFactory := factory.New(storer.Download(true))
	// Real steward over the mock storer + a local-only "retrieval" adapter.
	stewardSvc := steward.New(storer, &localRetriever{getter: storer.ChunkStore()}, storer.Cache())

	post := mockpost.New(mockpost.WithAcceptAll())
	// The mock batchstore must also carry a real *postage.Batch: bee's
	// presigned-stamp upload path (`swarm-postage-stamp` header →
	// newStampedPutter) calls batchStore.Get and dereferences
	// batch.Owner to verify the stamp signer. Without WithBatch the
	// mock returns a nil batch (with a nil error) and the handler
	// panics — a mock artifact real bee (whose batchstore returns the
	// on-chain batch) never exhibits. Owner is this node's address so
	// stamps minted by our own POST /envelope validate.
	batchStore := mockbatchstore.New(
		mockbatchstore.WithAcceptAllExistsFunc(),
		mockbatchstore.WithBatch(&postage.Batch{
			ID:          batchID,
			Value:       big.NewInt(100_000_000),
			Start:       1,
			Owner:       ethAddrBytes,
			Depth:       24,
			BucketDepth: 16,
			Immutable:   false,
		}),
	)
	accessControl := mockac.New()

	// Remaining mocks, mirroring newTestServer defaults.
	topologyDriver := topologymock.NewTopologyDriver()
	acc := accountingmock.NewAccounting()
	settlement := swapmock.New()
	chequebook := chequebookmock.NewChequebook()
	ln := lightnode.NewContainer(overlay)
	transactionSvc := transactionmock.New()
	stateStore := statestore.NewStateStore()
	pseudosettleSvc := pseudosettle.New(nil, logger, stateStore, nil, big.NewInt(10000), big.NewInt(10000), p2pmock.New())
	erc20 := erc20mock.New()
	backend := backendmock.New()

	extraOpts := api.ExtraOptions{
		TopologyDriver: topologyDriver,
		Accounting:     acc,
		Pseudosettle:   pseudosettleSvc,
		LightNodes:     ln,
		Swap:           settlement,
		Chequebook:     chequebook,
		Storer:         storer,
		Resolver:       resolverMock.NewResolver(),
		FeedFactory:    feedFactory,
		Post:           post,
		AccessControl:  accessControl,
		Steward:        stewardSvc,
		SyncStatus:     func() (bool, error) { return true, nil },
	}

	s := api.New(
		pk.PublicKey,
		pssPk.PublicKey,
		ethereumAddress,
		nil, // no whitelisted withdrawal addresses
		logger,
		transactionSvc,
		batchStore,
		api.FullMode,
		true, // chequebook enabled
		true, // swap enabled
		backend,
		[]string{"*"}, // CORS
		inmemstore.New(),
	)
	defer s.Close()

	s.SetP2P(nil)
	// Note: no redistribution agent is set (api_test creates one from a
	// test-only mock contract). Only /redistributionstate depends on it.
	s.SetSwarmAddress(&overlay)
	probe := api.NewProbe()
	probe.SetHealthy(api.ProbeStatusOK)
	probe.SetReady(api.ProbeStatusOK)
	s.SetProbe(probe)

	noOpTracer, tracerCloser, err := tracing.NewTracer(&tracing.Options{Enabled: false})
	if err != nil {
		fatal("tracer: %v", err)
	}
	defer tracerCloser.Close()

	s.Configure(signer, noOpTracer, api.Options{
		CORSAllowedOrigins: []string{"*"},
		WsPingPeriod:       60 * time.Second,
	}, extraOpts, 1 /* chain ID */, erc20)

	s.Mount()
	s.EnableFullAPI()

	// Drain direct uploads unconditionally (api_test only does this for
	// DirectUpload tests, but a conformance oracle must accept both modes).
	quit := make(chan struct{})
	defer close(quit)
	go drainPusherFeed(storer.PusherFeed(), chunkStore, quit)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fatal("listen on %s: %v", *addr, err)
	}
	srv := &http.Server{Handler: s}

	errC := make(chan error, 1)
	go func() { errC <- srv.Serve(listener) }()

	startup, _ := json.Marshal(map[string]string{
		"listening": listener.Addr().String(),
		"batchId":   hex.EncodeToString(batchID),
		"owner":     hex.EncodeToString(ethereumAddress.Bytes()),
	})
	fmt.Println(string(startup))

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigC:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	case err := <-errC:
		if err != nil && err != http.ErrServerClosed {
			fatal("serve: %v", err)
		}
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "beemock: "+format+"\n", args...)
	os.Exit(1)
}
