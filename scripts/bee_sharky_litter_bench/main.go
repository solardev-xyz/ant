// Command litterbench exercises the real bee local chunk path used for cached
// chunks: LevelDB retrieval index + sharky blob read, via
// pkg/storer/internal/transaction and pkg/storer/internal/chunkstore.
//
// Build from a bee v2 checkout (must live under pkg/storer/… so internal
// imports resolve):
//
//	cp -r scripts/bee_sharky_litter_bench $BEE/pkg/storer/cmd/litterbench
//	cd $BEE && go run ./pkg/storer/cmd/litterbench
//
// Chunk payloads match ant's litter_disk_cache_bench (4 KiB segments, CAC via
// pkg/cac). Dataset: litter-ally.eth assets under LITTER_BENCH_DIR.

package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethersphere/bee/v2/pkg/cac"
	"github.com/ethersphere/bee/v2/pkg/log"
	"github.com/ethersphere/bee/v2/pkg/sharky"
	"github.com/ethersphere/bee/v2/pkg/storage/leveldbstore"
	storagemigration "github.com/ethersphere/bee/v2/pkg/storage/migration"
	"github.com/ethersphere/bee/v2/pkg/storer/internal/transaction"
	localmigration "github.com/ethersphere/bee/v2/pkg/storer/migration"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	chunkPayloadMax = swarm.ChunkSize
	shardCount      = 32
)

type dirFS struct {
	basedir string
}

func (d *dirFS) Open(path string) (fs.File, error) {
	return os.OpenFile(filepath.Join(d.basedir, path), os.O_RDWR|os.O_CREATE, 0o644)
}

func collectFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	return files, err
}

func main() {
	baseDir := os.Getenv("LITTER_BENCH_DIR")
	if baseDir == "" {
		baseDir = "/tmp/litter_bench"
	}
	files, err := collectFiles(baseDir)
	if err != nil || len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no files under %s: %v\n", baseDir, err)
		os.Exit(2)
	}

	var chunks []swarm.Chunk
	var sourceBytes uint64
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			panic(err)
		}
		sourceBytes += uint64(len(data))
		for offset := 0; offset < len(data); offset += chunkPayloadMax {
			end := offset + chunkPayloadMax
			if end > len(data) {
				end = len(data)
			}
			payload := data[offset:end]
			ch, err := cac.New(payload)
			if err != nil {
				panic(err)
			}
			chunks = append(chunks, ch)
		}
	}
	n := len(chunks)
	var dataBytes uint64
	for _, ch := range chunks {
		dataBytes += uint64(len(ch.Data()))
	}
	fmt.Printf("source files: %d, source bytes: %.2f MiB, CAC chunks: %d, chunk.Data() bytes: %.2f MiB\n",
		len(files), float64(sourceBytes)/(1024*1024), n, float64(dataBytes)/(1024*1024))

	tmpRoot, err := os.MkdirTemp("", "bee-litter-storer-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpRoot)

	indexPath := filepath.Join(tmpRoot, "indexstore")
	if err := os.MkdirAll(indexPath, 0o777); err != nil {
		panic(err)
	}
	ldb, err := leveldbstore.New(indexPath, &opt.Options{
		OpenFilesCacheCapacity: 256,
		BlockCacheCapacity:     32 * 1024 * 1024,
		WriteBuffer:            32 * 1024 * 1024,
		DisableSeeksCompaction: false,
		CompactionL0Trigger:    8,
		Filter:                 filter.NewBloomFilter(64),
	})
	if err != nil {
		panic(err)
	}
	defer ldb.Close()

	if err := storagemigration.Migrate(ldb, "core-migration", localmigration.BeforeInitSteps(ldb, log.Noop)); err != nil {
		panic(err)
	}

	sharkyPath := filepath.Join(tmpRoot, "sharky")
	if err := os.MkdirAll(sharkyPath, 0o777); err != nil {
		panic(err)
	}
	dirty := filepath.Join(sharkyPath, ".DIRTY")
	if err := os.WriteFile(dirty, nil, 0o644); err != nil {
		panic(err)
	}
	defer os.Remove(dirty)

	sh, err := sharky.New(&dirFS{basedir: sharkyPath}, shardCount, swarm.SocMaxChunkSize)
	if err != nil {
		panic(err)
	}

	st := transaction.NewStorage(sh, ldb)
	defer st.Close()

	ctx := context.Background()
	const putBatch = 8192
	t0 := time.Now()
	for i := 0; i < len(chunks); i += putBatch {
		end := i + putBatch
		if end > len(chunks) {
			end = len(chunks)
		}
		slice := chunks[i:end]
		err := st.Run(ctx, func(s transaction.Store) error {
			for _, ch := range slice {
				if err := s.ChunkStore().Put(ctx, ch); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("populate %d chunks (batched tx, %d chunks/commit) in %.2fs\n", n, putBatch, time.Since(t0).Seconds())

	addrs := make([]swarm.Address, n)
	for i, ch := range chunks {
		addrs[i] = ch.Address()
	}
	avgData := float64(dataBytes) / float64(n)
	var sink atomic.Uint64

	for _, k := range []int{1, 2, 4, 8, 16, 32, 64} {
		var stop atomic.Bool
		var ops atomic.Uint64
		start := time.Now()
		dur := 3 * time.Second
		var wg sync.WaitGroup
		for ti := 0; ti < k; ti++ {
			ti := ti
			wg.Add(1)
			go func() {
				defer wg.Done()
				idx := ti
				cs := st.ChunkStore()
				for !stop.Load() {
					addr := addrs[idx%len(addrs)]
					idx++
					ch, err := cs.Get(ctx, addr)
					if err != nil {
						continue
					}
					ops.Add(1)
					sink.Add(uint64(len(ch.Data())))
				}
			}()
		}
		time.Sleep(dur)
		stop.Store(true)
		wg.Wait()
		elapsed := time.Since(start).Seconds()
		qps := float64(ops.Load()) / elapsed
		mibps := (qps * avgData) / (1024 * 1024)
		fmt.Printf("K=%2d bee chunkstore (sharky+retrieval idx): %10.0f ops/s  (~%6.1f MiB/s chunk.Data throughput)\n", k, qps, mibps)
	}
	_ = sink.Load()
}
