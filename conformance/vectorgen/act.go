// ACT (access control) vectors, produced by bee's canonical
// pkg/accesscontrol implementation.
//
// Two tiers, mirroring the encryption vectors:
//
//   - Deterministic cases pin the primitives with injected keys: the
//     ECDH session KDF (lookup key / access-key-decryption key), the
//     access-key wrap, reference encryption, the ACT key-value store's
//     simple-manifest JSON bytes + chunk address, and multi-epoch
//     history manifests built through bee's real load-add-store cycle
//     (fixed timestamps -> byte-exact roots), plus a Lookup truth
//     table.
//
//   - Frozen flows run the REAL accesscontrol.Controller (random
//     access keys, random grantee-list encryption) and dump every
//     chunk it stored: an upload flow (UploadHandler with a fresh
//     history) and a grantee flow (UpdateHandler adding two grantees,
//     then UploadHandler against that history). Ant must resolve both
//     from the frozen chunks alone.
package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"sort"

	"github.com/ethersphere/bee/v2/pkg/accesscontrol"
	"github.com/ethersphere/bee/v2/pkg/accesscontrol/kvs"
	"github.com/ethersphere/bee/v2/pkg/crypto"
	"github.com/ethersphere/bee/v2/pkg/encryption"
	"github.com/ethersphere/bee/v2/pkg/file"
	"github.com/ethersphere/bee/v2/pkg/file/loadsave"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline"
	"github.com/ethersphere/bee/v2/pkg/file/pipeline/builder"
	"github.com/ethersphere/bee/v2/pkg/file/redundancy"
	"github.com/ethersphere/bee/v2/pkg/storage/inmemchunkstore"
	"github.com/ethersphere/bee/v2/pkg/swarm"
	"golang.org/x/crypto/sha3"
)

type actKdfCase struct {
	Name string `json:"name"`
	// The session holder's private scalar.
	SecretHex string `json:"secretHex"`
	// The counterparty public key (compressed SEC1).
	CounterpartyPubHex string `json:"counterpartyPubHex"`
	// Derived keys for nonces [0x00] and [0x01].
	LookupKeyHex string `json:"lookupKeyHex"`
	AkdkHex      string `json:"akdkHex"`
}

type actRefCase struct {
	Name         string `json:"name"`
	RefHex       string `json:"refHex"`
	AccessKeyHex string `json:"accessKeyHex"`
	EncryptedHex string `json:"encryptedHex"`
}

type actChunk struct {
	AddressHex string `json:"addressHex"`
	DataHex    string `json:"dataHex"`
}

type actKvsRow struct {
	KeyHex   string `json:"keyHex"`
	ValueHex string `json:"valueHex"`
}

type actKvsCase struct {
	Name         string      `json:"name"`
	Rows         []actKvsRow `json:"rows"`
	ManifestJSON string      `json:"manifestJson"`
	RootHex      string      `json:"rootHex"`
	Chunks       []actChunk  `json:"chunks"`
}

type actHistoryStep struct {
	Timestamp int64             `json:"timestamp"`
	ActRefHex string            `json:"actRefHex"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	// History root after this step's load-add-store cycle.
	RootHex string `json:"rootHex"`
}

type actLookupCase struct {
	Timestamp int64 `json:"timestamp"`
	// Expected entry reference; "" when Lookup errors.
	ActRefHex string `json:"actRefHex"`
	Error     string `json:"error,omitempty"`
}

type actHistoryCase struct {
	Name    string           `json:"name"`
	Steps   []actHistoryStep `json:"steps"`
	Lookups []actLookupCase  `json:"lookups"`
	Chunks  []actChunk       `json:"chunks"`
}

type actUploadFlow struct {
	PublisherSecretHex string     `json:"publisherSecretHex"`
	ContentRefHex      string     `json:"contentRefHex"`
	HistoryRefHex      string     `json:"historyRefHex"`
	EncryptedRefHex    string     `json:"encryptedRefHex"`
	Chunks             []actChunk `json:"chunks"`
}

type actGranteeFlow struct {
	PublisherSecretHex string   `json:"publisherSecretHex"`
	GranteeSecretsHex  []string `json:"granteeSecretsHex"`
	// Compressed grantee public keys, in bee's list order.
	GranteePubsHex  []string   `json:"granteePubsHex"`
	EGranteeRefHex  string     `json:"egranteeRefHex"`
	HistoryRefHex   string     `json:"historyRefHex"`
	ContentRefHex   string     `json:"contentRefHex"`
	EncryptedRefHex string     `json:"encryptedRefHex"`
	Chunks          []actChunk `json:"chunks"`
}

func actPrivateKey(label string) *ecdsa.PrivateKey {
	pk, err := crypto.DecodeSecp256k1PrivateKey(det(label, 32))
	if err != nil {
		panic(err)
	}
	return pk
}

func compressedHex(pub *ecdsa.PublicKey) string {
	return hexs(crypto.EncodeSecp256k1PublicKey(pub))
}

// actStore couples an in-memory chunk store with bee's real loadsave
// (plain pipeline at redundancy NONE — what the api's ACT paths use).
func actStore() (*inmemchunkstore.ChunkStore, file.LoadSaver, file.LoadSaver) {
	store := inmemchunkstore.New()
	plain := loadsave.New(store, store, func() pipeline.Interface {
		return builder.NewPipelineBuilder(context.Background(), store, false, redundancy.NONE)
	}, redundancy.NONE)
	encrypted := loadsave.New(store, store, func() pipeline.Interface {
		return builder.NewPipelineBuilder(context.Background(), store, true, redundancy.NONE)
	}, redundancy.NONE)
	return store, plain, encrypted
}

func dumpChunks(store *inmemchunkstore.ChunkStore) []actChunk {
	m := map[string][]byte{}
	err := store.Iterate(context.Background(), func(ch swarm.Chunk) (bool, error) {
		m[ch.Address().String()] = append([]byte(nil), ch.Data()...)
		return false, nil
	})
	if err != nil {
		panic(err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]actChunk, 0, len(keys))
	for _, k := range keys {
		out = append(out, actChunk{AddressHex: k, DataHex: hexs(m[k])})
	}
	return out
}

func sessionKeys(priv *ecdsa.PrivateKey, pub *ecdsa.PublicKey) (lookup, akdk []byte) {
	s := accesscontrol.NewDefaultSession(priv)
	keys, err := s.Key(pub, [][]byte{{0}, {1}})
	if err != nil {
		panic(err)
	}
	return keys[0], keys[1]
}

func actXor(key, data []byte) []byte {
	out, err := encryption.New(encryption.Key(key), 0, 0, sha3.NewLegacyKeccak256).Encrypt(data)
	if err != nil {
		panic(err)
	}
	return out
}

func actVectors() any {
	ctx := context.Background()
	publisher := actPrivateKey("act/publisher")
	grantee1 := actPrivateKey("act/grantee/1")
	grantee2 := actPrivateKey("act/grantee/2")

	// --- KDF cases (deterministic) ---
	kdf := func(name, secretLabel string, priv *ecdsa.PrivateKey, pub *ecdsa.PublicKey) actKdfCase {
		lookup, akdk := sessionKeys(priv, pub)
		return actKdfCase{
			Name:               name,
			SecretHex:          hexs(det(secretLabel, 32)),
			CounterpartyPubHex: compressedHex(pub),
			LookupKeyHex:       hexs(lookup),
			AkdkHex:            hexs(akdk),
		}
	}
	kdfCases := []actKdfCase{
		// Publisher with itself (the fresh-history self-grant).
		kdf("publisher-self", "act/publisher", publisher, &publisher.PublicKey),
		// Publisher -> grantee (AddGrantee's wrap keys).
		kdf("publisher-to-grantee1", "act/publisher", publisher, &grantee1.PublicKey),
		// Grantee -> publisher (the download side; must equal the above
		// pair thanks to ECDH symmetry).
		kdf("grantee1-to-publisher", "act/grantee/1", grantee1, &publisher.PublicKey),
	}

	// --- access-key wrap + reference encryption (deterministic) ---
	accessKey := det("act/accesskey", 32)
	_, akdkPub := sessionKeys(publisher, &grantee1.PublicKey)
	wrap := actRefCase{
		Name:         "wrap-access-key",
		RefHex:       hexs(accessKey),
		AccessKeyHex: hexs(akdkPub),
		EncryptedHex: hexs(actXor(akdkPub, accessKey)),
	}
	ref32 := det("act/ref/plain", 32)
	ref64 := det("act/ref/encrypted", 64)
	refCases := []actRefCase{
		wrap,
		{
			Name:         "encrypt-ref-32",
			RefHex:       hexs(ref32),
			AccessKeyHex: hexs(accessKey),
			EncryptedHex: hexs(actXor(accessKey, ref32)),
		},
		{
			Name:         "encrypt-ref-64",
			RefHex:       hexs(ref64),
			AccessKeyHex: hexs(accessKey),
			EncryptedHex: hexs(actXor(accessKey, ref64)),
		},
	}

	// --- kvs manifest (deterministic; bee's kvs over a simple manifest) ---
	kvsCases := []actKvsCase{}
	{
		store, ls, _ := actStore()
		s, err := kvs.New(ls)
		if err != nil {
			panic(err)
		}
		lookupP, akdkP := sessionKeys(publisher, &publisher.PublicKey)
		lookup1, akdk1 := sessionKeys(publisher, &grantee1.PublicKey)
		lookup2, akdk2 := sessionKeys(publisher, &grantee2.PublicKey)
		rows := []actKvsRow{}
		for _, pair := range [][2][]byte{
			{lookupP, actXor(akdkP, accessKey)},
			{lookup1, actXor(akdk1, accessKey)},
			{lookup2, actXor(akdk2, accessKey)},
		} {
			if err := s.Put(ctx, pair[0], pair[1]); err != nil {
				panic(err)
			}
			rows = append(rows, actKvsRow{KeyHex: hexs(pair[0]), ValueHex: hexs(pair[1])})
		}
		root, err := s.Save(ctx)
		if err != nil {
			panic(err)
		}
		chunks := dumpChunks(store)
		// The manifest JSON is the (single) root chunk's payload past
		// the 8-byte span.
		var manifestJSON string
		for _, c := range chunks {
			if c.AddressHex == root.String() {
				data := mustDecodeHex(c.DataHex)
				manifestJSON = string(data[8:])
			}
		}
		kvsCases = append(kvsCases, actKvsCase{
			Name:         "three-grantee-kvs",
			Rows:         rows,
			ManifestJSON: manifestJSON,
			RootHex:      root.String(),
			Chunks:       chunks,
		})
	}

	// --- history manifests (deterministic timestamps, bee's real
	//     load-add-store cycle, exactly what the controller does) ---
	historyCases := []actHistoryCase{}
	{
		store, ls, _ := actStore()
		t1, t2, t3 := int64(1_700_000_000), int64(1_700_001_000), int64(1_700_005_000)
		actRef1 := swarm.NewAddress(det("act/history/actref/1", 32))
		actRef2 := swarm.NewAddress(det("act/history/actref/2", 32))
		actRef3 := swarm.NewAddress(det("act/history/actref/3", 32))
		glref2 := det("act/history/glref/2", 64)
		glref3 := det("act/history/glref/3", 64)

		steps := []actHistoryStep{}
		h, err := accesscontrol.NewHistory(ls)
		if err != nil {
			panic(err)
		}
		if err := h.Add(ctx, actRef1, &t1, nil); err != nil {
			panic(err)
		}
		root1, err := h.Store(ctx)
		if err != nil {
			panic(err)
		}
		steps = append(steps, actHistoryStep{Timestamp: t1, ActRefHex: actRef1.String(), RootHex: root1.String()})

		h, err = accesscontrol.NewHistoryReference(ls, root1)
		if err != nil {
			panic(err)
		}
		mtdt2 := map[string]string{"encryptedglref": hexs(glref2)}
		if err := h.Add(ctx, actRef2, &t2, &mtdt2); err != nil {
			panic(err)
		}
		root2, err := h.Store(ctx)
		if err != nil {
			panic(err)
		}
		steps = append(steps, actHistoryStep{Timestamp: t2, ActRefHex: actRef2.String(), Metadata: mtdt2, RootHex: root2.String()})

		h, err = accesscontrol.NewHistoryReference(ls, root2)
		if err != nil {
			panic(err)
		}
		mtdt3 := map[string]string{"encryptedglref": hexs(glref3)}
		if err := h.Add(ctx, actRef3, &t3, &mtdt3); err != nil {
			panic(err)
		}
		root3, err := h.Store(ctx)
		if err != nil {
			panic(err)
		}
		steps = append(steps, actHistoryStep{Timestamp: t3, ActRefHex: actRef3.String(), Metadata: mtdt3, RootHex: root3.String()})

		// Lookup truth table against the final history.
		lookups := []actLookupCase{}
		final, err := accesscontrol.NewHistoryReference(ls, root3)
		if err != nil {
			panic(err)
		}
		for _, ts := range []int64{0, 1, t1 - 1, t1, t1 + 500, t2, t2 + 1, t3, t3 + 1_000_000} {
			entry, err := final.Lookup(ctx, ts)
			if err != nil {
				lookups = append(lookups, actLookupCase{Timestamp: ts, Error: err.Error()})
				continue
			}
			lookups = append(lookups, actLookupCase{Timestamp: ts, ActRefHex: entry.Reference().String()})
		}

		historyCases = append(historyCases, actHistoryCase{
			Name:    "three-epochs",
			Steps:   steps,
			Lookups: lookups,
			Chunks:  dumpChunks(store),
		})
	}

	// --- frozen controller flows (random keys, frozen once written) ---
	uploadFlow := actUploadFlow{}
	{
		store, ls, _ := actStore()
		ctrl := accesscontrol.NewController(accesscontrol.NewLogic(
			accesscontrol.NewDefaultSession(publisher),
		))
		contentRef := swarm.NewAddress(det("act/upload/content", 32))
		_, historyRef, encryptedRef, err := ctrl.UploadHandler(ctx, ls, contentRef, &publisher.PublicKey, swarm.ZeroAddress)
		if err != nil {
			panic(err)
		}
		uploadFlow = actUploadFlow{
			PublisherSecretHex: hexs(det("act/publisher", 32)),
			ContentRefHex:      contentRef.String(),
			HistoryRefHex:      historyRef.String(),
			EncryptedRefHex:    encryptedRef.String(),
			Chunks:             dumpChunks(store),
		}
	}

	granteeFlow := actGranteeFlow{}
	{
		store, ls, gls := actStore()
		ctrl := accesscontrol.NewController(accesscontrol.NewLogic(
			accesscontrol.NewDefaultSession(publisher),
		))
		addList := []*ecdsa.PublicKey{&grantee1.PublicKey, &grantee2.PublicKey}
		_, egranteeRef, historyRef, _, err := ctrl.UpdateHandler(
			ctx, ls, gls, swarm.ZeroAddress, swarm.ZeroAddress, &publisher.PublicKey, addList, nil,
		)
		if err != nil {
			panic(err)
		}
		contentRef := swarm.NewAddress(det("act/grantee/content", 32))
		_, _, encryptedRef, err := ctrl.UploadHandler(ctx, ls, contentRef, &publisher.PublicKey, historyRef)
		if err != nil {
			panic(err)
		}
		granteeFlow = actGranteeFlow{
			PublisherSecretHex: hexs(det("act/publisher", 32)),
			GranteeSecretsHex:  []string{hexs(det("act/grantee/1", 32)), hexs(det("act/grantee/2", 32))},
			GranteePubsHex:     []string{compressedHex(&grantee1.PublicKey), compressedHex(&grantee2.PublicKey)},
			EGranteeRefHex:     egranteeRef.String(),
			HistoryRefHex:      historyRef.String(),
			ContentRefHex:      contentRef.String(),
			EncryptedRefHex:    encryptedRef.String(),
			Chunks:             dumpChunks(store),
		}
	}

	return map[string]any{
		"description":        "swarm ACT (access control): kdfCases/refCases/kvsCases/historyCases pin bee's pkg/accesscontrol primitives with injected keys and fixed timestamps (byte-exact for ant); uploadFlow and granteeFlow freeze full accesscontrol.Controller runs (random access keys + encrypted grantee lists) that ant must resolve from the chunks alone",
		"publisherSecretHex": hexs(det("act/publisher", 32)),
		"publisherPubHex":    compressedHex(&publisher.PublicKey),
		"kdfCases":           kdfCases,
		"refCases":           refCases,
		"kvsCases":           kvsCases,
		"historyCases":       historyCases,
		"uploadFlow":         uploadFlow,
		"granteeFlow":        granteeFlow,
	}
}

func mustDecodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}
