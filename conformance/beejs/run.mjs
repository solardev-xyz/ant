#!/usr/bin/env node
// bee-js drop-in pack: drive UNMODIFIED bee-js against ant's in-memory
// gateway (memgateway) and bee's real API layer (beemock), assert every
// operation succeeds on both, and that content-addressed references are
// byte-identical across backends.
//
// This is the "drop-in replacement" claim from ant's PLAN.md, tested
// with the canonical JS client instead of hand-rolled HTTP.
//
// Usage (spawns both servers itself):
//   node run.mjs
// Env overrides: BEEMOCK_BIN, MEMGATEWAY_BIN, BATCH_ID.

import { Bee, Topic, PrivateKey } from '@ethersphere/bee-js';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { existsSync } from 'node:fs';

const here = dirname(fileURLToPath(import.meta.url));
const BATCH = process.env.BATCH_ID ?? 'bb'.repeat(32);

// deterministic test identity — same key as the checked-in vectors
// (keccak stream of "ant-conformance/key/1"; owner ff6316…cb4e)
const FEED_KEY = new PrivateKey('b81dcb6350c980d80602e37c8d42ca7cf1ca93e85240bac7879cf7cecaeca9c6');
const TOPIC = Topic.fromString('beejs-pack/topic/1');

async function spawnServer(name, cmd, args) {
  const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'inherit'] });
  const rl = createInterface({ input: child.stdout });
  const [line] = await Promise.race([
    once(rl, 'line'),
    new Promise((_, rej) => setTimeout(() => rej(new Error(`${name}: no hello line`)), 120_000)),
  ]);
  const hello = JSON.parse(line);
  const base = `http://${hello.listening}`;
  // wait for health
  for (let i = 0; i < 100; i++) {
    try {
      const r = await fetch(`${base}/health`);
      if (r.ok) return { child, base };
    } catch {}
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`${name} not healthy at ${base}`);
}

const results = [];
function record(backend, name, ok, detail = '') {
  results.push({ backend, name, ok, detail });
  const icon = ok ? 'ok  ' : 'FAIL';
  console.log(`  [${icon}] ${backend.padEnd(4)} ${name}${detail ? ` — ${detail}` : ''}`);
}

// Each scenario returns a map of deterministic references for
// cross-backend comparison.
async function runScenarios(backend, base) {
  const bee = new Bee(base);
  const refs = {};

  // 1. bytes round-trip
  try {
    const payload = new TextEncoder().encode('beejs differential payload');
    const up = await bee.uploadData(BATCH, payload);
    refs.data = up.reference.toHex();
    const down = await bee.downloadData(up.reference);
    record(backend, 'uploadData/downloadData', Buffer.compare(down.toUint8Array(), payload) === 0, refs.data);
  } catch (e) {
    record(backend, 'uploadData/downloadData', false, e.message);
  }

  // 2. single file with metadata
  try {
    const up = await bee.uploadFile(BATCH, 'hello beejs world\n', 'hello.txt', { contentType: 'text/plain' });
    refs.file = up.reference.toHex();
    const down = await bee.downloadFile(up.reference);
    const ok = down.name === 'hello.txt' && new TextDecoder().decode(down.data.toUint8Array()) === 'hello beejs world\n';
    record(backend, 'uploadFile/downloadFile', ok, refs.file);
  } catch (e) {
    record(backend, 'uploadFile/downloadFile', false, e.message);
  }

  // 3. collection with index document + nested path
  try {
    const indexBytes = new TextEncoder().encode('<h1>index</h1>');
    const dataBytes = new TextEncoder().encode('nested data');
    const up = await bee.uploadCollection(
      BATCH,
      [
        { path: 'index.html', size: indexBytes.length, file: new File([indexBytes], 'index.html') },
        { path: 'sub/data.txt', size: dataBytes.length, file: new File([dataBytes], 'data.txt') },
      ],
      { indexDocument: 'index.html' },
    );
    refs.collection = up.reference.toHex();
    const down = await bee.downloadFile(up.reference, 'sub/data.txt');
    record(
      backend,
      'uploadCollection/downloadFile(subpath)',
      new TextDecoder().decode(down.data.toUint8Array()) === 'nested data',
      refs.collection,
    );
  } catch (e) {
    record(backend, 'uploadCollection/downloadFile(subpath)', false, e.message);
  }

  // 4. tags lifecycle
  try {
    const tag = await bee.createTag();
    const got = await bee.retrieveTag(tag.uid);
    await bee.deleteTag(tag.uid);
    record(backend, 'createTag/retrieveTag/deleteTag', got.uid === tag.uid);
  } catch (e) {
    record(backend, 'createTag/retrieveTag/deleteTag', false, e.message);
  }

  // 5. SOC write + read (client-signed, the feed-update primitive)
  try {
    const writer = bee.makeSOCWriter(FEED_KEY);
    const identifier = new Uint8Array(32).fill(7);
    const socPayload = new TextEncoder().encode('soc payload via bee-js');
    const up = await writer.upload(BATCH, identifier, socPayload);
    refs.soc = up.reference.toHex();
    const reader = bee.makeSOCReader(FEED_KEY.publicKey().address());
    const soc = await reader.download(identifier);
    record(backend, 'makeSOCWriter/makeSOCReader', Buffer.compare(soc.payload.toUint8Array(), socPayload) === 0, refs.soc);
  } catch (e) {
    record(backend, 'makeSOCWriter/makeSOCReader', false, e.message);
  }

  // 6. feeds: two updates, latest resolution, manifest dereference
  try {
    const writer = bee.makeFeedWriter(TOPIC, FEED_KEY);
    const u0 = await writer.uploadPayload(BATCH, new TextEncoder().encode('feed update zero'));
    const r0 = await writer.downloadPayload();
    const ok0 =
      new TextDecoder().decode(r0.payload.toUint8Array()) === 'feed update zero' &&
      r0.feedIndex.toBigInt() === 0n;
    const u1 = await writer.uploadPayload(BATCH, new TextEncoder().encode('feed update one'));
    const r1 = await writer.downloadPayload();
    const ok1 =
      new TextDecoder().decode(r1.payload.toUint8Array()) === 'feed update one' &&
      r1.feedIndex.toBigInt() === 1n;
    refs.feedUpdate0 = u0.reference.toHex();
    refs.feedUpdate1 = u1.reference.toHex();
    record(backend, 'feed uploadPayload x2 + latest', ok0 && ok1, `idx0=${ok0} idx1=${ok1}`);

    const latest = await bee.fetchLatestFeedUpdate(TOPIC, FEED_KEY.publicKey().address());
    record(backend, 'fetchLatestFeedUpdate', latest.feedIndexNext?.toBigInt() === 2n);

    // Manifest dereference needs a *reference-style* feed (the classic
    // website flow: update points at an uploaded file/collection). A
    // raw-payload feed can't be served through /bzz.
    const siteTopic = Topic.fromString('beejs-pack/site/1');
    const siteWriter = bee.makeFeedWriter(siteTopic, FEED_KEY);
    const site = await bee.uploadFile(BATCH, 'site content v1\n', 'site.txt', { contentType: 'text/plain' });
    await siteWriter.uploadReference(BATCH, site.reference);
    const manifest = await bee.createFeedManifest(BATCH, siteTopic, FEED_KEY.publicKey().address());
    refs.feedManifest = manifest.toHex();
    const viaManifest = await bee.downloadFile(manifest);
    record(
      backend,
      'createFeedManifest + downloadFile(deref)',
      new TextDecoder().decode(viaManifest.data.toUint8Array()) === 'site content v1\n',
      refs.feedManifest,
    );
  } catch (e) {
    record(backend, 'feeds', false, e.message);
  }

  return refs;
}

// --- main ---------------------------------------------------------------

const beemockBin = process.env.BEEMOCK_BIN ?? join(here, '../beemock/beemock');
if (!existsSync(beemockBin)) {
  console.error(`beemock binary not found at ${beemockBin} — build with: cd conformance/beemock && go build -o beemock .`);
  process.exit(2);
}
const memgatewayBin = process.env.MEMGATEWAY_BIN;

console.log('spawning beemock…');
const beemock = await spawnServer('beemock', beemockBin, ['-addr', '127.0.0.1:0']);
console.log(`  beemock at ${beemock.base}`);

console.log('spawning memgateway…');
const mem = memgatewayBin
  ? await spawnServer('memgateway', memgatewayBin, [])
  : await spawnServer('memgateway', 'cargo', ['run', '-q', '-p', 'ant-conformance', '--bin', 'memgateway']);
console.log(`  memgateway (ant) at ${mem.base}`);

let exitCode = 0;
try {
  console.log('\n--- bee (beemock) ---');
  const beeRefs = await runScenarios('bee', beemock.base);
  console.log('\n--- ant (memgateway) ---');
  const antRefs = await runScenarios('ant', mem.base);

  console.log('\n--- cross-backend reference equality ---');
  for (const key of Object.keys(beeRefs)) {
    const same = beeRefs[key] === antRefs[key];
    record('xref', key, same, same ? beeRefs[key] : `bee=${beeRefs[key]} ant=${antRefs[key]}`);
  }

  const failures = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failures.length}/${results.length} checks passed`);
  if (failures.length > 0) exitCode = 1;
} finally {
  beemock.child.kill();
  mem.child.kill();
}
process.exit(exitCode);
