// GSOC TWO-UPDATE regression against live mainnet — the case the
// 2026-07-13 hardening pass exists for: the old lurker deduped by chunk
// address alone, and since GSOC reuses one stable SOC address per
// (identifier, owner), every update after the first was silently
// dropped. This script sends two different payloads to the SAME SOC
// address and requires BOTH to come back through the subscription.
//
// Caveat pinned in PSS-GSOC.md ("GSOC same-address rooms"): the reserve
// keeps one SOC per address, so update #2 must land *after* the lurker
// already pulled update #1 — we wait for delivery #1 before sending #2.
import { Bee, Identifier } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API = 'http://127.0.0.1:1733';
const BATCH = process.env.BATCH;
const OVERLAY = process.env.OVERLAY;
const bee = new Bee(API);

const identifier = Identifier.fromString('ant-gsoc-2up-' + Date.now());
const signer = bee.gsocMine(OVERLAY, identifier, 12);
console.log('mined GSOC signer; SOC address =', signer.publicKey().address().toString());

const received = [];
const sub = bee.gsocSubscribe(signer.publicKey().address(), identifier, {
  onMessage: (msg) => {
    const s = Buffer.from(msg.toUint8Array ? msg.toUint8Array() : msg).toString('utf8');
    console.log('  📨 delivered:', s);
    received.push(s);
  },
  onError: (e) => console.log('sub error:', e.message),
  onClose: () => {},
});

await new Promise((r) => setTimeout(r, 12000)); // WS open + lurker start

const p1 = 'update-1 @ ' + new Date().toISOString();
console.log('sending update #1…');
await bee.gsocSend(BATCH, signer, identifier, p1);

let deadline = Date.now() + 90000;
while (!received.includes(p1) && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1000));
}
if (!received.includes(p1)) {
  console.log('\n❌ update #1 never delivered — cannot even reach the regression');
  sub.cancel();
  process.exit(2);
}

const p2 = 'update-2 @ ' + new Date().toISOString();
console.log('update #1 confirmed; sending update #2 to the SAME address…');
await bee.gsocSend(BATCH, signer, identifier, p2);

deadline = Date.now() + 90000;
while (!received.includes(p2) && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1000));
}
sub.cancel();

if (received.includes(p1) && received.includes(p2)) {
  console.log('\n✅ TWO-UPDATE PASS — same SOC address delivered twice (dedup fix holds live)');
  process.exit(0);
} else {
  console.log('\n❌ REGRESSION — update #2 was not delivered (received:', received, ')');
  process.exit(2);
}
