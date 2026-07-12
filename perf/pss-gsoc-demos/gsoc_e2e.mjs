// GSOC end-to-end against a live ant node on mainnet, driven by bee-js.
//   receiver: gsocMine into ant's own overlay, gsocSubscribe (WS → ant)
//   sender:   gsocSend (POST /soc → ant) — pushes into the neighborhood
//   expect:   ant's lurker pulls it back from a full node → WS delivers
import { Bee, Identifier, Topic } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API = 'http://127.0.0.1:1733';
const BATCH = process.env.BATCH;                 // 64-hex, no 0x
const OVERLAY = process.env.OVERLAY;             // ant node overlay, 64-hex
const bee = new Bee(API);

const identifier = Identifier.fromString('ant-gsoc-e2e-' + Date.now());
const signer = bee.gsocMine(OVERLAY, identifier, 12);
const address = signer.publicKey().address().toString();
console.log('mined GSOC signer; SOC address =', address);

const payload = 'hello from bee-js GSOC @ ' + new Date().toISOString();
let received = null;

const sub = bee.gsocSubscribe(signer.publicKey().address(), identifier, {
  onMessage: (msg) => {
    received = Buffer.from(msg.toUint8Array ? msg.toUint8Array() : msg).toString('utf8');
    console.log('LURKER DELIVERED:', received);
  },
  onError: (e) => console.log('sub error:', e.message),
  onClose: () => console.log('sub closed'),
});

// Give the subscription a moment to open the WS + start the lurker.
await new Promise((r) => setTimeout(r, 12000));

console.log('sending GSOC update…');
await bee.gsocSend(BATCH, signer, identifier, payload);
console.log('sent; waiting up to 60s for the lurker to pull it back…');

const deadline = Date.now() + 60000;
while (!received && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1000));
}
sub.cancel();

if (received === payload) {
  console.log('\n✅ E2E PASS — bee-js sent, ant lurker received the exact payload');
  process.exit(0);
} else if (received) {
  console.log('\n⚠️  received a message but payload mismatch:', received);
  process.exit(1);
} else {
  console.log('\n❌ E2E TIMEOUT — no message delivered within 60s');
  process.exit(2);
}
