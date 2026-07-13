// Diagnostic variant of gsoc_two_updates: after sending update #2,
// also retrieve the SOC chunk from the NETWORK (cache bypassed) to
// decide which side loses it — if mainnet serves update-2 but the WS
// never delivers it, the receive path is at fault; if mainnet still
// serves update-1, the second push never actually replaced the chunk.
import { Bee, Identifier } from '/home/florian/claude/ant/conformance/beejs/node_modules/@ethersphere/bee-js/dist/mjs/index.js';

const API = 'http://127.0.0.1:1733';
const BATCH = process.env.BATCH;
const OVERLAY = process.env.OVERLAY;
const bee = new Bee(API);

const identifier = Identifier.fromString('ant-gsoc-diag-' + Date.now());
const signer = bee.gsocMine(OVERLAY, identifier, 12);

const received = [];
const sub = bee.gsocSubscribe(signer.publicKey().address(), identifier, {
  onMessage: (msg) => {
    const s = Buffer.from(msg.toUint8Array ? msg.toUint8Array() : msg).toString('utf8');
    console.log('  📨 WS delivered:', s);
    received.push(s);
  },
  onError: (e) => console.log('sub error:', e.message),
  onClose: () => {},
});

await new Promise((r) => setTimeout(r, 12000));

const p1 = 'update-1 @ ' + Date.now();
const r1 = await bee.gsocSend(BATCH, signer, identifier, p1);
const soc = r1.reference.toString();
console.log('update #1 pushed; SOC chunk address =', soc);

let deadline = Date.now() + 90000;
while (!received.includes(p1) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 1000));
if (!received.includes(p1)) { console.log('❌ update #1 not delivered; abort'); process.exit(2); }

const p2 = 'update-2 @ ' + Date.now();
const r2 = await bee.gsocSend(BATCH, signer, identifier, p2);
console.log('update #2 pushed; reference =', r2.reference.toString());

// Poll the network's view of the chunk while waiting for WS delivery.
const socPayload = (buf) => Buffer.from(buf).subarray(32 + 65 + 8).toString('utf8');
deadline = Date.now() + 90000;
let lastNet = null;
while (!received.includes(p2) && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 5000));
  try {
    const resp = await fetch(`${API}/chunks/${soc}`, { headers: { 'swarm-cache': 'false' } });
    if (resp.ok) {
      const body = socPayload(await resp.arrayBuffer());
      if (body !== lastNet) {
        lastNet = body;
        console.log('  🌐 network now serves:', body);
      }
    } else {
      console.log('  🌐 network retrieve failed:', resp.status);
    }
  } catch (e) {
    console.log('  🌐 retrieve error:', e.message);
  }
}
sub.cancel();

const wsGot2 = received.includes(p2);
const netGot2 = lastNet === p2;
console.log(`\nWS delivered update-2: ${wsGot2}; network serves update-2: ${netGot2}`);
if (wsGot2) { console.log('✅ TWO-UPDATE PASS'); process.exit(0); }
if (netGot2) { console.log('❌ RECEIVE-SIDE LOSS — chunk replaced on mainnet but lurker never delivered it'); process.exit(3); }
console.log('❌ SEND/REPLACE-SIDE LOSS — mainnet still serves update-1');
process.exit(4);
