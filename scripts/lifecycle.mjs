import assert from 'node:assert/strict';
import path from 'node:path';
import { writeFile, mkdir } from 'node:fs/promises';
import { start, Client, packet, sleep } from './harness.mjs';

const helper = process.env.HOLLOW_TUNNEL_HELPER;
assert.ok(helper, 'Set HOLLOW_TUNNEL_HELPER to the bundled tunnel executable');
const host = await start('rust', {}, ['--start-paused', '--exit-on-stdin-close', '--tunnel-helper', path.resolve(helper), '--upstream', 'https://relay.hollow.to']);
const headers = { 'x-hollow-relay-service-token': host.token, 'content-type': 'application/json' };
const status = async () => (await fetch(`${host.info.localHttpUrl}/service`, { headers })).json();
const control = async (route, body = {}) => {
 const response = await fetch(`${host.info.localHttpUrl}/${route}`, { method:'POST', headers, body:JSON.stringify(body) });
 assert.ok(response.ok); return response.json();
};
async function waitFor(predicate, label, timeout=90000) {
 const deadline = Date.now()+timeout;
 while(Date.now()<deadline) { const value=await status(); if(predicate(value))return value; await sleep(150); }
 throw new Error(`Timeout: ${label}`);
}
const catalog = async () => (await (await fetch('https://relay.hollow.to/plugins/hollow-relay/relays')).json()).relays;
async function publicPackets(url) {
 const a=await new Client(url).open(), b=await new Client(url).open();
 try {
  const room=`relay-lifecycle-${Date.now()}`;
  await a.join(room,'first'); await b.join(room,'second');
  a.send('BROADCAST',{roomId:room,payload:'reused tunnel'});
  assert.equal((await b.next('BROADCAST')).payload,'reused tunnel');
  a.send('REALTIME',{roomId:room,packet:packet(room)});
  assert.equal((await b.next('REALTIME')).packet.lane,'game-critical');
 } finally { a.close();b.close(); }
}
try {
 // Flicking off while the first tunnel is still connecting must not cancel it.
 await control('enabled',{enabled:true});
 await control('enabled',{enabled:false});
 const first=await waitFor(s=>s.tunnel?.running,'first paused tunnel');
 const url=first.publicWsUrl, created=first.tunnel.startedAt, pid=first.pid;
 assert.equal(first.enabled,false);assert.equal(first.registration.registered,false);
 assert.equal((await fetch(`${host.info.localHttpUrl}/enabled`,{method:'POST',headers:{'content-type':'application/json'},body:'{"enabled":true}'})).status,401);
 const paused=new Client(url);
 await paused.next('HOLLOW_RELAY_PAUSED'); paused.close();
 assert.ok(!(await catalog()).some(r=>r.publicWsUrl===url));
 for(let i=0;i<3;i++) {
  await control('enabled',{enabled:true});
  const online=await waitFor(s=>s.registration.registered,'catalog rejoin');
  assert.equal(online.publicWsUrl,url);assert.equal(online.tunnel.startedAt,created);assert.equal(online.pid,pid);
  if(i===0) await publicPackets(url);
  const active=await new Client(url).open();
  const closed=new Promise(resolve=>active.socket.once('close',resolve));
  await control('enabled',{enabled:false});
  await Promise.race([closed,sleep(3000).then(()=>{throw new Error('Paused relay kept a client connected');})]);active.close();
  const off=await status();assert.equal(off.enabled,false);assert.equal(off.tunnel.running,true);assert.equal(off.publicWsUrl,url);
 }
 const retirementDeadline=Date.now()+10000;
 while((await catalog()).some(r=>r.publicWsUrl===url)&&Date.now()<retirementDeadline)await sleep(200);
 assert.ok(!(await catalog()).some(r=>r.publicWsUrl===url),'Paused relay is removed from discovery');
 const reset=await control('refresh');assert.equal(reset.enabled,false);assert.equal(reset.tunnel.running,false);
 const second=await waitFor(s=>s.tunnel.running&&s.publicWsUrl!==url,'hard-reset replacement');
 assert.equal(second.pid,pid);assert.equal(second.enabled,false);assert.equal(second.registration.registered,false);
 const newUrl=second.publicWsUrl;
 await control('enabled',{enabled:true});await waitFor(s=>s.registration.registered,'replacement registration');await publicPackets(newUrl);
 // Reset during startup must cancel that attempt rather than wait its 180s timeout.
 await control('refresh');await sleep(100);await control('refresh');
 const third=await waitFor(s=>s.tunnel.running&&s.registration.registered&&s.publicWsUrl!==newUrl,'reset during connecting');
 await publicPackets(third.publicWsUrl);
 const result={date:new Date().toISOString(),passed:true,togglesRetainTunnel:true,pauseDuringStartupRetainsTunnel:true,pausedPublicClientsRejected:true,pauseClosesExistingClients:true,pausedCatalogEntryRemoved:true,hardResetChangesUrl:true,hardResetPreservesEnabledState:true,resetDuringConnectingWorks:true,publicSignedJoinsBroadcastRealtime:true,firstUrl:url,resetUrl:newUrl,finalUrl:third.publicWsUrl};
 const output=process.env.HOLLOW_LIFECYCLE_REPORT||path.resolve('results/lifecycle.json');
 await mkdir(path.dirname(output),{recursive:true});await writeFile(output,JSON.stringify(result,null,2)+'\n');
 console.log(JSON.stringify(result,null,2));
} finally { await host.stop(); }
