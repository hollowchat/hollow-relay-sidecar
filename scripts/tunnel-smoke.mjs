import assert from 'node:assert/strict';
import { writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { Client, nodeRepo, sleep, start } from './harness.mjs';
const helper=process.env.HOLLOW_TUNNEL_HELPER || path.join(nodeRepo,'bin',process.platform==='win32'?'hollow-relay-tunnel.exe':'hollow-relay-tunnel');
const upstream=process.env.HOLLOW_TEST_UPSTREAM || 'http://127.0.0.1:7447';
const cloudflared=process.env.HOLLOW_TEST_CLOUDFLARED;
const upstreamWs=process.env.HOLLOW_TEST_UPSTREAM_WS;
const server=await start('rust',{},['--label','Native sidecar verification',...(cloudflared?['--cloudflared',cloudflared]:['--tunnel-helper',helper]),'--upstream',upstream,...(upstreamWs?['--upstream-ws',upstreamWs]:[])]);
const clients=[];let status;
try {
 console.log('Native sidecar started; waiting for public tunnel and registration.');
 const deadline=Date.now()+210000;
 let previous='';
 while(Date.now()<deadline){status=await (await fetch(server.info.localHttpUrl+'/service',{headers:{'x-hollow-relay-service-token':server.token},signal:AbortSignal.timeout(5000)})).json();const progress=JSON.stringify({tunnel:status.tunnel,registration:status.registration});if(progress!==previous){console.log(progress);previous=progress;}if(status.tunnel?.running&&status.registration?.registered)break;await sleep(1000);}
 assert.equal(status.tunnel?.running,true,JSON.stringify(status.tunnel)+' '+server.errors());
 const first=await new Client(status.publicWsUrl).open(),second=await new Client(status.publicWsUrl).open();clients.push(first,second);
 await first.join('sidecar-public-smoke','first');await second.join('sidecar-public-smoke','second');
 first.send('BROADCAST',{roomId:'sidecar-public-smoke',payload:'native through real Cloudflare tunnel'});
 assert.equal((await second.next('BROADCAST')).payload,'native through real Cloudflare tunnel');
 assert.equal(status.registration?.registered,true,JSON.stringify(status.registration));
 const catalog=await (await fetch(upstream+'/plugins/hollow-relay/relays')).json();
 assert.ok(catalog.relays.some(relay=>relay.publicWsUrl===status.publicWsUrl),'Upstream catalog must contain the native relay');
 const result={date:new Date().toISOString(),passed:true,publicSignedJoins:true,publicBroadcast:true,upstreamRegistration:true,catalogVerified:true,provider:status.provider,tunnelImplementation:cloudflared?'official-cloudflared':'native-helper'};
 await mkdir('results',{recursive:true});await writeFile(`results/tunnel-smoke-${cloudflared?'cloudflared':'native'}.json`,JSON.stringify(result,null,2));await writeFile('results/tunnel-smoke.json',JSON.stringify(result,null,2));
 console.log('Native relay public tunnel: signed joins, broadcast, federation registration and catalog passed.');
}finally{clients.forEach(c=>c.close());await server.stop();}
