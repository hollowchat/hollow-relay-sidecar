import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { auth, Client, identity, packet, sleep, start } from './harness.mjs';
const results=[];
async function suite(kind){
  const server=await start(kind);const clients=[];
  const open=async()=>{const c=await new Client(server.info.localWsUrl).open();clients.push(c);return c;};
  async function check(name,fn){await fn();results.push({relay:kind,name,passed:true});console.log(`${kind}: ${name}`);}
  try {
    const health=await (await fetch(server.info.localHttpUrl+'/health')).json();assert.equal(health.signedJoinsRequired,true);
    const a=await open(),b=await open(),key=identity();
    await check('signed joins, tampering, expiration and replay',async()=>{
      a.send('JOIN',{roomId:'auth',peerId:'a'});assert.match((await a.next('ERROR')).message,/authorization/i);
      const invalid=auth('auth','a',key);invalid.signature='00'.repeat(64);a.send('JOIN',{roomId:'auth',peerId:'a',authorization:invalid});assert.match((await a.next('ERROR')).message,/verification/i);
      const expired=auth('auth','a',key,{createdAt:Date.now()-20000,expiresAt:Date.now()-10000});a.send('JOIN',{roomId:'auth',peerId:'a',authorization:expired});assert.match((await a.next('ERROR')).message,/expired/i);
      const valid=auth('auth','a',key);a.send('JOIN',{roomId:'auth',peerId:'a',authorization:valid});assert.equal((await a.next('JOINED')).identityPublicKey,key.publicKey);
      b.send('JOIN',{roomId:'auth',peerId:'a',authorization:valid});assert.match((await b.next('ERROR')).message,/replay/i);
      const high=auth('auth','a',key);const order=BigInt('0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141');high.signature=high.signature.slice(0,64)+(order-BigInt('0x'+high.signature.slice(64))).toString(16).padStart(64,'0');b.send('JOIN',{roomId:'auth',peerId:'a',authorization:high});assert.match((await b.next('ERROR')).message,/verification/i);
      b.send('JOIN',{roomId:'evil\nroom',peerId:'b',authorization:auth('evil\nroom','b',key)});assert.match((await b.next('ERROR')).message,/invalid/i);
    });
    await check('identity ownership and same-identity reconnect',async()=>{
      b.send('JOIN',{roomId:'auth',peerId:'a',authorization:auth('auth','a',identity())});assert.match((await b.next('ERROR')).message,/owned/i);
      await b.join('auth','a',key);assert.equal((await a.next('DISCONNECTED')).reason,'peer-replaced');
    });
    const c=await open(),d=await open(),e=await open();const room='party:conformance';
    await c.join(room,'c',identity(),{position:{x:0,y:0,z:0}});await d.join(room,'d',identity(),{position:{x:1,y:0,z:0}});await e.join(room,'e',identity(),{position:{x:100,y:0,z:0}});
    c.clear();d.clear();e.clear();
    await check('direct, broadcast, room listing and isolation',async()=>{
      c.send('DIRECT',{roomId:room,toPeerId:'d',payload:{hello:1}});assert.equal((await d.next('DIRECT')).fromPeerId,'c');
      c.send('BROADCAST',{roomId:room,includeSelf:true,payload:'broadcast'});for(const client of [c,d,e])assert.equal((await client.next('BROADCAST')).payload,'broadcast');
      c.send('ROOMS');assert.equal((await c.next('ROOMS')).rooms[0].peers.length,3);
      b.send('DIRECT',{roomId:room,toPeerId:'d'});assert.match((await b.next('ERROR')).message,/Not joined/i);
      c.send('DIRECT',{roomId:room,toPeerId:'nobody'});assert.match((await c.next('ERROR')).message,/not in room/i);
    });
    await check('realtime lanes, identity binding, recipients and position updates',async()=>{
      for(const lane of ['voice','game-input','game-snapshot','game-critical','bulk']){c.send('REALTIME',{roomId:room,packet:packet(room,1,lane),destinationPeerIds:['d']});const received=await d.next('REALTIME');assert.equal(received.packet.senderId,'c');assert.equal(received.fromPeerId,'c');assert.equal(received.packet.lane,lane);}
      c.send('REALTIME',{roomId:room,packet:packet(room,10),radius:5});assert.equal((await d.next('REALTIME')).packet.sequence,10);await sleep(40);assert.equal(e.frames.filter(f=>f[0]==='REALTIME').length,0);
      e.send('UPDATE',{roomId:room,metadata:{position:{x:2,y:0,z:0}}});e.send('PING');await e.next('PONG');
      c.send('REALTIME',{roomId:room,packet:packet(room,11),radius:5});await d.next('REALTIME');assert.equal((await e.next('REALTIME')).packet.sequence,11);
      c.send('REALTIME',{roomId:room,packet:packet(room,12),maxRecipients:1});await d.next('REALTIME');await sleep(40);assert.equal(e.frames.filter(f=>f[0]==='REALTIME').length,0);
    });
    await check('rejects wrong scope, stale packets and lane overflow',async()=>{
      for(const bad of [{...packet(room),scopeId:'another'},{...packet(room),expiresAt:Date.now()-1},{...packet(room),sequence:0.5},packet(room,0,'voice',901)]){c.send('REALTIME',{roomId:room,packet:bad});await c.next('ERROR');}
      c.send('JOIN',{roomId:'metadata',peerId:'c',metadata:{value:'x'.repeat(9000)},authorization:auth('metadata','c',identity())});assert.match((await c.next('ERROR')).message,/metadata/i);
    });
    await check('leave and connection cleanup',async()=>{
      e.send('LEAVE',{roomId:room});assert.equal((await d.next('PEER_LEFT')).peerId,'e');e.close();d.clear();c.close();assert.equal((await d.next('PEER_LEFT')).peerId,'c');
    });
    if(kind==='rust')await check('control authentication and shutdown',async()=>{
      assert.equal((await fetch(server.info.localHttpUrl+'/service')).status,401);
      assert.equal((await fetch(server.info.localHttpUrl+'/shutdown',{method:'POST'})).status,401);
      const status=await (await fetch(server.info.localHttpUrl+'/service',{headers:{'x-hollow-relay-service-token':server.token}})).json();assert.equal(status.tunnel,null);
    });
  }finally{clients.forEach(c=>c.close());await server.stop();}
  const limited=await start(kind,{HOLLOW_RELAY_MAX_MEMBERS_PER_ROOM:'2',HOLLOW_RELAY_MESSAGES_PER_SECOND:'3'});const sockets=[];
  try{
    for(let i=0;i<3;i++){const c=await new Client(limited.info.localWsUrl).open();sockets.push(c);if(i<2)await c.join('limited','peer'+i);else{c.send('JOIN',{roomId:'limited',peerId:'peer'+i,authorization:auth('limited','peer'+i,identity())});assert.match((await c.next('ERROR')).message,/capacity/i);}}
    sockets[0].send('PING');await sockets[0].next('PONG');sockets[0].send('PING');await sockets[0].next('PONG');sockets[0].send('PING');assert.match((await sockets[0].next('ERROR')).message,/rate limit/i);
    results.push({relay:kind,name:'room capacity and rate limits',passed:true});
  }finally{sockets.forEach(c=>c.close());await limited.stop();}
}
for(const kind of ['node','rust'])await suite(kind);
const owned=await start('rust',{},['--exit-on-stdin-close']);owned.child.stdin.end();await Promise.race([owned.exit,sleep(4000).then(()=>{throw new Error('Parent pipe EOF did not stop sidecar')})]);results.push({relay:'rust',name:'parent pipe lifetime',passed:true});
const controlled=await start('rust',{},['--exit-on-stdin-close']);const controlledExit=controlled.exit;await controlled.stop();assert.equal(await controlledExit,0,'Authenticated shutdown must exit cleanly even with the parent stdin pipe open');results.push({relay:'rust',name:'authenticated stop with parent pipe open',passed:true});
await mkdir('results',{recursive:true});await writeFile('results/conformance.json',JSON.stringify({date:new Date().toISOString(),results},null,2));console.log(`Passed ${results.length} checks against real relay processes.`);
