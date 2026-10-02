import assert from 'node:assert/strict';
import { mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { gzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { Client, nativeBinary, nodeRepo, packet, requireNode, root, start } from './harness.mjs';
await mkdir('.tmp',{recursive:true});await mkdir('results',{recursive:true});
const compiled=path.join(root,'.tmp/node-room-relay.cjs');
requireNode('esbuild').buildSync({entryPoints:[path.join(nodeRepo,'src/roomRelayServer.ts')],outfile:compiled,bundle:true,platform:'node',format:'cjs',minify:true});
const fingerprint={nativeSha256:createHash('sha256').update(await readFile(nativeBinary)).digest('hex'),nodeSourceSha256:createHash('sha256').update(await readFile(path.join(nodeRepo,'src/roomRelayServer.ts'))).digest('hex'),rustProtocolSha256:createHash('sha256').update(await readFile(path.join(root,'src/protocol.rs'))).digest('hex')};
const env={HOLLOW_NODE_COMPILED:compiled,HOLLOW_RELAY_MESSAGES_PER_SECOND:'1000000',HOLLOW_RELAY_INGRESS_BYTES_PER_SECOND:'1073741824'};
const rounds=Number(process.env.BENCH_ROUNDS||5),samples=Number(process.env.BENCH_SAMPLES||10000);
function memory(pid){
 if(process.platform!=='win32')return null;
 const text=execFileSync('powershell.exe',['-NoProfile','-NonInteractive','-Command',`$p=Get-Process -Id ${pid}; [pscustomobject]@{rss=$p.WorkingSet64; private=$p.PrivateMemorySize64; cpu=$p.TotalProcessorTime.TotalSeconds}|ConvertTo-Json -Compress`],{encoding:'utf8',windowsHide:true});return JSON.parse(text.trim());
}
function percentile(values,n){const sorted=[...values].sort((a,b)=>a-b);return sorted[Math.min(sorted.length-1,Math.floor(sorted.length*n))];}
async function throughput(server,fanout,total){
 const clients=[];const room=`bench-${fanout}`;
 try{
  for(let i=0;i<=fanout;i++){const client=await new Client(server.info.localWsUrl).open();clients.push(client);await client.join(room,`peer-${i}`);}
  clients.forEach(c=>c.clear());
  const sender=clients[0],recipients=clients.slice(1),latencies=[],started=new Map(),remaining=new Map();
  let next=0,completed=0,delivered=0;let resolveDone,rejectDone;
  const done=new Promise((resolve,reject)=>{resolveDone=resolve;rejectDone=reject});
  const deadline=setTimeout(()=>rejectDone(new Error(`Benchmark timeout: ${completed}/${total}; ${server.errors()}`)),45000);
  function pump(){while(next<total&&started.size<32){const sequence=next++;started.set(sequence,performance.now());remaining.set(sequence,fanout);sender.send('REALTIME',{roomId:room,packet:packet(room,sequence)});}}
  recipients.forEach(client=>{client.onFrame=(kind,body)=>{if(kind!=='REALTIME')return false;const seq=body.packet.sequence;assert.equal(body.packet.senderId,'peer-0');const sent=started.get(seq);if(sent===undefined)return true;delivered++;latencies.push(performance.now()-sent);const left=remaining.get(seq)-1;if(left===0){remaining.delete(seq);started.delete(seq);completed++;if(completed===total)resolveDone();else pump();}else remaining.set(seq,left);return true;};});
  const startTime=performance.now();pump();await done;const elapsed=performance.now()-startTime;
  clearTimeout(deadline);recipients.forEach(c=>c.onFrame=null);
  assert.equal(delivered,total*fanout);
  return {fanout,ingressMessages:total,deliveries:delivered,durationMs:elapsed,deliveriesPerSecond:delivered/(elapsed/1000),latencyP50Ms:percentile(latencies,.5),latencyP95Ms:percentile(latencies,.95),latencyP99Ms:percentile(latencies,.99)};
 }finally{clients.forEach(c=>c.close());}
}
async function ping(server,total=1000){
 const client=await new Client(server.info.localWsUrl).open(),times=[];
 try {for(let i=0;i<total+100;i++){const startTime=performance.now();client.send('PING');await client.next('PONG');if(i>=100)times.push(performance.now()-startTime);}return {pingP50Ms:percentile(times,.5),pingP95Ms:percentile(times,.95),pingP99Ms:percentile(times,.99)};}
 finally{client.close();}
}
const results=[];
for(let round=0;round<rounds;round++)for(const kind of round%2===0?['node','rust']:['rust','node']){
 const startTime=performance.now();const server=await start(kind,env);const startupMs=performance.now()-startTime;
 try{
  const idle=memory(server.child.pid);const rtt=await ping(server);await throughput(server,2,500);
  const before=memory(server.child.pid);const workloads=[];
  for(const fanout of [2,8,32]){workloads.push(await throughput(server,fanout,samples));}
  const after=memory(server.child.pid);const health=await (await fetch(server.info.localHttpUrl+'/health')).json();
  assert.equal(health.realtimeDroppedPackets,0);
  const result={round:round+1,relay:kind,startupMs,idle,...rtt,workloads,after,cpuSeconds:after&&before?after.cpu-before.cpu:null,droppedPackets:health.realtimeDroppedPackets};results.push(result);
  await writeFile('results/benchmark.json',JSON.stringify({environment:{date:new Date().toISOString(),platform:process.platform,arch:process.arch,cpu:os.cpus()[0].model,cpuCount:os.cpus().length,node:process.version,rounds,samples,fingerprint,transport:'loopback WebSocket, signed joins, 256-byte realtime payload, 32-frame pipeline',limits:'same raised ingress limits on both implementations'},results},null,2));
  console.log(JSON.stringify({round:round+1,relay:kind,startupMs,idleMiB:idle?idle.rss/1048576:null,pingP95Ms:rtt.pingP95Ms,fanout:workloads.map(w=>({peers:w.fanout,deliveriesPerSecond:Math.round(w.deliveriesPerSecond),p95Ms:w.latencyP95Ms}))}));
 }finally{await server.stop();}
}
const summarize=kind=>{
 const group=results.filter(r=>r.relay===kind);const median=fn=>percentile(group.map(fn),.5);
 return {relay:kind,startupMs:median(r=>r.startupMs),idleMiB:median(r=>(r.idle?.rss||0)/1048576),loadedMiB:median(r=>(r.after?.rss||0)/1048576),cpuSeconds:median(r=>r.cpuSeconds||0),pingP95Ms:median(r=>r.pingP95Ms),workloads:[2,8,32].map(fanout=>({fanout,deliveriesPerSecond:median(r=>r.workloads.find(w=>w.fanout===fanout).deliveriesPerSecond),latencyP95Ms:median(r=>r.workloads.find(w=>w.fanout===fanout).latencyP95Ms)}))};
};
const binary=await readFile(nativeBinary);const bundle=await stat(compiled);
const summary={results:['node','rust'].map(summarize),sizes:{nativeBytes:binary.length,nativeGzipBytes:gzipSync(binary,{level:9}).length,nodeRelayBundleBytes:bundle.size,nodeRuntimeBytes:(await stat(process.execPath)).size}};
await writeFile('results/summary.json',JSON.stringify(summary,null,2));console.log(JSON.stringify(summary,null,2));
