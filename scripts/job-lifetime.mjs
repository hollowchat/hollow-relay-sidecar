import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { root, sleep, start } from './harness.mjs';
if(process.platform!=='win32'){console.log('Windows Job Object test only.');process.exit(0);}
await mkdir('.tmp',{recursive:true});const file=path.join(root,'.tmp/lifetime-child.pid'),helper=path.join(root,'.tmp/tunnel-child.exe');
await rm(file,{force:true});
execFileSync('rustc',['tests/fixtures/tunnel-child.rs','-O','-o',helper],{windowsHide:true});
const parent=await start('rust',{HOLLOW_TEST_CHILD_PID_FILE:file},['--tunnel-helper',helper]);
let pid;
try{
 for(let i=0;i<50;i++){try{pid=Number(await readFile(file,'utf8'));break;}catch{await sleep(100);}}
 assert.ok(pid,'The helper must run after job assignment');
 const alive=()=>{try{process.kill(pid,0);return true;}catch{return false;}};
 assert.equal(alive(),true);parent.child.kill();await parent.exit;
 for(let i=0;i<50&&alive();i++)await sleep(100);
 assert.equal(alive(),false,'Killing the sidecar must terminate its job-owned tunnel child');
 await mkdir('results',{recursive:true});await writeFile('results/job-lifetime.json',JSON.stringify({date:new Date().toISOString(),passed:true,spawnSuspended:true,jobAssignedBeforeResume:true,abruptParentTerminationKillsChild:true},null,2));
 console.log('Windows job lifetime passed: abrupt sidecar termination kills its helper.');
}finally{await parent.stop();if(pid){try{process.kill(pid);}catch{}}}
