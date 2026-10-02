import { createRequire } from 'node:module';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { generateKeyPairSync, ECDH, sign, randomUUID } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
export const root = fileURLToPath(new URL('../', import.meta.url));
export const nodeRepo = process.env.HOLLOW_NODE_REPO || path.resolve(root, '../hollow-relay-plugin');
export const requireNode = createRequire(path.join(nodeRepo, 'package.json'));
export const { WebSocket } = requireNode('ws');
export const nativeBinary = path.resolve(root, process.env.HOLLOW_NATIVE_BINARY || `target/release/hollow-relay-sidecar${process.platform === 'win32' ? '.exe' : ''}`);
export const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const ownedChildren=new Set();
process.once('exit',()=>{for(const child of ownedChildren){if(child.exitCode===null)child.kill();}});
export function identity() {
  const pair = generateKeyPairSync('ec', { namedCurve: 'secp256k1' });
  const der = pair.publicKey.export({ format: 'der', type: 'spki' });
  return { privateKey: pair.privateKey, publicKey: ECDH.convertKey(der.subarray(-65), 'secp256k1', undefined, undefined, 'compressed').toString('hex') };
}
export function auth(roomId, peerId, key, changes = {}) {
  const createdAt = Date.now();
  const data = { version: 1, action: 'join', roomId, peerId, publicKey: key.publicKey, nonce: randomUUID(), createdAt, expiresAt: createdAt + 60000, ...changes };
  const text = ['hollow-room-auth-v1', data.action, data.roomId, data.peerId, data.publicKey.toLowerCase(), data.nonce, data.createdAt, data.expiresAt].join('\n');
  const signature = sign('sha256', Buffer.from(text), { key: key.privateKey, dsaEncoding: 'ieee-p1363' });
  const order = BigInt('0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141');
  const s = BigInt(`0x${signature.subarray(32).toString('hex')}`);
  if (s > order / 2n) Buffer.from((order - s).toString(16).padStart(64, '0'), 'hex').copy(signature, 32);
  return { ...data, signature: signature.toString('hex') };
}
export function packet(room, sequence = 0, lane = 'game-critical', bytes = 256) {
  const createdAt = Date.now();
  return { protocol: 'hollow-realtime/1', scopeId: room, sessionId: 'benchmark-session', senderId: 'forged', lane, sequence, epoch: 1, createdAt, expiresAt: createdAt + (lane === 'voice' ? 140 : lane === 'game-input' ? 100 : lane === 'game-snapshot' ? 180 : lane === 'bulk' ? 30000 : 5000), payloadBase64: Buffer.alloc(bytes, 1).toString('base64') };
}
export class Client {
  constructor(url) {
    this.frames = []; this.waiters = []; this.socket = new WebSocket(url);
    this.socket.on('error',error=>{this.failure=error;for(const waiter of this.waiters){clearTimeout(waiter.timer);waiter.reject(error);}this.waiters=[];});
    this.socket.on('message', raw => {
      const [kind, body] = JSON.parse(raw.toString());
      if (this.onFrame?.(kind, body)) return;
      const index = this.waiters.findIndex(w => w.kind === kind && w.test(body));
      if (index >= 0) {const waiter=this.waiters.splice(index,1)[0]; clearTimeout(waiter.timer);waiter.resolve(body);}
      else this.frames.push([kind,body]);
    });
  }
  async open() {await this.next('WELCOME');return this;}
  next(kind, test = () => true, timeout = 10000) {
    if(this.failure)return Promise.reject(this.failure);
    const index = this.frames.findIndex(f => f[0] === kind && test(f[1]));
    if(index>=0)return Promise.resolve(this.frames.splice(index,1)[0][1]);
    return new Promise((resolve,reject)=>{
      const waiter={kind,test,resolve,reject,timer:null};waiter.timer=setTimeout(()=>{this.waiters=this.waiters.filter(w=>w!==waiter);reject(new Error(`Timed out: ${kind}`));},timeout);this.waiters.push(waiter);
    });
  }
  send(kind,body={}) {this.socket.send(JSON.stringify([kind,body]));}
  async join(room,peer,key=identity(),metadata=undefined) {const joined=this.next('JOINED');this.send('JOIN',{roomId:room,peerId:peer,metadata,authorization:auth(room,peer,key)});await joined;return key;}
  clear() {this.frames.length=0;}
  close() {this.socket.terminate();for(const w of this.waiters)clearTimeout(w.timer);this.waiters=[];}
}
export async function start(kind, extraEnv={}, extraArgs=[]) {
  const token=randomUUID();
  const args=kind==='rust' ? extraArgs : [...(extraEnv.HOLLOW_NODE_COMPILED ? [] : ['--import',pathToFileURL(path.join(nodeRepo,'node_modules/tsx/dist/loader.mjs')).href]),path.join(root,'scripts/node-runner.mjs')];
  const child=spawn(kind==='rust'?nativeBinary:process.execPath,args,{cwd:root,env:{...process.env,HOLLOW_RELAY_CONTROL_TOKEN:token,HOLLOW_NODE_REPO:nodeRepo,...extraEnv},stdio:['pipe','pipe','pipe'],windowsHide:true});
  ownedChildren.add(child);child.once('exit',()=>ownedChildren.delete(child));
  let errors='';child.stderr.on('data',b=>errors=(errors+b).slice(-8192));
  const info=await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>{child.kill();reject(new Error(`Startup timeout ${kind}: ${errors}`));},30000);
    const lines=createInterface({input:child.stdout});
    child.once('error',error=>{clearTimeout(timer);reject(error)});
    child.once('exit',code=>{clearTimeout(timer);reject(new Error(`Relay exited ${code}: ${errors}`))});
    lines.on('line',line=>{try{const value=JSON.parse(line);if(value.type==='ready'){clearTimeout(timer);lines.close();resolve(value);}}catch{}});
  });
  const exit=new Promise(resolve=>child.once('exit',resolve));
  return {kind,child,info,token,errors:()=>errors,exit,async stop(){if(child.exitCode!==null)return;if(kind==='rust')await fetch(`${info.localHttpUrl}/shutdown`,{method:'POST',headers:{'x-hollow-relay-service-token':token}}).catch(()=>{});else child.stdin.end();await Promise.race([exit,sleep(4000)]);if(child.exitCode===null){child.kill();await exit;}}};
}
