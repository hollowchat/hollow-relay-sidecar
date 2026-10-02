import path from 'node:path';
import { pathToFileURL } from 'node:url';
const { RoomRelayServer } = await import(pathToFileURL(process.env.HOLLOW_NODE_COMPILED || path.join(process.env.HOLLOW_NODE_REPO,'src/roomRelayServer.ts')));
const server = new RoomRelayServer({sfu:null});
const info=await server.start();
console.log(JSON.stringify({type:'ready',pid:process.pid,...info}));
process.stdin.resume();
const stop=()=>server.stop().finally(()=>process.exit(0));
process.stdin.once('end',stop);process.once('SIGINT',stop);process.once('SIGTERM',stop);
