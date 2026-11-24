import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn,execFileSync} from 'node:child_process';
import {once} from 'node:events';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import {fileURLToPath} from 'node:url';

test('Windows 便携运行环境可读取独立空库并提供页面', {skip:process.platform!=='win32',timeout:20000},async t=>{
 const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'mw-trace-smoke-'));
 const sqlite=path.join(root,'runtime','sqlite3.exe'),db=path.join(dir,'fixture.db');
 execFileSync(sqlite,[db,'CREATE TABLE session(id TEXT,parent_id TEXT,title TEXT,agent TEXT,model TEXT,directory TEXT,time_created INTEGER,time_updated INTEGER,time_archived INTEGER,tokens_input INTEGER,tokens_output INTEGER,tokens_reasoning INTEGER,tokens_cache_read INTEGER,tokens_cache_write INTEGER);']);
 const reservation=net.createServer();reservation.listen(0,'127.0.0.1');await once(reservation,'listening');
 const port=reservation.address().port;await new Promise(resolve=>reservation.close(resolve));
 const app=spawn(path.join(root,'runtime','node.exe'),[path.join(root,'server.mjs')],{
  cwd:root,windowsHide:true,stdio:'ignore',
  env:{...process.env,USERPROFILE:dir,MOBILEWORK_HOME:dir,MOBILEWORK_OPENCODE_DB:db,MOBILEWORK_SQLITE:sqlite,MOBILEWORK_TRACE_PORT:String(port)}
 });
 t.after(async()=>{
  if(app.exitCode===null){const exited=once(app,'exit');app.kill();await exited;}
  await fs.rm(dir,{recursive:true,force:true,maxRetries:10,retryDelay:100});
 });
 let page;
 for(let i=0;i<50;i++){
  if(app.exitCode!==null)throw Error('独立服务启动失败');
  try{page=await fetch(`http://127.0.0.1:${port}/`,{signal:AbortSignal.timeout(500)});if(page.ok)break;}catch{}
  await new Promise(resolve=>setTimeout(resolve,100));
 }
 assert.equal(page?.status,200);
 assert.match(await page.text(),/质量团队对话TRACE追踪工具/);
 const sessions=await(await fetch(`http://127.0.0.1:${port}/api/sessions`)).json();
 assert.equal(sessions.ok,true);assert.equal(sessions.version,'v0.0.35');assert.deepEqual(sessions.sessions,[]);
});
