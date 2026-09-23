// Pass the package's dist directory as the first argument.
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
const base=resolve(process.argv[2]);
const {SentenceBuffer,ready}=await import(pathToFileURL(`${base}/sentences.js`));
const {Converter,AudioFormat}=await import(pathToFileURL(`${base}/audio.js`));
await ready;
const text='Hello world. This is a test of sentence boundaries. '.repeat(100),pcm=Buffer.alloc(48000);
let start=performance.now();
for(let i=0;i<20;i++){const buffer=new SentenceBuffer(65536);for(let offset=0;offset<text.length;offset+=7)[...buffer.feed(text.slice(offset,offset+7))];[...buffer.feed('',true)];}
const sentence_ms=(performance.now()-start)/20;start=performance.now();
for(let i=0;i<20;i++){const c=new Converter(AudioFormat.MULAW_8000);c.process(pcm,true);}
console.log(JSON.stringify({sentence_ms,audio_ms:(performance.now()-start)/20}));
