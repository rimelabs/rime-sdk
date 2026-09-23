// Resolve from the isolated install directory, never from the source checkout.
import assert from 'node:assert/strict';
import {Rime,AudioFormat,RimeCancelledError} from '@rimelabs/sdk';
const {native} = await import(new URL('./native.js', import.meta.resolve('@rimelabs/sdk')));
assert.deepEqual(new native.SentenceBuffer(65536).feed('Hello. World.', true), ['Hello.', ' World.']);
const client=new Rime({apiKey:'package-test'});
assert.equal(typeof client.native.constructor.testing, 'undefined', 'test hooks leaked into release');
const stream=client.tts.stream('Hello.');
assert.equal(stream.format,AudioFormat.PCM_24000);
await stream.cancel();
await assert.rejects(stream.next(),RimeCancelledError);
await client.close();
