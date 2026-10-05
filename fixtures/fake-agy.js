#!/usr/bin/env node
'use strict';

const readline = require('readline');
const args = process.argv.slice(2);
const conversationIndex = args.indexOf('--conversation');
const conversation = conversationIndex >= 0 ? args[conversationIndex + 1] : 'fake-' + process.pid;

if (args.includes('models')) {
  process.stdout.write('Gemini Fake Flash\nGemini Fake Pro\n');
  if (args.includes('--warning')) process.stderr.write('Please sign in to view available models. Launch CLI without arguments to sign in.\n');
  if (args.includes('--hang') || args.includes('--linger')) setInterval(() => {}, 1000);
  else process.exit(0);
}
if (args.includes('agents')) {
  process.stdout.write('fake-agent\n');
  process.exit(0);
}

let initialized = false;
const input = readline.createInterface({ input: process.stdin });
input.on('line', (line) => {
  const message = JSON.parse(line);
  if (message.message?.content === 'auth-failure') {
    process.stderr.write('Authentication required\nError: authentication failed or timed out\n');
    process.exit(1);
  }
  if (!initialized) {
    initialized = true;
    process.stdout.write(JSON.stringify({ event: 'init', conversation_id: conversation }) + '\n');
  }
  const content = message.message && message.message.content;
  const delay = String(content).includes('slow') ? 200 : 10;
  setTimeout(() => {
    const result = { conversation_id: conversation, status: 'SUCCESS' };
    if (content === 'output-field') result.output = 'fake:output-field';
    else if (content === 'content-field') result.content = [{ text: 'fake:content-field' }];
    else if (content === 'step-field') {
      process.stdout.write(JSON.stringify({ event: 'step_update', step_update: { step_type: 'agent_response', text: 'fake:step-field' } }) + '\n');
    } else if (content === 'empty-result') result.denied_actions = [{ action: 'command', display_name: 'RunCommand' }];
    else if (content === 'stream-large') {
      process.stdout.write(JSON.stringify({ event: 'step_update', step_update: { step_type: 'agent_response', text: 'x'.repeat(500) } }) + '\n');
    } else if (content === 'trigger-error-event') {
      process.stdout.write(JSON.stringify({ event: 'error', error: { message: 'Quota exceeded from fake agy' } }) + '\n');
      return;
    }
    else result.response = 'fake:' + content;
    process.stdout.write(JSON.stringify({
      event: 'result',
      result,
    }) + '\n');
  }, delay);
});
