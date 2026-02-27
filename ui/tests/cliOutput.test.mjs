import test from 'node:test';
import assert from 'node:assert/strict';
import { parseJsonFromMixedOutput } from '../src/cliOutput.js';

test('parses pure JSON output', () => {
  const output = '{"ok":true,"exit_code":0}';
  assert.deepEqual(parseJsonFromMixedOutput(output), { ok: true, exit_code: 0 });
});

test('parses JSON at end of mixed command output', () => {
  const output = [
    '[DEL] C:/project/results/ligand_vina.log',
    '[CSV] Truncated: C:/project/results/summary.csv',
    '{"ok":true,"exit_code":0,"message":"purged"}'
  ].join('\n');
  assert.deepEqual(parseJsonFromMixedOutput(output), {
    ok: true,
    exit_code: 0,
    message: 'purged'
  });
});

test('returns null when no JSON object is present', () => {
  const output = 'Pipeline cleaned successfully';
  assert.equal(parseJsonFromMixedOutput(output), null);
});
