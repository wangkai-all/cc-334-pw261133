// Cross-language trailer round-trip: JS builds the on-disk file exactly as the
// browser does, prints base64; Python side (test_trailer_e2e.sh companion)
// strips it and asserts byte equality.
'use strict';
import { appendManifestTrailer } from '../extension/core.js';

const cases = [
  new TextEncoder().encode('task_id,report\nabc,sales\n# detail,x\n'),
  new TextEncoder().encode('a,b\n1,2\n'),
  new TextEncoder().encode('no-final-newline'),
];
const manifest = { download_id: 'd1', task_id: 't1', sha256: 'deadbeef' };
const out = cases.map((c) => {
  const merged = appendManifestTrailer(c, manifest);
  return Buffer.from(merged).toString('base64');
});
process.stdout.write(JSON.stringify(out));
