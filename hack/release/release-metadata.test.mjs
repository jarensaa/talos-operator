import assert from 'node:assert/strict'
import { test } from 'node:test'
import { releaseMetadata } from './release-metadata.mjs'

test('new release passes its version to the image build', () => {
  assert.deepEqual(releaseMetadata({ nextRelease: { version: '0.2.0' } }, [], 'v0.1.0'), {
    releaseCreated: true, version: '0.2.0', latest: '0.2.0',
  })
})

test('rerun retains an already created release tag', () => {
  assert.deepEqual(releaseMetadata(false, ['v0.2.0'], 'v0.2.0'), {
    releaseCreated: false, version: '0.2.0', latest: '0.2.0',
  })
})

test('non-release commit and bootstrap marker do not create a release image', () => {
  assert.deepEqual(releaseMetadata(false, ['v0.0.0'], 'v0.0.0'), {
    releaseCreated: false, version: '', latest: '',
  })
  assert.deepEqual(releaseMetadata(false, []), { releaseCreated: false, version: '', latest: '' })
})

test('non-release commit builds on the latest release', () => {
  assert.deepEqual(releaseMetadata(false, [], 'v8.1.0'), {
    releaseCreated: false, version: '', latest: '8.1.0',
  })
})

test('ambiguous tags fail safely', () => {
  assert.throws(() => releaseMetadata(false, ['v0.1.0', 'v0.2.0']), /Multiple release tags/)
})
