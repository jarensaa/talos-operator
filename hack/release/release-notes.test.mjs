// The release job decides the version and writes the notes with the plugins .releaserc.json
// configures. They are run here the way semantic-release runs them, on pull requests too, so that
// a release tooling bump whose packages do not work together fails its checks instead of the
// release after it is merged.
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import { analyzeCommits } from '@semantic-release/commit-analyzer'
import { generateNotes } from '@semantic-release/release-notes-generator'

const config = JSON.parse(readFileSync(new URL('../../.releaserc.json', import.meta.url), 'utf8'))

function pluginConfig (name) {
  const entry = config.plugins.find((p) => (Array.isArray(p) ? p[0] : p) === name)
  assert.ok(entry, `.releaserc.json configures ${name}`)

  return Array.isArray(entry) ? entry[1] : {}
}

const commits = [
  { hash: 'a'.repeat(40), message: 'feat: say at rest when a machine would not boot its installer' },
  { hash: 'b'.repeat(40), message: 'fix: a rolled-back node whose services do not run is not reported as fine' },
  { hash: 'c'.repeat(40), message: 'docs: describe the support bundle' },
]

const context = {
  cwd: process.cwd(),
  env: {},
  logger: { log () {}, error () {} },
  options: { repositoryUrl: 'https://github.com/jarensaa/talos-operator.git' },
  commits,
  lastRelease: { gitTag: 'v0.1.0', version: '0.1.0' },
  nextRelease: { gitTag: 'v0.2.0', version: '0.2.0' },
}

test('the configured commit analyzer reads a feature as a minor release', async () => {
  assert.equal(await analyzeCommits(pluginConfig('@semantic-release/commit-analyzer'), context), 'minor')
})

test('the configured release notes generator writes the notes of a release', async () => {
  const notes = await generateNotes(pluginConfig('@semantic-release/release-notes-generator'), context)

  assert.match(notes, /0\.2\.0/)
  assert.match(notes, /Features/)
  assert.match(notes, /say at rest when a machine would not boot its installer/)
  assert.match(notes, /Bug Fixes/)
  assert.doesNotMatch(notes, /describe the support bundle/, 'a docs commit is no part of the notes')
})
