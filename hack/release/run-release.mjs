// Runs semantic-release and reports what the publish job builds (docs: CLAUDE.md, Releases).
// Outputs: release_created, version (the release at HEAD, if any) and latest (the newest release
// reachable from HEAD), to stdout and to GITHUB_OUTPUT.
import { appendFileSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import semanticRelease from 'semantic-release'
import { releaseMetadata } from './release-metadata.mjs'

const result = await semanticRelease()
const tagsAtHead = execFileSync('git', ['tag', '--points-at', 'HEAD'], { encoding: 'utf8' }).trim().split('\n')
let latestTag = ''
try {
  latestTag = execFileSync('git', ['describe', '--tags', '--abbrev=0', '--match', 'v[0-9]*.[0-9]*.[0-9]*', 'HEAD'], { encoding: 'utf8' }).trim()
} catch {
  // No release tag yet.
}
const metadata = releaseMetadata(result, tagsAtHead, latestTag)
const output = `release_created=${metadata.releaseCreated}\nversion=${metadata.version}\nlatest=${metadata.latest}\n`
process.stdout.write(output)
if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, output)
