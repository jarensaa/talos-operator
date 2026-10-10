const releaseTag = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/

// releaseMetadata says what to publish for HEAD. latestTag is the newest release tag reachable
// from HEAD (`git describe`), if any.
export function releaseMetadata(result, tagsAtHead, latestTag = '') {
  const publishedVersion = result?.nextRelease?.version
  if (publishedVersion) return { releaseCreated: true, version: publishedVersion, latest: publishedVersion }

  // A workflow rerun after semantic-release has already tagged this commit must still republish
  // the release's image and chart if the earlier publish failed.
  const matching = tagsAtHead.filter(tag => tag !== 'v0.0.0' && releaseTag.test(tag))
  if (matching.length > 1) throw new Error(`Multiple release tags point at HEAD: ${matching.join(', ')}`)
  // A commit that isn't a release builds on the latest one; its image is labelled with it.
  const latest = latestTag !== 'v0.0.0' && releaseTag.test(latestTag) ? latestTag.slice(1) : ''
  return { releaseCreated: false, version: matching[0]?.slice(1) ?? '', latest }
}
