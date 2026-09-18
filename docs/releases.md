# Independent releases

Python and Node.js have separate versions, changelogs, and release workflows.
A release does not change the other package's version. No packages have been
published as part of this local implementation.

For Python, update `python/pyproject.toml`, `python/CHANGELOG.md`, and `python/uv.lock`
with `uv lock`. For Node.js, update `typescript/package.json`,
`typescript/package-lock.json`, and `typescript/CHANGELOG.md`.
Run the package checks before tagging.

The Python workflow accepts `python-v<version>` tags, such as `python-v0.1.0a1`.
The Node.js workflow accepts `typescript-v<version>` tags, such as
`typescript-v0.1.0-alpha.1`. Both verify the tag against the package version,
run tests, and build before publishing. Node.js prereleases use the `next` dist-tag.

Configure a GitHub `pypi` environment with a `PYPI_TOKEN` secret and an `npm`
environment with an `NPM_TOKEN` secret before release. Use environment approval
rules for registry publication. These workflows have not run on GitHub yet.
Confirm the Themis deployment contract and complete the live checks in
[validation](validation.md) before the first public release.

The checks workflow selects the language for package-only changes. Shared
conformance, documentation, and workflow changes run both languages. The CI matrix
covers Python 3.11 and 3.13, and Node.js 22 and 24. Local results do not establish
that every CI platform has passed.
