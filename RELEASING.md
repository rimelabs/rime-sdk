# Releases

We publish `rimelabs-sdk` to PyPI, `@rimelabs/sdk` to npm, and
`github.com/rimelabs/rime-sdk/go` from the `go/` directory of this repository.

## Make this repository public and release Go

1. Open the code PR for Go support. Use a title such as `feat: add Go TTS SDK`.
   Wait for **Package checks** to pass.
2. Before changing visibility, confirm that the repository's source, Git history,
   and Actions logs can be public. GitHub exposes these when visibility changes.
3. Open the repository's **Settings > General > Danger Zone > Change repository
   visibility**. Select **Public**, then complete GitHub's confirmation steps.
   An organization owner may need to permit this change.
4. Complete the one-time Actions setting below if it is not already enabled.
   Go needs no registry account, publishing token, or separate repository.
5. Squash and merge the code PR with its `feat:` title. Release Please then opens
   or updates the release PR. Check that it includes Go and that `go/version.txt`
   agrees with the Go entry in `.release-please-manifest.json`.
6. Wait for all release PR checks. Review every package listed in the PR, since
   merging releases all of them. Then merge the release PR.
7. The **Release** workflow creates a tag such as `go/v0.1.0-alpha.2`. Use the
   actual version from the release PR. The Go job tests the tagged source, then
   downloads and builds the public module in a clean temporary project. Check
   that this job passes before announcing the release.
8. In a new Go project, install the released version:

   ```sh
   go mod init example.com/rime-test
   go get github.com/rimelabs/rime-sdk/go@v0.1.0-alpha.2
   ```

   Replace the example version with the released version. Import the SDK with
   `rime "github.com/rimelabs/rime-sdk/go"`. Follow [the Go README](go/README.md)
   to synthesize speech with `RIME_API_KEY`.

Go publication happens when the tag is public. There is no package upload step.
The tag prefix must be `go/v`, because the module is in `go/`. Python and
TypeScript keep their existing tag formats. Release Please creates all tags.
The Go job requests the exact version from `proxy.golang.org` and verifies it
through `sum.golang.org`. It uses no Git credentials or local module replacement.

The checks after tagging cannot prevent publication. The release PR checks must
pass before you merge. Never move or replace a published Go version. For a code
fix, release a new version. For a temporary proxy or visibility failure, use the
`go_tag` recovery input with the existing tag, such as `go/v0.1.0-alpha.2`.

## One-time setup

Complete these settings before merging the first release PR.
The automation setup PR does not itself publish a package.

1. In GitHub, open **Settings > Actions > General > Workflow permissions**.
   Enable **Allow GitHub Actions to create and approve pull requests**.
   The workflow uses `GITHUB_TOKEN`; it does not need a personal token or GitHub App.
2. In the [PyPI project publishing settings](https://pypi.org/manage/project/rimelabs-sdk/settings/publishing/),
   add a GitHub trusted publisher with these values.

   | Field | Value |
   | --- | --- |
   | Owner | `rimelabs` |
   | Repository | `rime-sdk` |
   | Workflow filename | `release-please.yml` |
   | Environment | `pypi` |

3. In the [npm package settings](https://www.npmjs.com/package/@rimelabs/sdk/access),
   add a GitHub Actions trusted publisher with these values.

   | Field | Value |
   | --- | --- |
   | Organization or user | `rimelabs` |
   | Repository | `rime-sdk` |
   | Workflow filename | `release-please.yml` |
   | Environment | `npm` |
   | Allowed actions, if shown | Allow direct publication with `npm publish` |

Both publishing jobs run in `release-please.yml`. Enter only the filename.
The jobs use GitHub-hosted runners and short-lived OIDC credentials.
The workflow does not read `PYPI_TOKEN` or `NPM_TOKEN`.
After a successful release through each trusted publisher, remove its old GitHub
secret and revoke the corresponding registry token.

The visibility change does not change the existing PyPI or npm trusted publisher
identities. npm provenance remains disabled under the current workflow policy.

## Routine release

1. Make changes in a branch. Open a PR with a Conventional Commit title.
   Use **Squash and merge**, and keep that title as the squash commit message.

   | Title | Intent |
   | --- | --- |
   | `fix: handle stream cancellation` | Bug fix |
   | `feat: add voice filtering` | New feature |
   | `feat!: change the stream interface` | Breaking change |
   | `chore: update development tooling` | Maintenance |

2. Wait for package checks and merge the code PR.
3. GitHub runs **Release** on `main`. Release Please opens or updates one release
   PR with the affected packages' versions and changelogs. More code merges update
   this same PR. You can leave it open until you want to publish.
4. Review the release PR. Check the package versions, release notes, and the
   **Package checks** results. Wait for checks on the latest commit. Do not enable
   automatic merging for release PRs.
5. Merge the release PR. The **Release** workflow creates tags and GitHub releases,
   then runs each affected package job. Go publication starts when its tag exists;
   its job checks the public module download.
6. Confirm that the publishing jobs passed. Check the version on
   [PyPI](https://pypi.org/project/rimelabs-sdk/) and
   [npm](https://www.npmjs.com/package/@rimelabs/sdk).
   Test an install of the exact version in a clean project before announcing it.
7. Check that the published-package example checks pass before announcing the
   release. They install the exact released SDK outside the repository workspace.

You do not need to edit package versions or create tags for routine releases.
Go, Python, and Node.js have independent versions. One release PR can include
one or more packages. Merging that PR releases every package listed in it.

Release Please uses changed file paths to select packages. A commit that only
changes shared files such as `conformance/`, `docs/`, or workflows does not by
itself select a package. If a shared change needs a package release, include
an appropriate change inside each affected package with a `fix:` or `feat:` commit.
Maintenance commits alone generally do not start a release. Use `fix:` or `feat:`
for user-visible package changes, and describe breaking changes in the PR body.

## Published example checks

After Python or TypeScript publication succeeds, the release workflow runs
**Check published SDK examples**. It copies examples from the release tag into
a temporary directory outside the workspace, installs the exact published SDK,
and checks Python imports or TypeScript types. Normal CI runs the example tests
against local SDK code. The release check does not edit dependency files or open
an update PR.

If a registry is slow to expose a new package, the install retries for a bounded
period. Rerun a failed job after publication, or run **Check published SDK examples**
from Actions with the language, published version, and matching release tag.
Use the PyPI spelling for Python and the npm spelling for TypeScript. This check
does not publish packages.

## API dependency updates

The SDK uses published `rime-api` and `@rimelabs/api` packages.
[Dependabot](.github/dependabot.yml) checks for new versions each weekday after
its config reaches `main`. It opens separate Python and Node.js update PRs and
updates each package's exact dependency version and lockfile. The config limits
version updates to these two dependencies. Both API packages are excluded from
the cooldown, so a new release is eligible at the next check.

To check immediately, open **Insights > Dependency graph > Dependabot** in GitHub.
For each package manager, open **Recent update jobs** and select **Check for updates**.

Review each update and wait for **Package checks** to pass. Keep the generated
`fix(deps):` title when you squash and merge. Release Please then includes the
affected SDK package in a release PR. Review and merge that release PR to publish.
An API release does not publish an SDK release on its own.

Copybara does not manage these dependency versions. Dependabot reads the package
registries, so no change to the API release workflow is needed.

## Alpha versions

All three packages remain in alpha. The config uses the `prerelease` versioning strategy
and marks GitHub releases as prereleases. Starting from the current versions,
fixes and features advance the alpha number.

| Location | Example next version or tag |
| --- | --- |
| Release Please manifest, all packages | `0.1.0-alpha.2` |
| Go `version.txt` and Git tag | `0.1.0-alpha.2`, `go/v0.1.0-alpha.2` |
| Python package and `uv.lock` | `0.1.0a2` |
| Python Git tag | `python-v0.1.0-alpha.2` |
| Node.js package and Git tag | `0.1.0-alpha.2`, `typescript-v0.1.0-alpha.2` |

Release Please uses SemVer internally. `uv version` normalizes the Python version
and updates `uv.lock` on the release PR before CI runs. The same step updates
the root npm workspace lockfile after a TypeScript version change. The old
`python-v0.1.0a1` tag remains unchanged. Future Python tags use the SemVer spelling.
The bootstrap commit and manifest start tracking after the first published alphas.

All npm releases use the `latest` distribution tag, including alpha releases.
Use `npm install @rimelabs/sdk` to install the current release. The version still
identifies it as alpha. The workflow no longer updates the `next` tag.

PyPI has no equivalent distribution tag to move. A new version appears when
publication succeeds. Use `uv add --prerelease=allow rimelabs-sdk` while the
package remains in alpha.

To leave alpha, make a separate reviewed PR that sets `prerelease` to `false`,
changes `versioning` to `default`, and sets a deliberate `release-as` version
for each package, for example `0.1.0`. Review the generated release PR and update
installation guidance for stable releases. Remove the `release-as` overrides
after that release so later versions advance normally. Do not edit the manifest
to request a release.

## How CI starts

The **Release** workflow merges the latest `main` into an open release PR branch
before updating metadata and starting checks. This includes test and maintenance
commits even when Release Please leaves the release notes unchanged. If the branch
already includes `main`, this step creates no commit. A merge conflict stops
preparation; resolve it on the release branch, then rerun **Release** on `main`.

GitHub suppresses most new workflow runs for changes made by `GITHUB_TOKEN`.
The **Release** workflow therefore starts **Package checks** explicitly on the
release PR branch after the Python metadata update. This uses `workflow_dispatch`,
which GitHub permits with the built-in token.

Publishing jobs run directly in **Release**, using the tags returned by Release
Please. They build the tagged commit. There are no tag-push publishing workflows.
Only runs on `main` can prepare or publish releases.

## Failed runs and recovery

If release PR preparation or its checks fail, fix the cause first. You can run
**Release** from the Actions page on `main` with all recovery fields empty to
retry preparation. This also updates Python metadata and starts release PR checks.

If a publishing job fails, the GitHub tag or release can already exist. Check the
registry before retrying. A GitHub release alone does not prove that upload succeeded.

1. Fix the cause, such as a trusted publisher setting.
2. Open **Actions > Release > Run workflow**, and select `main`.
3. Enter the existing tag in `python_tag`, `typescript_tag`, or `go_tag` for the
   failed package job. Leave other fields empty unless those jobs also failed.
   These inputs skip Release Please. Python and TypeScript publish the selected
   tagged source. Go verifies its existing public release.
4. Check the jobs and registries again.

PyPI retries use `uv publish --check-url` to check existing files. npm rejects a
version that already exists. If npm already has the intended version, verify that
publication instead of trying to replace it. Fix a published package with a new
version. Do not move or delete a published release tag.

If a code change is needed after tagging, publish a new version through a new
release PR. A retry builds the original tagged source, not the current package code.

## References

- [Release Please](https://github.com/googleapis/release-please)
- [Release Please Action and GitHub token behavior](https://github.com/googleapis/release-please-action#github-credentials)
- [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
- [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/)

- [Go module publishing](https://go.dev/doc/modules/publishing)
- [Go module tags for subdirectories](https://go.dev/ref/mod#vcs-version)
- [GitHub repository visibility](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/managing-repository-settings/setting-repository-visibility)
## Rust

Rust versions and changelogs are independent of the other SDKs. Release Please
uses the Rust strategy and `rust-v` tags. Configure a crates.io trusted publisher
for `rimelabs-sdk`, repository `rimelabs/rime-sdk`, workflow `release-please.yml`,
and GitHub environment `crates`, restricted to `main`. The initial upload may
require a scoped crates.io token before trusted publisher setup.

The SDK requires the published `rimelabs-api` 0.3.x package. Dependabot opens
updates for the registry dependency; review and test them before release.

Release checks use `cargo test --locked` and `cargo package --locked` against
the registry dependency. Recovery can
select an existing `rust-v` tag through the workflow's `rust_tag` input.

