# Releases

We publish `rimelabs-sdk` to PyPI and `@rimelabs/sdk` to npm.
The source repository stays private. The packages are public.

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

npm Trusted Publishing works with our private repository. npm provenance does
not support private source repositories, so this workflow disables provenance.

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
   then checks, builds, and publishes each affected package.
6. Confirm that the publishing jobs passed. Check the version on
   [PyPI](https://pypi.org/project/rimelabs-sdk/) and
   [npm](https://www.npmjs.com/package/@rimelabs/sdk).
   Test an install of the exact version in a clean project before announcing it.

You do not need to edit package versions or create tags for routine releases.
Python and Node.js have independent versions. One release PR can include either
package or both. Merging that PR releases every package listed in it.

Release Please uses changed file paths to select packages. A commit that only
changes shared files such as `conformance/`, `docs/`, or workflows does not by
itself select either package. If a shared change needs a package release, include
an appropriate change inside each affected package with a `fix:` or `feat:` commit.
Maintenance commits alone generally do not start a release. Use `fix:` or `feat:`
for user-visible package changes, and describe breaking changes in the PR body.

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

Both packages remain in alpha. The config uses the `prerelease` versioning strategy
and marks GitHub releases as prereleases. Starting from the current versions,
fixes and features advance the alpha number.

| Location | Example next version or tag |
| --- | --- |
| Release Please manifest, both packages | `0.1.0-alpha.2` |
| Python package and `uv.lock` | `0.1.0a2` |
| Python Git tag | `python-v0.1.0-alpha.2` |
| Node.js package and Git tag | `0.1.0-alpha.2`, `typescript-v0.1.0-alpha.2` |

Release Please uses SemVer internally. `uv version` normalizes the Python version
and updates `uv.lock` on the release PR before CI runs. The old
`python-v0.1.0a1` tag remains unchanged. Future Python tags use the SemVer spelling.
The bootstrap commit and manifest start tracking after the first published alphas.

npm alpha releases use the `next` distribution tag. Stable releases use `latest`.
The initial npm publication also assigned `latest` to alpha.1; this workflow does
not move `latest` when it publishes another alpha. Use `@rimelabs/sdk@next` for
the current alpha.

To leave alpha, make a separate reviewed PR that sets `prerelease` to `false`,
changes `versioning` to `default`, and sets a deliberate `release-as` version
for each package, for example `0.1.0`. Review the generated release PR and update
installation guidance for stable releases. Remove the `release-as` overrides
after that release so later versions advance normally. Do not edit the manifest
to request a release.

## How CI starts

GitHub suppresses most new workflow runs for changes made by `GITHUB_TOKEN`.
The **Release** workflow therefore starts **Package checks** explicitly on the
release PR branch after the Python metadata update. This uses `workflow_dispatch`,
which GitHub permits with the built-in token.

Publishing jobs run directly in **Release**, using the tags returned by Release
Please. They build the tagged commit. There are no tag-push publishing workflows.
Only runs on `main` can prepare or publish releases.

## Failed runs and recovery

If release PR preparation or its checks fail, fix the cause first. You can run
**Release** from the Actions page on `main` with both recovery fields empty to
retry preparation. This also updates Python metadata and starts release PR checks.

If a publishing job fails, the GitHub tag or release can already exist. Check the
registry before retrying. A GitHub release alone does not prove that upload succeeded.

1. Fix the cause, such as a trusted publisher setting.
2. Open **Actions > Release > Run workflow**, and select `main`.
3. Enter the existing tag in `python_tag` or `typescript_tag` for the package that
   still needs publication. Leave the other field empty unless both failed.
   These inputs skip Release Please and publish the selected tagged source.
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
