# Release guidance

Follow [RELEASING.md](RELEASING.md). Use Conventional Commit PR titles: `feat:`
for features, `fix:` for fixes, and `!` for breaking changes. Preserve the title
when squash merging. A breaking change's footer appears in the changelog of every
package the commit touches. If only some SDKs break, say which in the footer's
first words, or split them into their own PR (see RELEASING.md).
