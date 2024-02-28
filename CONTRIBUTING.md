# Contributing

Thank you for contributing to `lading`! We find `lading` very useful for our
work and hope you have as well and are interested in incorporating your changes
in a timely fashion. To help us help you we ask that you follow a handful of
guidelines when crafting your change.

## Guidelines

0. Please send us changes as PRs. We don't have any constraints on branch names
   but do please add descriptive commit messages to your PR. We provide a
   template for your convenience.

0. Please include a changelog entry in `CHANGELOG.md` with all non-trivial changes.
   If a change is trivial, label the PR as `no-changelog` to avoid a CI ding.

0. Do consider tagging reviewers only after CI checks are green.

0. Do consider that the smaller your change the easier it is to review and the
   more likely we are to incorporate it. If you have a big change in mind, let's
   hash it out in an issue first, just so we agree on your idea before you put
   the time into it.

0. Test your changes, both in test code and by running `lading` with your
   changes in place. Preferentially we make use of property tests in this
   project. We can help you write those if you've never done property testing.

0. We require signed commits. Github's
   [documentation](https://docs.github.com/en/authentication/managing-commit-signature-verification/about-commit-signature-verification)
   is a big help at getting this set up.

0. If you add a dependency please be aware that we require licenses to conform
   to the set listed in our
   [cargo-deny](https://github.com/EmbarkStudios/cargo-deny)
   configuration. License check is part of the CI workflow.
