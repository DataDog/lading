# Frozen 1.x reference reader

This private Cargo workspace tests the capture contract with the baseline Arrow and Parquet libraries. Its resolver and lockfile stay separate from the writer, so compatible dependency updates do not force a reader update.

`ci/test` builds the native executable before it runs the test suite. The executable reads a temporary Parquet file and returns the fixed test record view as JSON. Floating-point values use their bit patterns in that test transport to preserve non-finite values and negative zero.

Keep the reference source, dependency pins, lockfile, and reader settings frozen. Do not change them to make a same-major writer change pass.

The JSON transport and record model are test support, not part of the capture format.
