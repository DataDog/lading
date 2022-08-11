# Sheepdog

Sheepdog is the integration testing harness for `lading`. It's implemented using
plain async rust tests that launch a `ducks` process and a `lading` process and
assert on measurements of `lading's` operation.