# certs/

Put the Barclays root CA here as `CARoot.pem` before building the image. The Schema Registry's
certificate chains to it, so in SECURE mode the connector can't fetch a schema ID without it.

This folder is in the repository, but only this README is tracked. Git ignores everything else
here (`.gitignore`), so a certificate dropped here is never committed.

At build time, [`Docker/Dockerfile`](../Docker/Dockerfile) copies every `*.pem` and `*.crt` in
this folder into `/etc/pki/ca-trust/source/anchors/` and runs `update-ca-trust extract`. The
result is `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`, the file that
`schema_registry.ca_location` and `cyberark.ca_bundle_path` point at.

If this folder holds no certificate, the build still succeeds but logs a warning. The image then
trusts only the base image's CAs. A build from a fresh clone, as in CI, is always in that
state.
