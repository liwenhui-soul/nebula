#!/bin/sh

# Decrypt the file
gpg --quiet --batch --yes --decrypt --passphrase="$SECRET_PASSPHRASE" \
--output ./tests/secrets/nebula.license ./tests/secrets/nebula_test.license.gpg
