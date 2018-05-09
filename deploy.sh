#!/bin/bash

# Transpile
npm run build
echo "src/* transpiled to Node 6 in build/*"
# Test
npm test
if [[ $? -ne 0 ]] ; then
  echo "Tests failed."
  exit $?
fi
echo 'test complete'

# Upgrade patch version
# npm version patch
# echo 'updated patch version'
#
# # Deploy to NPM
# npm publish --access public
# echo 'publish complete'
