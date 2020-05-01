# CHANGELOG

## v0.2.0 - 2020-03-21
### added
- added recursive copy, move, remove, setacl operations.
- added `download` function to export and download assets via GCS.
- `createFolder` now behaves like `mkdir -p`.
- added `getCWD`, `cd` to get/set working directory.
- added optional `bucket_prefix` parameter to `init()` to set default bucket
folder for staging operations. 

### changed
- Compatible with earthengine cloud api
- No longer compatible with python 2.x
- ingestAsset no longer accepts date parameter
- ingestAsset, uploadAsset, uploadAssets deprecated
