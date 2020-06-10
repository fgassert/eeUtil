# CHANGELOG

## v0.2.1 - 2020-06-10

### added
- upload and ingest now accept zipped shapefiles.

### fixed
- fixed error in recursive remove.
- fixed error in upload.

## v0.2.0 - 2020-05-11

### added
- added recursive copy, move, remove, setacl operations.
- added `download` function to export and download assets via GCS.
- `createFolder` now behaves like `mkdir -p`.
- added `getCWD`, `cd` to get/set working directory.
- added optional `bucket_prefix` parameter to `init()` to set default bucket
folder for staging operations.
- added getTasks

### changed
- Compatible with earthengine cloud api
- No longer compatible with python 2.x
- ingestAsset no longer accepts date parameter
- ingestAsset, uploadAsset, uploadAssets deprecated
