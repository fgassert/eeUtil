# CHANGELOG

## v0.2.2 - 2020-09-22

### fixed
- fixed error in recursive setacl of image collections
- fixed bug in gsbucket.download
- fixed bug in setting `bucket_prefix` in eeUtil.init
- silence warning on file_cache for oauth>4.0.0

### changed
- bumped google-cloud-storage@1.31.1 and earthengine-api@0.1.232 dependencies

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
