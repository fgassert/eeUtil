# CHANGELOG

## v0.3.0 - 2021-05-13

### fixed
- Paths starting with `/` are assumed to be `earthengine-public` assets if no project is specified.
- Use module logger level instead of root logger.

### added
- Add `tree` function to recursively list assets in folder
- Add `export`, `exportImage`, and `exportTable` to export assets to cloud storage
- Add `saveImage` and `findOrSaveImage` to export or cache images as assets

### changed
- Update `download` to work for feature collections
- Update `download` to be able to recursively download image collections
- Storage operations can take a bucket as an optional parameter instead of always using the default.
- Storage operations will try to authenticate with default environment credentials the first time they are called.


## v0.2.3 - 2021-02-05

### fixed
- fix recursive move (addresses issue #7).

### added
- move or copy into a directory using the same basename by specifying a path with a trailing `/`.
- upload and ingest now accept `ingest_params` dictionary to pass to `ee.data.startIngestion()`.

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
