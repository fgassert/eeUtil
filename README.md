# eeUtil

Wrapper for easier data management with Earth Engine python sdk

Requires account with access to Google Cloud Storage and Earth Engine.

```
import eeUtil

# initialize from environment variables
eeUtil.init(project='my-project', bucket='my-bucket')

# create image collection
eeUtil.createFolder('mycollection', imageCollection=True)

# upload image to collection
eeUtil.uploadAsset('image.tif', 'mycollection/myasset')
eeUtil.setAcl('mycollection', 'public')
eeUtil.ls('mycollection')

# export image to cloud storage and download
eeUtil.downloadAsset('mycollection/myasset')
```

__Install__

`pip install eeUtil`

__Develop__

```
git clone https://github.com/fgassert/eeUtil.git
cd eeUtil
pip install -e .
```

### Nice things?

- More consistent python bindings
- Adds recursive `copy`, `move`, `remove`, `setAcl`, `createFolder`.
- GEE paths not starting with `/`, `users/` `projects/` are relative to your user root folder (`users/<username>` or `projects/<project-id>/assets`)
- `upload` and `download` stage files via Google Cloud Storage so you don't have
  to.

### Usage

The easiest way to authorize eeUtil is using [service account credentials](https://developers.google.com/earth-engine/service_account). Once you create a service account and download your `credentials.json` set these in your environment.

```
export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

Initalize these credentials by calling `eeUtil.init()`.

```
eeUtil.init()
```

If you don't provide credentials to eeUtil.init(), it defaults to reading from credentials from the environment, and attempts to read credentials as saved by `earthengine authenticate` for Earth Engine and `gcloud auth application-default login` for Google Cloud Storage. 

```
eeUtil.init(service_account=GEE_SERVICE_ACCOUNT, 
            credential_path=GOOGLE_APPLICATION_CREDENTIALS, 
            project=GEE_PROJECT, 
            bucket=GEE_STAGING_BUCKET, 
            credential_json=GEE_JSON)
```

 - `service_account` Service account name. For more information on GEE service accounts, see: https://developers.google.com/earth-engine/service_account `[default: GEE_SERVICE_ACCOUNT]`
 - `credential_path` Path to json file containing private key. This or `credential_json` is required for service accounts. `[default: GOOGLE_APPLICATION_CREDENTIALS]`
 - `project` Project to use for GEE and GCS bucket. `[default: GEE_PROJECT or CLOUDSDK_CORE_PROJECT]`
 - `bucket` Storage bucket for staging assets for ingestion. Will create new bucket if none provided. `[default: GEE_STAGING_BUCKET]`
 - `credential_json` Pass json string as alternative to `credential_path`. `[default: GEE_JSON]`


