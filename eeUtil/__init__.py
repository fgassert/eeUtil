'''
Python wrapper for easier data management on Google Earth Engine.

Files are staged via Google Cloud Storage for upload.
A service account with access to GEE and Storage is required.

See: https://developers.google.com/earth-engine/service_account

```
import eeUtil

# initialize from environment variables
eeUtil.init()

# create image collection
eeUtil.createFolder('mycollection', imageCollection=True)

# upload image to collection
eeUtil.uploadAsset('image.tif', 'mycollection/myasset')
eeUtil.setAcl('mycollection', 'public')
eeUtil.ls('mycollection')

# export from earthengine to storage and download
eeUtil.downloadAsset('mycollection/myasset', 'image.tif')
```
'''

from .eeutil import *