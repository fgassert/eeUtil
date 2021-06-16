import os
import ee
import logging
import time
import datetime
import json
import math
import warnings

from . import gsbucket

STRICT = True

GEE_JSON = os.getenv("GEE_JSON")
GEE_SERVICE_ACCOUNT = os.getenv("GEE_SERVICE_ACCOUNT") or "service account"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GEE_PROJECT = os.getenv("GEE_PROJECT") or os.getenv("CLOUDSDK_CORE_PROJECT")
GEE_STAGING_BUCKET = os.getenv("GEE_STAGING_BUCKET")
GEE_STAGING_BUCKET_PREFIX = os.getenv("GEE_STAGING_BUCKET_PREFIX")

FOLDER_TYPES = (ee.data.ASSET_TYPE_FOLDER, ee.data.ASSET_TYPE_FOLDER_CLOUD)
IMAGE_COLLECTION_TYPES = (ee.data.ASSET_TYPE_IMAGE_COLL, ee.data.ASSET_TYPE_IMAGE_COLL_CLOUD)
IMAGE_TYPES = ('Image', 'IMAGE')
TABLE_TYPES = ('Table', 'TABLE')

# Unary GEE home directory
_cwd = ''
_gs_bucket_prefix = ''


logger = logging.getLogger(__name__)


#######################
# 0. Config functions #
#######################

def init(service_account=GEE_SERVICE_ACCOUNT,
         credential_path=GOOGLE_APPLICATION_CREDENTIALS,
         project=GEE_PROJECT, bucket=GEE_STAGING_BUCKET,
         bucket_prefix=GEE_STAGING_BUCKET_PREFIX,
         credential_json=GEE_JSON):
    '''
    Initialize Earth Engine and Google Storage bucket connection.

    Defaults to read from environment.

    If no service_account is provided, will attempt to use credentials saved by
    `earthengine authenticate`, and `gcloud auth application-default login`
    utilities.

    `service_account` Service account name. Will need access to both GEE and
                      Storage
    `credential_path` Path to json file containing private key
    `project`         GCP project for earthengine and storage bucket
    `bucket`          Storage bucket for staging assets for ingestion
    `bucket_prefix`   Default bucket folder for staging operations
    `credential_json` Json-string to use instead of `credential_path`

    https://developers.google.com/earth-engine/service_account
    '''
    global _gs_bucket_prefix
    init_opts = {}
    if credential_json:
        init_opts['credentials'] = ee.ServiceAccountCredentials(service_account, key_data=credential_json)
    elif credential_path:
        init_opts['credentials'] = ee.ServiceAccountCredentials(service_account, key_file=credential_path)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    if project:
        init_opts['project'] = project
    ee.Initialize(**init_opts)
    try:
        gsbucket.init(bucket, **init_opts)
    except Exception as e:
        logger.warning("Could not authenticate Google Cloud Storage Bucket. Upload and download functions will not work.")
        logger.error(e)
    if bucket_prefix:
        _gs_bucket_prefix = bucket_prefix


def initJson(credential_json=GEE_JSON, project=GEE_PROJECT,
             bucket=GEE_STAGING_BUCKET):
    '''
    Writes json string to credential file and initializes

    Defaults from GEE_JSON env variable
    '''
    init('service_account', None, project, bucket, credential_json)


def setBucketPrefix(prefix=''):
    '''Set the default prefix to be used for storage bucket operations'''
    global _gs_bucket_prefix
    _gs_bucket_prefix = prefix


########################
# 1. Utility functions #
########################

def formatDate(date):
    '''Format date as ms since last epoch'''
    if isinstance(date, int):
        return date
    seconds = (date - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return int(seconds * 1000)


#################################
# 2. Asset management functions #
#################################

def getHome():
    '''Get user root directory'''
    project = ee._cloud_api_utils._cloud_api_user_project
    if project == ee.data.DEFAULT_CLOUD_API_USER_PROJECT:
        assetRoots = ee.data.getAssetRoots()
        if not len(assetRoots):
            raise Exception(f"No available assets for provided credentials in project {project}")
        return assetRoots[0]['id']
    else:
        return f'projects/{project}/assets/'


def getCWD():
    '''Get current directory or root directory'''
    global _cwd
    if not _cwd:
        _cwd = getHome()
    return _cwd


def cd(path):
    '''Change CWD'''
    global _cwd
    path = os.path.normpath(_path(path))
    if isFolder(path):
        _cwd = path
    else:
        raise Exception(f"{path} is not a folder")
    return _cwd


def _path(path):
    '''Add cwd to path if not full path'''
    if path:
        abspath = path[0] == '/'
        path = path[1:] if abspath else path

        if len(path) > 6 and path[:6] == 'users/':
            return f'projects/{ee.data.DEFAULT_CLOUD_API_USER_PROJECT}/{path}'
        elif len(path) > 9 and path[:9] == 'projects/':
            return path
        else:
            basepath = 'projects/earthengine-public/assets/' if abspath else getCWD()
            return os.path.join(basepath, path)

    return getCWD()


def getQuota():
    '''Get GEE usage quota'''
    return ee.data.getAssetRootQuota(getHome())


def info(asset=''):
    '''Get asset info'''
    return ee.data.getInfo(_path(asset))


def exists(asset):
    '''Check if asset exists'''
    return True if info(asset) else False


def isFolder(asset, image_collection_ok=True):
    '''Check if path is folder or imageCollection'''
    if ee._cloud_api_utils.is_asset_root(asset):
        return True
    asset_info = info(asset)
    folder_types = FOLDER_TYPES
    if image_collection_ok:
        folder_types += IMAGE_COLLECTION_TYPES
    return asset_info and asset_info['type'] in folder_types


def ls(path='', abspath=False, details=False, pageToken=None):
    '''List assets in path'''
    resp = ee.data.listAssets({'parent': _path(path), 'pageToken':pageToken})
    for a in resp['assets']:
        a['name'] = a['name'] if abspath else os.path.basename(a['name'])
        yield (a if details else a['name'])
    if 'nextPageToken' in resp:
        for a in ls(path, abspath, details, pageToken=resp['nextPageToken']):
            yield a


def _tree(folder, details=False, _basepath=''):
    for item in ls(folder, abspath=True, details=True):
        if item['type'] in FOLDER_TYPES+IMAGE_COLLECTION_TYPES:
            for child in _tree(item['name'], details, _basepath):
                yield child
        if _basepath and item['name'][:len(_basepath)] == _basepath:
            item['name'] = item['name'][len(_basepath):]
        yield (item if details else item['name'])


def tree(folder, abspath=False, details=False):
    '''Recursively list all assets in folder

    Args:
        folder (string): Earth Engine folder or image collection
        relpath (bool): Return the relative path of assets in the folder
        details (bool): Return a dict representation of each asset instead of only the assetId string

    Returns:
        If details is False:
        list: paths to assets

        If details is True:
        list: asset info dicts
    '''
    folder = _path(folder)
    _basepath = '' if abspath else f'{folder.rstrip("/")}/'

    return _tree(folder, details, _basepath)


def getAcl(asset):
    '''Get ACL of asset or folder'''
    return ee.data.getAssetAcl(_path(asset))


def setAcl(asset, acl={}, overwrite=False, recursive=False):
    '''Set ACL of asset

    `acl`       ('public'|'private'| ACL specification )
    `overwrite` If false, only change specified values
    '''
    path = _path(asset)
    if recursive and isFolder(path, image_collection_ok=False):
        children = ls(path, abspath=True)
        for child in children:
            setAcl(child, acl, overwrite, recursive)
    _acl = {} if overwrite else getAcl(path)
    _acl.pop('owners', None)
    if acl == 'public':
        _acl["all_users_can_read"] = True
    elif acl == 'private':
        _acl["all_users_can_read"] = False
    else:
        _acl.update(acl)
    acl = json.dumps(_acl)
    logger.debug('Setting ACL to {} on {}'.format(acl, path))
    ee.data.setAssetAcl(path, acl)


def setProperties(asset, properties={}):
    '''Set asset properties'''
    return ee.data.setAssetProperties(_path(asset), properties)


def createFolder(path, image_collection=False, overwrite=False,
                 public=False):
    '''Create folder or image collection,

    Automatically creates intermediate folders a la `mkdir -p`
    '''
    path = _path(path)
    upper = os.path.split(path)[0]
    if not isFolder(upper):
        createFolder(upper)
    if overwrite or not isFolder(path):
        ftype = (ee.data.ASSET_TYPE_IMAGE_COLL if image_collection
                 else ee.data.ASSET_TYPE_FOLDER)
        logger.debug(f'Created {ftype} {path}')
        ee.data.createAsset({'type': ftype}, path, overwrite)
    if public:
        setAcl(path, 'public')


def createImageCollection(path, overwrite=False, public=False):
    '''Create image collection'''
    createFolder(path, True, overwrite, public)


def copy(src, dest, overwrite=False, recursive=False):
    '''Copy asset'''
    if dest[-1] == '/':
        dest = dest + os.path.basename(src)
    if recursive and isFolder(src):
        is_image_collection = info(src)['type'] in IMAGE_COLLECTION_TYPES
        createFolder(dest, is_image_collection)
        for child in ls(src):
            copy(os.path.join(src, child), os.path.join(dest, child), overwrite, recursive)
    else:
        ee.data.copyAsset(_path(src), _path(dest), overwrite)


def move(src, dest, overwrite=False, recursive=False):
    '''Move asset'''
    if dest[-1] == '/':
        dest = dest + os.path.basename(src)
    src = _path(src)
    copy(src, _path(dest), overwrite, recursive)
    remove(src, recursive)


def remove(asset, recursive=False):
    '''Delete asset from GEE'''
    if recursive and isFolder(asset):
        for child in ls(asset, abspath=True):
            remove(child, recursive)
    logger.debug('Deleting asset {}'.format(asset))
    ee.data.deleteAsset(_path(asset))


################################
# 3. Task management functions #
################################

def getTasks(active=False):
    '''Return a list of all recent tasks

    If active is true, return tasks with status in
    'READY', 'RUNNING', 'UNSUBMITTED'
    '''
    if active:
        return [t for t in ee.data.getTaskList() if t['state'] in (
            ee.batch.Task.State.READY,
            ee.batch.Task.State.RUNNING,
            ee.batch.Task.State.UNSUBMITTED,
        )]
    return ee.data.getTaskList()


def _checkTaskCompleted(task_id):
    '''Return True if task completed else False'''
    status = ee.data.getTaskStatus(task_id)[0]
    if status['state'] in (ee.batch.Task.State.CANCELLED,
                           ee.batch.Task.State.FAILED):
        if 'error_message' in status:
            logger.error(status['error_message'])
        if STRICT:
            raise Exception(f"Task {status['id']} ended with state {status['state']}")
        return True
    elif status['state'] == ee.batch.Task.State.COMPLETED:
        return True
    return False


def waitForTasks(task_ids=[], timeout=3600):
    '''Wait for tasks to complete, fail, or timeout

    Waits for all active tasks if task_ids is not provided

    Note: Tasks will not be canceled after timeout, and
    may continue to run.
    '''
    if not task_ids:
        task_ids = [t['id'] for t in getTasks(active=True)]

    start = time.time()
    elapsed = 0
    while elapsed < timeout or timeout == 0:
        elapsed = time.time() - start
        finished = [_checkTaskCompleted(task) for task in task_ids]
        if all(finished):
            logger.info(f'Tasks {task_ids} completed after {elapsed}s')
            return True
        time.sleep(5)
    logger.warning(f'Stopped waiting for {len(task_ids)} tasks after {timeout} seconds')
    if STRICT:
        raise Exception(f'Stopped waiting for {len(task_ids)} tasks after {timeout} seconds')
    return False


def waitForTask(task_id, timeout=3600):
    '''Wait for task to complete, fail, or timeout'''
    return waitForTasks([task_id], timeout)


#######################
# 4. Import functions #
#######################

def ingestAsset(gs_uri, asset, date=None, wait_timeout=None, bands=[]):
    '''[DEPRECATED] please use eeUtil.ingest instead'''
    warnings.warn('[DEPRECATED] please use eeUtil.ingest instead', DeprecationWarning)
    return ingest(gs_uri, asset, wait_timeout, bands)


def _guessIngestTableType(path):
    if os.path.splitext(path)[-1] in ['.csv', '.zip']:
        return True
    return False


def ingest(gs_uri, asset, wait_timeout=None, bands=[], ingest_params={}):
    '''
    Ingest asset from GS to EE

    `gs_uri`       should be formatted `gs://<bucket>/<blob>`
    `asset`        destination path
    `wait_timeout` if non-zero, wait timeout secs for task completion
    `bands`        optional band name list
    `ingest_params`dict optional additional ingestion params to pass to
                   ee.data.startIngestion() or ee.data.startTableIngestion()
                   'id' and 'sources' are provided by this function
    '''
    asset_id = _path(asset)
    params = ingest_params.copy()
    if _guessIngestTableType(gs_uri):
        params.update({'id': asset_id, 'sources': [{'primaryPath': gs_uri}]})
        request_id = ee.data.newTaskId()[0]
        task_id = ee.data.startTableIngestion(request_id, params, True)['id']
    else:
        # image asset
        params = {'id': asset_id, 'tilesets': [{'sources': [{'primaryPath': gs_uri}]}]}
        if bands:
            if isinstance(bands[0], str):
                bands = [{'id': b} for b in bands]
            params['bands'] = bands
        request_id = ee.data.newTaskId()[0]
        task_id = ee.data.startIngestion(request_id, params, True)['id']
    logger.info(f"Ingesting {gs_uri} to {asset}: {task_id}")
    if wait_timeout is not None:
        waitForTask(task_id, wait_timeout)

    return task_id


def uploadAsset(filename, asset, gs_prefix='', date='', public=False,
                timeout=3600, clean=True, bands=[]):
    '''[DEPRECATED] please use eeUtil.upload instead'''
    warnings.warn('[DEPRECATED] please use eeUtil.upload instead', DeprecationWarning)
    return upload([filename], [asset], gs_prefix, public, timeout, clean, bands)[0]


def uploadAssets(files, assets, gs_prefix='', dates=[], public=False,
                 timeout=3600, clean=True, bands=[]):
    '''[DEPRECATED] please use eeUtil.upload instead'''
    warnings.warn('[DEPRECATED] please use eeUtil.upload instead', DeprecationWarning)
    return upload(files, assets, gs_prefix, public, timeout, clean, bands)


def upload(files, assets, gs_prefix='', public=False,
                 timeout=3600, clean=True, bands=[], ingest_params={}):
    '''Stage files to cloud storage and ingest into Earth Engine

    Currently supports `tif`, `zip` (shapefile), and `csv`

    `files`        local file path or list of paths
    `assets`       destination asset ID or list of asset IDs
    `gs_prefix`    storage bucket folder for staging (else files are staged to bucket root)
    `public`       set acl public after upload if True
    `timeout`      wait timeout secs for completion of GEE ingestion
    `clean`        delete files from storage bucket after completion
    `bands`        optional band names to assign, all assets must have the same number of bands
    `ingest_params`optional additional ingestion params to pass to
                   ee.data.startIngestion() or ee.data.startTableIngestion()
    '''
    if type(files) is str and type(assets) is str:
        files = [files]
        assets = [assets]
    if len(assets) != len(files):
        raise Exception(f"Files and assets must be of same length. Found {len(files)}, {len(assets)}")

    gs_prefix = gs_prefix or _gs_bucket_prefix
    task_ids = []

    gs_uris = gsbucket.stage(files, gs_prefix)
    for i in range(len(files)):
        task_ids.append(ingest(gs_uris[i], assets[i], timeout, bands))

    try:
        waitForTasks(task_ids, timeout)
        if public:
            for asset in assets:
                setAcl(asset, 'public')
    except Exception as e:
        logger.error(e)
    if clean:
        gsbucket.remove(gs_uris)

    return assets


#######################
# 5. Export functions #
#######################

def _getAssetCrs(assetInfo):
    return assetInfo['bands'][0]['crs']


def _getAssetCrsTransform(assetInfo):
    return assetInfo['bands'][0]['crs_transform']


def _getAssetProjection(assetInfo):
    return ee.Projection(_getAssetCrs(assetInfo), _getAssetCrsTransform(assetInfo))


def _getAssetScale(assetInfo):
    return _getAssetProjection(assetInfo).nominalScale()


def _getExportDescription(path):
    desc = path.replace('/', ':')
    return desc[-100:] if len(desc)>100 else desc


def _getAssetBounds(assetInfo):
    coordinates = assetInfo['properties']['system:footprint']['coordinates']
    if coordinates[0][0] in ['-Infinity', 'Infinity']:
        coordinates = [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
    if _getAssetCrs(assetInfo) == 'EPSG:4326':
        return ee.Geometry.LinearRing(
            coords=coordinates,
            proj='EPSG:4326',
            geodesic=False
        )
    return ee.Geometry.LinearRing(coordinates)


def _getAssetBitdepth(assetInfo):
    bands = assetInfo['bands']
    bit_depth = 0
    for band in bands:
        if band['data_type']['precision'] == 'double':
            bit_depth += 64
        elif band['data_type'].get('max'):
            minval = band['data_type'].get('min', 0)
            maxval = band['data_type'].get('max')
            bit_depth += math.log(maxval-minval + 1, 2)
        else:
            bit_depth += 32
    return bit_depth


def _getAssetExportDims(proj, scale, bounds, bit_depth, cloudOptimized=False):
    MAX_EXPORT_BYTES = 2**34 # 17179869184

    proj = ee.Projection(proj) if isinstance(proj, str) else proj
    proj = proj.atScale(scale)
    proj_coords = bounds.bounds(1, proj).coordinates().getInfo()[0]
    topright = proj_coords[2]
    bottomleft = proj_coords[0]
    x = topright[0] - bottomleft[0]
    y = topright[1] - bottomleft[1]
    x = math.ceil(x / 256.0) * 256
    y = math.ceil(y / 256.0) * 256
    byte_depth = bit_depth / 8
    total_bytes = x * y * byte_depth
    if total_bytes > MAX_EXPORT_BYTES:
        depth = int(math.log(MAX_EXPORT_BYTES / byte_depth, 2))
        y = 2 ** (depth // 2)
        x = 2 ** (depth // 2 + depth % 2)
        logger.warning(f'Export size (2^{math.log(total_bytes,2)}) more than 2^{math.log(MAX_EXPORT_BYTES,2)} bytes, dicing to {x}x{y} tiles')

    return x,y


def _getImageExportArgs(image, bucket, fileNamePrefix,
                        description=None, region=None, scale=None, crs=None,
                        maxPixels=1e13, fileDimensions=None, fileFormat='GeoTIFF',
                        cloudOptimized=False, **kwargs):
    assetInfo = ee.Image(image).getInfo()

    description = description or _getExportDescription(f'gs://{bucket}/{fileNamePrefix}')
    scale = scale or _getAssetScale(assetInfo)
    crs = crs or _getAssetProjection(assetInfo)
    region = region or _getAssetBounds(assetInfo)
    fileDimensions = fileDimensions or _getAssetExportDims(crs, scale, region, _getAssetBitdepth(assetInfo), cloudOptimized)

    args = {
        'image': image,
        'description': description,
        'bucket': bucket,
        'fileNamePrefix': fileNamePrefix,
        'region': region,
        'scale': scale,
        'crs': crs,
        'maxPixels': maxPixels,
        'fileDimensions': fileDimensions,
        'fileFormat': fileFormat,
        'formatOptions': {
            'cloudOptimized': cloudOptimized,
        }
    }
    args.update(kwargs)

    return args


def _getImageSaveArgs(image, assetId, description=None, pyramidingPolicy='mean', region=None, scale=None, crs=None, 
                        maxPixels=1e13, **kwargs):
    assetInfo = ee.Image(image).getInfo()

    assetInfo = image.getInfo()
    description = description or _getExportDescription(assetId)
    scale = scale or _getAssetScale(assetInfo)
    crs = crs or _getAssetProjection(assetInfo)
    region = region or _getAssetBounds(assetInfo)
    pyramidingPolicy = {'.default': pyramidingPolicy} if isinstance(pyramidingPolicy, str) else pyramidingPolicy

    args = {
        'image': image,
        'description': description,
        'assetId': assetId,
        'pyramidingPolicy': pyramidingPolicy,
        'region': region,
        'crs': crs,
        'scale': scale,
        'maxPixels': maxPixels,
    }
    args.update(kwargs)

    return args

def _cast(image, dtype):
    '''Cast an image to a data type'''
    return {
        'uint8': image.uint8,
        'uint16': image.uint16,
        'uint32': image.uint32,
        'int8': image.int8,
        'int16': image.int16,
        'int32': image.int32,
        'int64': image.int64,
        'byte': image.byte,
        'short': image.short,
        'int': image.int,
        'long': image.long,
        'float': image.float,
        'double': image.double
    }[dtype]()


def saveImage(image, assetId, dtype=None, pyramidingPolicy='mean', wait_timeout=None, **kwargs):
    '''Export image to asset

    Attempts ot guess export args from image metadata if it exists

    Args:
        image (ee.Image): the Image to export
        assetId (str): the asset path to export to
        dtype (str): Cast to image to dtype before export ['byte'|'int'|'float'|'double'...]
        pyramidingPolicy (str, dict): default or per-band asset pyramiding policy ['mean', 'mode', 'sample', 'max'...]
        wait_timeout (bool): if not None, wait at most timeout secs for export completion
        **kwargs: additional parameters to pass to ee.batch.Export.image.toAsset()

    Returns:
        str: task id
    '''
    path = _path(assetId)
    if dtype:
        image = _cast(image, dtype)
    args = _getImageSaveArgs(image, path, pyramidingPolicy=pyramidingPolicy, **kwargs)

    logger.info(f'Exporting image to {path}')
    task = ee.batch.Export.image.toAsset(**args)
    task.start()
    if wait_timeout is not None:
        waitForTask(task.id, wait_timeout)

    return task.id


def findOrSaveImage(image, assetId, wait_timeout=None, **kwargs):
    '''Export an Image to asset, or return the image asset if it already exists

    Will avoid duplicate exports by checking for existing tasks with matching descriptions.

    Args:
        image (ee.Image): The image to cache
        asset_id (str): The asset path to export to or load from
        wait_timeout (bool): If not None, wait at most timeout secs for export completion
        kwargs: additional export arguments to pass to eeUtil.saveImage()

    Returns:
        ee.Image: the cached image if it exists, or the image that was just exported
    '''
    path = _path(assetId)
    if exists(path):
        logger.debug(f'Asset {os.path.basename(path)} exists, using cached asset.')
        return ee.Image(path)
    description = kwargs.get('description', _getExportDescription(path))
    existing_task = next(filter(lambda t: t['description'] == description, getTasks(active=True)), None)
    if existing_task:
        logger.info(f'Task with description {description} already in progress, skipping export.')
        task_id = existing_task['id']
    else:
        task_id = saveImage(image, path, **kwargs)
    if wait_timeout is not None:
        waitForTask(task_id, wait_timeout)

    return image


def exportImage(image, blob, bucket=None, fileFormat='GeoTIFF', cloudOptimized=False, dtype=None,
                overwrite=False, wait_timeout=None, **kwargs):
    '''Export an Image to cloud storage

    Args:
        image (ee.Image): Image to export
        blob (str): Filename to export to (excluding extention)
        bucket (str): Cloud storage bucket
        fileFormat (str): Export file format ['geotiff'|'tfrecord']
        cloudOptimized (bool): (GeoTIFF only) export as Cloud Optimized GeoTIFF
        dtype (str): Cast to image to dtype before export ['byte'|'int'|'float'|'double'...]
        overwrite (bool): Overwrite existing files
        wait_timeout (int): If non-zero, wait timeout secs for task completion
        **kwargs: Additional parameters to pass to ee.batch.Export.image.toCloudStorage()

    Returns:
        (str, str): taskId, destination uri
    '''
    bucket = gsbucket._defaultBucketName(bucket)

    if dtype:
        image = _cast(image, dtype)

    ext = {'geotiff':'.tif', 'tfrecord':'.tfrecord'}[fileFormat.lower()]
    uri = gsbucket.asURI(blob+ext, bucket)

    exists = gsbucket.getTileBlobs(uri)
    if exists and not overwrite:
        logger.info(f'{len(exists)} blobs matching {blob} exists, skipping export')
        return

    args = _getImageExportArgs(image, bucket, blob, cloudOptimized=cloudOptimized, **kwargs)
    task = ee.batch.Export.image.toCloudStorage(**args)
    task.start()

    logger.info(f'Exporting to {uri}')
    if wait_timeout is not None:
        waitForTask(task.id)

    return task.id, uri



def exportTable(table, blob, bucket=None, fileFormat='GeoJSON',
                overwrite=False, wait_timeout=None, **kwargs):
    '''
    Export FeatureCollection to cloud storage

    Args:
        table (ee.FeatureCollection): FeatureCollection to export
        blob (str): Filename to export to (excluding extention)
        bucket (str): Cloud storage bucket
        fileFormat (str): Export file format ['csv'|'geojson'|'shp'|'tfrecord'|'kml'|'kmz']
        overwrite (bool): Overwrite existing files
        wait_timeout (int): If non-zero, wait timeout secs for task completion
        **kwargs: Additional parameters to pass to ee.batch.Export.image.toCloudStorage()

    Returns:
        (str, str): taskId, destination uri
    '''

    blobname = f'{blob}.{fileFormat.lower()}'
    uri = gsbucket.asURI(blobname, bucket)
    exists = gsbucket.exists(uri)

    if exists and not overwrite:
        logger.info(f'Blob matching {blobname} exists, skipping export')
        return

    args = {
        'collection': table,
        'description': _getExportDescription(uri),
        'bucket': gsbucket._defaultBucketName(bucket),
        'fileFormat': fileFormat,
        'fileNamePrefix': blob
    }
    args.update(kwargs)
    task = ee.batch.Export.table.toCloudStorage(**args)
    task.start()

    logger.info(f'Exporting to {uri}')
    if wait_timeout is not None:
        waitForTask(task.id)

    return task.id, uri


def export(assets, bucket=None, prefix='', recursive=False,
           overwrite=False, wait_timeout=None, cloudOptimized=False, **kwargs):
    '''Export assets to cloud storage

    Exports one or more assets to cloud storage.
    FeatureCollections are exported as GeoJSON.
    Images are exported as GeoTIFF.
    Use `recursive=True` to export all assets in folders or ImageCollections.

    Args:
        assets (str, list): Asset(s) to export
        bucket (str): Google cloud storage bucket name
        prefix (str): Optional folder to export assets to (prepended to asset names)
        recursive (bool): Export all assets in folder or image collection (asset)
        overwrite (bool): Overwrite existing assets
        wait_timeout (int): If not None, wait timeout secs for task completion
        cloudOptimized (bool): Export Images as Cloud Optimized GeoTIFFs
        **kwargs: Additional export arguments passed to ee.batch.Export.{}.toCloudStorage()

    Returns:
        (list, list): TaskIds, URIs

    '''
    prefix = prefix or _gs_bucket_prefix
    assets = (assets,) if isinstance(assets, str) else assets
    paths = [os.path.basename(a) for a in assets]
    infos = [info(a) for a in assets]

    for item in infos[:]:
        if item is None:
            raise Exception('Asset does not exist.')
        if item['type'] in FOLDER_TYPES+IMAGE_COLLECTION_TYPES:
            if recursive:
                folder = f"{item['name']}/"
                for c in tree(item['name'], abspath=True, details=True):
                    infos.append(c)
                    paths.append(c['name'][len(folder):])
            else:
                raise Exception(f"{item['name']} is a folder/ImageCollection. Use recursive=True to export all assets in folder")

    tasks = []
    uris = []
    for item, path in zip(infos, paths):
        blob = os.path.join(prefix, path)
        result = None
        if item['type'] in IMAGE_TYPES:
            image = ee.Image(item['name'])
            result = exportImage(image, blob, bucket, cloudOptimized=cloudOptimized, overwrite=overwrite, **kwargs)
        elif item['type'] in TABLE_TYPES:
            table = ee.FeatureCollection(item['name'])
            result = exportTable(table, blob, bucket, overwrite=overwrite, **kwargs)
        if result:
            task, uri = result
            tasks.append(task)
            uris.append(uri)

    if wait_timeout is not None:
        waitForTasks(tasks)

    return tasks, uris


def download(assets, directory=None, gs_bucket=None, gs_prefix='', clean=True, recursive=False, timeout=3600, **kwargs):
    '''Export image assets to cloud storage, then downloads to local machine

    `asset`     Asset ID or list of asset IDs
    `directory` Optional local directory to save assets to
    `gs_prefix` GS bucket for staging (else default bucket)
    `gs_prefix` GS folder for staging (else files are staged to bucket root)
    `clean`     Remove file from GS after download
    `recursive` Download all assets in folders
    `timeout`   Wait timeout secs for GEE export task completion
    `kwargs`    Additional args to pass to ee.batch.Export.{}.toCloudStorage()
    '''
    gs_prefix = gs_prefix or _gs_bucket_prefix
    if directory and not os.path.isdir(directory):
        raise Exception(f"Folder {directory} does not exist")

    tasks, uris = export(assets, gs_bucket, gs_prefix, recursive, overwrite=True, wait_timeout=timeout, **kwargs)

    filenames = []

    for uri in uris:
        for _uri in gsbucket.getTileBlobs(uri):
            path = gsbucket.pathFromURI(_uri)
            fname = path[len(gs_prefix):].lstrip('/') if gs_prefix else path
            filenames.append(fname)
            gsbucket.download(_uri, fname, directory=directory)
            if clean:
                gsbucket.remove(_uri)

    return filenames






#ALIAS
mkdir = createFolder
rm = remove
mv = move
cp = copy

# old function names
removeAsset = remove
downloadAsset = download
downloadAssets = download
