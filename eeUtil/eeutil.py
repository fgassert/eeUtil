import os
import ee
import logging
import time
import datetime
import json
import warnings

from . import gsbucket

STRICT = True

GEE_JSON = os.getenv("GEE_JSON")
GEE_SERVICE_ACCOUNT = os.getenv("GEE_SERVICE_ACCOUNT") or "service account"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GEE_PROJECT = os.getenv("GEE_PROJECT") or os.getenv("CLOUDSDK_CORE_PROJECT")
GEE_STAGING_BUCKET = os.getenv("GEE_STAGING_BUCKET")
GEE_STAGING_BUCKET_PREFIX = os.getenv("GEE_STAGING_BUCKET_PREFIX")

# Unary GEE home directory
_cwd = ''
_gs_bucket_prefix = ''


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
    if bucket_prefix:
        init_opts['prefix'] = bucket_prefix
    try:
        gsbucket.init(bucket, **init_opts)
    except Exception as e:
        logging.warning("Could not initialize Google Cloud Storage Bucket.")
        logging.error(e)
    if bucket_prefix:
        _gs_bucket_prefix = bucket_prefix


def initJson(credential_json=GEE_JSON, project=GEE_PROJECT,
             bucket=GEE_STAGING_BUCKET):
    '''
    Writes json string to credential file and initializes

    Defaults from GEE_JSON env variable
    '''
    init('service_account', None, project, bucket, credential_json)


def getHome():
    '''Get user root directory'''
    assetRoots = ee.data.getAssetRoots()
    project = ee._cloud_api_utils._cloud_api_user_project
    if project == ee.data.DEFAULT_CLOUD_API_USER_PROJECT:
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
        if path[0] == '/':
            return path[1:]
        elif len(path) > 6 and path[:6] == 'users/':
            return path
        elif len(path) > 9 and path[:9] == 'projects/':
            return path
        else:
            return os.path.join(getCWD(), path)
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


def isFolder(asset):
    '''Check if path is folder or imageCollection'''
    if ee._cloud_api_utils.is_asset_root(asset):
        return True
    asset_info = info(asset)
    return asset_info and asset_info['type'] in (ee.data.ASSET_TYPE_FOLDER, 
                                                 ee.data.ASSET_TYPE_FOLDER_CLOUD,
                                                 ee.data.ASSET_TYPE_IMAGE_COLL,
                                                 ee.data.ASSET_TYPE_IMAGE_COLL_CLOUD)


def ls(path='', abspath=False):
    '''List assets in path'''
    if abspath:
        return [a['id']
                for a in ee.data.getList({'id': _path(path)})]
    else:
        return [os.path.basename(a['id'])
                for a in ee.data.getList({'id': _path(path)})]


def getAcl(asset):
    '''Get ACL of asset or folder'''
    return ee.data.getAssetAcl(_path(asset))


def setAcl(asset, acl={}, overwrite=False, recursive=False):
    '''Set ACL of asset

    `acl`       ('public'|'private'| ACL specification )
    `overwrite` If false, only change specified values
    '''
    path = _path(asset)
    if recursive and isFolder(path):
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
    logging.debug('Setting ACL to {} on {}'.format(acl, path))
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
        logging.debug(f'Created {ftype} {path}')
        ee.data.createAsset({'type': ftype}, path, overwrite)
    if public:
        setAcl(path, 'public')


def createImageCollection(path, overwrite=False, public=False):
    '''Create image collection'''
    createFolder(path, True, overwrite, public)


def copy(src, dest, overwrite=False, recursive=False):
    '''Copy asset'''
    if recursive and isFolder(src):
        is_image_collection = info(src)['type'] in (ee.data.ASSET_TYPE_IMAGE_COLL_CLOUD,
                                                    ee.data.ASSET_TYPE_IMAGE_COLL)
        createFolder(dest, is_image_collection)
        for child in ls(src):
            copy(os.path.join(src, child), os.path.join(dest, child), overwrite, recursive)
    else:
        ee.data.copyAsset(_path(src), _path(dest), overwrite)


def move(src, dest, overwrite=False, recursive=False):
    '''Move asset'''
    src = _path(src)
    copy(src, _path(dest), overwrite, recursive=False)
    remove(src, recursive)


def remove(asset, recursive=False):
    '''Delete asset from GEE'''
    if recursive and isFolder(asset):
        for child in ls(asset, abspath=True):
            remove(child, recursive)
    logging.debug('Deleting asset {}'.format(asset))
    ee.data.deleteAsset(_path(asset))


def formatDate(date):
    '''Format date as ms since last epoch'''
    if isinstance(date, int):
        return date
    seconds = (date - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return int(seconds * 1000)


def setBucketPrefix(prefix=''):
    '''Set the default prefix to be used for storage bucket operations'''
    global _gs_bucket_prefix
    _gs_bucket_prefix = prefix


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
            logging.error(status['error_message'])
        if STRICT:
            raise Exception(status)
        logging.error(f"Task {status['id']} ended with state {status['state']}")
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
        task_ids = [t['id'] for t in getTasks() if t['state'] in (
            ee.batch.Task.State.READY,
            ee.batch.Task.State.RUNNING,
            ee.batch.Task.State.UNSUBMITTED,
        )]

    start = time.time()
    elapsed = 0
    while elapsed < timeout or timeout == 0:
        elapsed = time.time() - start
        finished = [_checkTaskCompleted(task) for task in task_ids]
        if all(finished):
            logging.debug(f'Tasks {task_ids} completed after {elapsed}s')
            return True
        time.sleep(5)
    logging.error(f'Stopped waiting for tasks after {timeout} seconds')
    if STRICT:
        raise Exception(task_ids)
    return False


def waitForTask(task_id, timeout=3600):
    '''Wait for task to complete, fail, or timeout'''
    return waitForTasks([task_id], timeout)


def ingestAsset(gs_uri, asset, date=None, wait_timeout=None, bands=[]):
    '''[DEPRECATED] please use eeUtil.ingest instead'''
    warnings.warn('[DEPRECATED] please use eeUtil.ingest instead', DeprecationWarning)   
    return ingest(gs_uri, asset, wait_timeout, bands)


def _guessIngestTableType(path):
    if os.path.splitext(path)[-1] in ['.csv', '.zip']:
        return True
    return False

def ingest(gs_uri, asset, wait_timeout=None, bands=[]):
    '''
    Upload asset from GS to EE

    `gs_uri`       should be formatted `gs://<bucket>/<blob>`
    `asset`        destination path
    `wait_timeout` if non-zero, wait timeout secs for task completion
    `bands`        optional band name dictionary
    '''
    asset_id = _path(asset)
    if _guessIngestTableType(gs_uri):
        params = {'id': asset_id, 'sources': [{'primaryPath': gs_uri}]}
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
    logging.debug(f"Ingesting {gs_uri} to {asset}: {task_id}")
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
                 timeout=3600, clean=True, bands=[]):
    '''Stage files to cloud storage and ingest into Earth Engine

    `files`        local file path or list of paths
    `assets`       destination asset ID or list of asset IDs
    `gs_prefix`    GS folder for staging (else files are staged to bucket root)
    `public`       set acl public if True
    `timeout`      wait timeout secs for completion of GEE ingestion
    `clean`        delete files from GS after completion
    `bands`        band names to assign, all assets must have the same number of bands
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
        logging.error(e)
    if clean:
        gsbucket.remove(gs_uris)
    
    return assets

def download(assets, directory=None, gs_prefix='', clean=True, timeout=3600, **kwargs):
    '''Export image assets to GS and downloads to local machine

    `asset`     Asset ID or list of asset IDs
    `directory` Optional local directory to save assets to
    `gs_prefix` GS folder for staging (else files are staged to bucket root)
    `timeout`   Wait timeout secs for GEE export task completion
    `clean`     Remove file from GS after download
    `kwargs`    Additional args to pass to ee.batch.Export.image.toCloudStorage()
    '''
    if type(assets) is str:
        assets = [assets]

    gs_prefix = gs_prefix or _gs_bucket_prefix
    task_ids = []
    uris = []
    if not os.path.isdir(directory):
        raise Exception(f"Folder {directory} does not exist")
    for asset in assets:
        image = ee.Image(_path(asset))
        path = os.path.join(gs_prefix, os.path.basename(asset))

        task = ee.batch.Export.image.toCloudStorage(
            image,
            bucket=gsbucket.getName(),
            fileNamePrefix=path,
            **kwargs
        )
        task.start()
        task_ids.append(task.id)

        uri = f'{gsbucket.asURI(path)}.tif'
        uris.append(uri)

        logging.debug(f"Exporting asset {asset} to {uri}")

    filenames = []

    try:
        waitForTasks(task_ids, timeout)
        for uri in uris:
            gsbucket.download(uri, directory=directory)
            if clean:
                gsbucket.remove(uri)
    except Exception as e:
        logging.error(e)

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
