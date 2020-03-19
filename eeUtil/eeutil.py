import os
import ee
import logging
import time
import datetime
import json

from . import gsbucket

STRICT = True

GEE_JSON = os.getenv("GEE_JSON")
GEE_SERVICE_ACCOUNT = os.getenv("GEE_SERVICE_ACCOUNT")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GEE_PROJECT = os.getenv("GEE_PROJECT")
GEE_STAGING_BUCKET = os.getenv("GEE_STAGING_BUCKET")

# Unary GEE home directory
_home = ''


def init(service_account=GEE_SERVICE_ACCOUNT,
         credential_path=GOOGLE_APPLICATION_CREDENTIALS,
         project=GEE_PROJECT, bucket=GEE_STAGING_BUCKET,
         credential_json=GEE_JSON):
    '''
    Initialize Earth Engine and Google Storage bucket connection.

    Defaults to read from environment.

    If no service_account is provided, will use default credentials from
    `earthengine authenticate` utility.

    `service_account` Service account name. Will need access to both GEE and
                      Storage
    `credential_path` Path to json file containing private key
    `project`         GCP project for earthengine and storage bucket
    `bucket`          Storage bucket for staging assets for ingestion

    https://developers.google.com/earth-engine/service_account
    '''
    init_opts = {}
    if service_account or credential_json:
        if credential_json:
            init_opts['credentials'] = ee.ServiceAccountCredentials(service_account, key_data=credential_json)
        elif credential_path:
            init_opts['credentials'] = ee.ServiceAccountCredentials(service_account, key_file=credential_path)
    if project:
        init_opts['project'] = project
    ee.Initialize(**init_opts)
    if credential_path:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    try:
        gsbucket.init(bucket, **init_opts)
    except Exception as e:
        logging.warning("Could not initialize Google Cloud Storage Bucket.")
        logging.error(e)


def initJson(credential_json=GEE_JSON, project=GEE_PROJECT,
             bucket=GEE_STAGING_BUCKET):
    '''
    Writes json string to credential file and initializes

    Defaults from GEE_JSON env variable
    '''
    init('service_account', None, project, bucket, credential_json)


def getHome():
    '''Get user root directory'''
    global _home
    assetRoots = ee.data.getAssetRoots()
    project = ee._cloud_api_utils._cloud_api_user_project
    if project == 'earthengine-legacy':
        if not len(assetRoots):
            raise Exception(f"No available assets for provided credentials in project {project}")
        _home = assetRoots[0]['id']
    else:
        _home = f'projects/{project}/assets/'
    return _home


def _getHome():
    '''Cached get user root directory'''
    global _home
    return _home if _home else getHome()


def _path(path):
    '''Add asset root directory to path if not already existing'''
    if path:
        if path[0] == '/':
            return path[1:]
        elif len(path) > 6 and path[:6] == 'users/':
            return path
        elif len(path) > 9 and path[:9] == 'projects/':
            return path
        else:
            return os.path.join(_getHome(), path)
    return _getHome()


def getQuota():
    '''Get GEE usage quota'''
    return ee.data.getAssetRootQuota(_getHome())


def info(asset=''):
    '''Get asset info'''
    return ee.data.getInfo(_path(asset))


def exists(asset):
    '''Check if asset exists'''
    return True if info(asset) else False


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


def setAcl(asset, acl={}, overwrite=False):
    '''Set ACL of asset

    `acl`       ('public'|'private'| ACL specification )
    `overwrite` If false, only change specified values
    '''
    _acl = {} if overwrite else getAcl(asset)
    _acl.pop('owners', None)
    if acl == 'public':
        _acl["all_users_can_read"] = True
    elif acl == 'private':
        _acl["all_users_can_read"] = False
    else:
        _acl.update(acl)
    acl = json.dumps(_acl)
    logging.debug('Setting ACL to {} on {}'.format(acl, asset))
    ee.data.setAssetAcl(_path(asset), acl)


def setProperties(asset, properties={}):
    '''Set asset properties'''
    return ee.data.setAssetProperties(_path(asset), properties)


def createFolder(path, imageCollection=False, overwrite=False,
                 public=False):
    '''Create folder or image collection'''
    ftype = (ee.data.ASSET_TYPE_IMAGE_COLL if imageCollection
             else ee.data.ASSET_TYPE_FOLDER)
    ee.data.createAsset({'type': ftype}, _path(path), overwrite)
    if public:
        setAcl(_path(path), 'public')


def createImageCollection(path, overwrite=False, public=False):
    '''Create image collection'''
    createFolder(path, True, overwrite, public)


def _checkTaskCompleted(task_id):
    '''Return True if task completed else False'''
    status = ee.data.getTaskStatus(task_id)[0]
    if status['state'] in (ee.batch.Task.State.CANCELLED,
                           ee.batch.Task.State.FAILED):
        if 'error_message' in status:
            logging.error(status['error_message'])
        if STRICT:
            raise Exception(status)
        logging.error('Task ended with state {}'.format(status['state']))
        return False
    elif status['state'] == ee.batch.Task.State.COMPLETED:
        return True
    return False


def waitForTasks(task_ids, timeout=3600):
    '''Wait for tasks to complete, fail, or timeout'''
    start = time.time()
    elapsed = 0
    while elapsed < timeout:
        elapsed = time.time() - start
        finished = [_checkTaskCompleted(task) for task in task_ids]
        if all(finished):
            return True
        time.sleep(5)
    logging.error('Tasks timed out after {} seconds'.format(timeout))
    if STRICT:
        raise Exception(task_ids)
    return False


def waitForTask(task_id, timeout=3600):
    '''Wait for task to complete, fail, or timeout'''
    start = time.time()
    elapsed = 0
    while elapsed < timeout:
        elapsed = time.time() - start
        finished = _checkTaskCompleted(task_id)
        if finished:
            return True
        time.sleep(5)
    logging.error('Task timed out after {} seconds'.format(timeout))
    if STRICT:
        raise Exception(task_id)
    return False


def copy(src, dest, allowOverwrite=False):
    '''Copy asset'''
    return ee.data.copyAsset(_path(src), _path(dest), allowOverwrite)


def move(src, dest, allowOverwrite=False):
    '''Move asset'''
    src = _path(src)
    copy(src, _path(dest), allowOverwrite)
    removeAsset(src)


def formatDate(date):
    '''Format date as ms since last epoch'''
    if isinstance(date, int):
        return date
    seconds = (date - datetime.datetime.utcfromtimestamp(0)).total_seconds()
    return int(seconds * 1000)


def ingestAsset(gs_uri, asset, date='', wait_timeout=0, bands=[]):
    '''
    Upload asset from GS to EE

    `gs_uri`       should be formatted `gs://<bucket>/<blob>`
    `asset`        destination path
    `date`         optional date tag (datetime.datetime or int ms since epoch)
    `wait_timeout` if non-zero, wait timeout secs for task completion
    `bands`        optional band name dictionary
    '''
    params = {'id': _path(asset),
              'tilesets': [{'sources': [{'primaryPath': gs_uri}]}]}
    if date:
        params['properties'] = {'system:time_start': formatDate(date),
                                'system:time_end': formatDate(date)}
    if bands:
        if isinstance(bands[0], str):
            bands = [{'id': b} for b in bands]
        params['bands'] = bands
    task_id = ee.data.newTaskId()[0]
    logging.debug('Ingesting {} to {}: {}'.format(gs_uri, asset, task_id))
    ee.data.startIngestion(task_id, params, True)
    if wait_timeout:
        waitForTask(task_id, wait_timeout)
    return task_id


def uploadAsset(filename, asset, gs_prefix='', date='', public=False,
                timeout=300, clean=True, bands=[]):
    '''
    Stage file to GS and ingest to EE

    `file`         local file paths
    `asset`        destination path
    `gs_prefix`    GS folder for staging (else files are staged to bucket root)
    `date`         Optional date tag (datetime.datetime or int ms since epoch)
    `public`       set acl public if True
    `timeout`      wait timeout secs for completion of GEE ingestion
    `clean`        delete files from GS after completion
    '''
    gs_uris = gsbucket.stage(filename, gs_prefix)
    try:
        ingestAsset(gs_uris[0], asset, date, timeout, bands)
        if public:
            setAcl(asset, 'public')
    except Exception as e:
        logging.error(e)
    if clean:
        gsbucket.remove(gs_uris)

# ALIAS
upload = uploadAsset

def uploadAssets(files, assets, gs_prefix='', dates=[], public=False,
                 timeout=300, clean=True, bands=[]):
    '''
    Stage files to GS and ingest to EE

    `files`        local file paths
    `assets`       destination paths
    `gs_prefix`    GS folder for staging (else files are staged to bucket root)
    `dates`        Optional date tags (datetime.datetime or int ms since epoch)
    `public`       set acl public if True
    `timeout`      wait timeout secs for completion of GEE ingestion
    `clean`        delete files from GS after completion
    '''
    gs_uris = gsbucket.stage(files, gs_prefix)
    if dates:
        task_ids = [ingestAsset(gs_uris[i], assets[i], dates[i], 0, bands)
                    for i in range(len(files))]
    else:
        task_ids = [ingestAsset(gs_uris[i], assets[i], '', 0, bands)
                    for i in range(len(files))]
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


def removeAsset(asset, recursive=False):
    '''Delete asset from GEE'''
    if recursive:
        if info(asset)['type'] in (ee.data.ASSET_TYPE_FOLDER,
                                   ee.data.ASSET_TYPE_IMAGE_COLL):
            for child in ls(asset, abspath=True):
                removeAsset(child)
    logging.debug('Deleting asset {}'.format(asset))
    ee.data.deleteAsset(_path(asset))


def downloadAsset(asset, filename=None, gs_prefix='', timeout=3600, clean=True, **kwargs):
    '''Export image asset to GS and download to local machine

    `asset`     Asset ID
    `filename`  Optional filename for export otherwise defaults to Asset ID
    `gs_prefix` GS folder for staging (else files are staged to bucket root)
    `timeout`   Wait timeout secs for export task completion
    `clean`     Remove file from GS after download
    `kwargs`    Additional args to pass to ee.batch.Export.image.toCloudStorage()
    '''
    if filename is None:
        filename = os.path.basename(asset)
    else:
        # .tif is automatically appended...
        filename = os.path.splitext(filename)[0]
    image = ee.Image(asset)
    path = os.path.join(gs_prefix, filename)
    task = ee.batch.Export.image.toCloudStorage(
        image,
        bucket=gsbucket.getName(),
        fileNamePrefix=path,
        **kwargs
    )
    task.start()

    uri = f"{gsbucket.asURI(path)}.tif"
    logging.debug(f"Exporting asset {asset} to {uri}")

    try:
        waitForTask(task.id, timeout)
        gsbucket.download(uri, filename)
        if clean:
            gsbucket.remove(uri)
    except Exception as e:
        logging.error(e)


# ALIAS
download = downloadAsset


def downloadAssets(assets, gs_prefix='', clean=True, timeout=3600, **kwargs):
    '''Export image assets to GS and download to local machine

    `asset`     Asset ID
    `gs_prefix` GS folder for staging (else files are staged to bucket root)
    `timeout`   Wait timeout secs for export task completion
    `clean`     Remove file from GS after download
    `kwargs`    Additional args to pass to ee.batch.Export.image.toCloudStorage()
    '''
    task_ids = []
    uris = []
    for asset in assets:
        image = ee.Image(asset)
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

    try:
        waitForTasks(task_ids, timeout)
        for uri in uris:
            gsbucket.download(uri)
            if clean:
                gsbucket.remove(uri)
    except Exception as e:
        logging.error(e)
