import os
import re
from google.cloud import storage
import logging
from . import eeutil

# Silence warnings from googleapiclient.discovery_cache
# see https://github.com/googleapis/google-api-python-client/issues/299
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


logger = logging.getLogger(__name__)

# Unary client object
_gsClient = None
_gsBucket = None


def init(bucket=None, project=None, credentials=None):
    '''Initialize Google Cloud Storage client and default bucket
    
    Args:
        bucket (str): Default bucket to use
        project (str): Authenticate to this GCP project
        credentials (google.auth.credentials.Credentials): OAuth credentials
    '''
    global _gsClient
    global _gsBucket
    _gsClient = storage.Client(project, credentials=credentials) if project else storage.Client(credentials=credentials)
    if bucket:
        _gsBucket = _gsClient.bucket(bucket)
        if not _gsBucket.exists():
            logger.warning('Bucket gs://{bucket} does not exist, creating')
            _gsBucket.create()


def Client():
    '''Returns Google Cloud Storage client'''
    if not _gsClient:
        init()
    return _gsClient


def Bucket(bucket=None):
    '''Returns authenticated Bucket object'''
    bucket = _defaultBucketName(bucket)
    return Client().bucket(bucket)


def _defaultBucketName(bucket=None):
    '''Returns default bucket name if bucket is None'''
    if bucket is not None:
        return bucket
    if _gsBucket is None:
        raise Exception('No default bucket, run eeUtil.init() to set a default bucket')
    return _gsBucket.name


def asURI(path, bucket=None):
    '''Returns blob path as URI'''
    bucket = _defaultBucketName(bucket)
    return f"gs://{os.path.join(bucket, path)}"


def isURI(path, bucket=''):
    '''Returns true if path is valid URI for bucket'''
    start = f'gs://{bucket}'
    return (
        len(path) > len(start)+2 
        and path[:len(start)] == start
        and path[len(start)+1:].find('/') > -1
    )


def pathFromURI(uri):
    '''Returns blob path from URI'''
    return fromURI(uri)[1]


def fromURI(uri):
    '''Returns bucket name and blob path from URI'''
    if not isURI(uri):
        raise Exception(f'Path {uri} does not match gs://<bucket>/<blob>')
    return uri[5:].split('/', 1)


def exists(uri):
    '''check if blob exists'''
    bucket, path = fromURI(uri)
    return Bucket(bucket).blob(path).exists()


def stage(files, prefix='', bucket=None):
    '''Upload files to GCS

    Uploads files to gs://<bucket>/<prefix>/<filename>

    Args:
        files (list, str): Filenames of local files to upload
        prefix (str): Folder to upload to (prepended to file name)
        bucket (str): GCS bucket to upload to

    Returns:
        list: URIs of uploaded files
    '''

    files = (files,) if isinstance(files, str) else files
    gs_uris = []
    for f in files:
        path = os.path.join(prefix, os.path.basename(f))
        uri = asURI(path, bucket)
        logger.info(f'Uploading {f} to {uri}')
        Bucket(bucket).blob(path).upload_from_filename(f)
        gs_uris.append(uri)
    return gs_uris


def remove(gs_uris):
    '''
    Remove blobs from GCS

    Args:
        gs_uris (list, str): Full paths to blob(s) to remove `gs://<bucket>/<blob>`
    '''
    gs_uris = (gs_uris,) if isinstance(gs_uris, str) else gs_uris

    paths = {}
    for uri in gs_uris:
        bucket, path = fromURI(uri)
        if bucket in paths:
            paths[bucket].append(path)
        else:
            paths[bucket] = [path]

    for bucket, paths in paths:
        logger.info(f"Deleting {paths} from gs://{bucket}")
        Bucket(bucket).delete_blobs(paths, on_error=lambda x:x)


def download(gs_uri, filename=None, directory=None):
    '''
    Download blob from GCS

    Args:
        gs_uri (string): full path to blob `gs://<bucket>/<blob>`
        filename (string): name of local file to save (default: blob name)
        directory (string): local directory to save files to
    '''
    if filename is None:
        filename = os.path.basename(gs_uri)
    if directory is not None:
        filename = os.path.join(directory, filename)
    
    bucket, path = fromURI(gs_uri)
    logger.info(f"Downloading {gs_uri}")
    Bucket(bucket).blob(path).download_to_filename(filename)


def getTileBlobs(uri):
    '''Check the existance of an exported image or image tiles

    Matches either <blob>.tif or <blob>00000000X-00000000X.tif following
    EE image export tiling naming scheme.

    Returns:
        list: List of matching blobs
    '''
    bucket, path = fromURI(uri)
    prefix = f'{os.path.dirname(path)}/'
    basename, ext = os.path.splitext(os.path.basename(path))

    blobs = Bucket(bucket).list_blobs(prefix=prefix, delimiter='/')
    pattern = re.compile(rf'{prefix}{basename}(\d{{10}}-\d{{10}})?{ext}$')
    matches = [asURI(blob.name, bucket) for blob in blobs if pattern.match(blob.name)]

    return matches