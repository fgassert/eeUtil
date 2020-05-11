import os
from google.cloud import storage
import logging

from . import eeutil

# Unary bucket object
_gsBucket = None

def init(bucket=None, project=None, credentials=None):
    '''Initalize google cloud storage bucket'''
    global _gsBucket
    global _gsBucketPrefix
    if not bucket:
        bucket = _getDefaultBucket()
        logging.warning('No bucket provided, attempting to use default {}'.format(bucket))
    gsClient = storage.Client(project, credentials=credentials) if project else storage.Client(credentials=credentials)
    _gsBucket = gsClient.bucket(bucket)
    if not _gsBucket.exists():
        logging.info('Bucket {} does not exist, creating'.format(bucket))
        _gsBucket.create()


def _getDefaultBucket():
    '''Generate new bucket name'''
    return 'eeutil-{}'.format(hash(eeutil.getHome()))


def getName():
    '''Returns bucket name'''
    if not _gsBucket:
        raise Exception('GS Bucket not initialized, run init()')
    return _gsBucket.name


def asURI(path, bucket=None):
    '''Returns blob path as URI'''
    if bucket is None:
        bucket = getName()
    return f"gs://{bucket}/{path}"


def isURI(path):
    '''Returns true if path is valid URI for this bucket'''
    min_len = len(getName()) + 5
    return len(path) > min_len and path[:min_len] == f'gs://{getName()}'


def pathFromURI(uri):
    '''Returns blob path from URI'''
    if isURI(uri):
        return uri[6 + len(getName()):]
    else:
        raise Exception(f'Path {uri} does not match gs://{getName()}/<blob>')


def stage(files, prefix=''):
    '''Upload files to GS with prefix'''
    if not _gsBucket:
        raise Exception('GS Bucket not initialized, run init()')

    files = (files,) if isinstance(files, str) else files
    gs_uris = []
    for f in files:
        path = os.path.join(prefix, os.path.basename(f))
        uri = asURI(path)
        logging.debug(f'Uploading {f} to {uri}')
        _gsBucket.blob(path).upload_from_filename(f)
        gs_uris.append(uri)
    return gs_uris


def remove(gs_uris):
    '''
    Remove blobs from GS

    `gs_uris` must be full paths `gs://<bucket>/<blob>`
    '''
    if not _gsBucket:
        raise Exception('GS Bucket not initialized, run init()')

    gs_uris = (gs_uris,) if isinstance(gs_uris, str) else gs_uris
    paths = []
    for uri in gs_uris:
        paths.append(pathFromURI(uri))

    logging.debug(f"Deleting {paths} from gs://{getName()}")    
    # on_error null function to ignore NotFound
    _gsBucket.delete_blobs(paths, lambda x:x)


def download(gs_uri, filename=None, directory=None):
    '''
    Download blob from GS

    `gs_uri` must be full path `gs://<bucket>/<blob>`
    `filename` name of local file to save defaults to remote name
    `directory` save files to directory
    '''
    if not _gsBucket:
        raise Exception('GS Bucket not initialized, run init()')

    if filename is None:
        filename = os.path.basename(gs_uri)
    if directory is not None:
        filename = os.path.join(directory, filename)

    logging.debug(f"Downloading gs://{path}")    
    if isURI:
        path = pathFromURI(gs_uri)
        _gsBucket.blob(path).download_to_filename(filename)