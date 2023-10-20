# -----------------------------------------------------------------------------
#                           Libraries
# -----------------------------------------------------------------------------
# Default
import os
from functools import partial

# pip
import boto3
from botocore.exceptions import ClientError
from tqdm.contrib.concurrent import process_map

# Own
from . import Utils as utils

# -----------------------------------------------------------------------------
#                           Functions
# -----------------------------------------------------------------------------
def listAllBuckets(profile_name: str) -> list[str]:
    '''List all the S3 buckets in the profile'''
    # Session
    session = boto3.Session(profile_name= profile_name)
    # Client
    s3_client = session.client('s3')
    # List
    response = s3_client.list_buckets()
    buckets = [x['Name'] for x in response['Buckets']]
    return buckets


def deleteBuckets(bucket_names: list[str], profile_name: str, auto_empty: bool= False,
                  message: str= ''):
    '''Multiprocess deletion of various buckets. Authomatic deletes it's files if
    necessary'''
    aux_function = partial(deleteBucket, profile_name= profile_name, 
                           auto_empty= auto_empty, verbose= False)

    # Deletion
    tqdm_message = message if message else '🚮  Deleting buckets'
    results = process_map(aux_function, bucket_names,
                          desc= tqdm_message,
                          chunksize= 1)
    # TODO: print undeleted bucket names


def deleteBucket(bucket_name: str, profile_name: str, auto_empty: bool= False,
                 verbose: bool= True) -> None:
    '''Deletes a bucket and empty it if necessary'''
    # Successfully deleted files, Errors in deleted files
    n_deleted, n_errors = 0, 0
    # Session
    session = boto3.Session(profile_name= profile_name)
    # Client
    s3_client = session.client('s3')

    # Automatically empty the bucket
    if auto_empty:
        response = s3_client.list_objects_v2(Bucket= bucket_name)
        # Bucket contains elements
        if 'Contents' in response.keys():
            # Paginator to iterate every 1000 elements
            s3_paginator = s3_client.get_paginator('list_objects_v2')
            for page in s3_paginator.paginate(Bucket= bucket_name):
                paths = [file['Key'] for file in page['Contents']]
                delete_response = _deleteFromBucket(paths= paths, 
                                                    profile_name= profile_name,
                                                    bucket_name= bucket_name)
                n_errors += delete_response[1]
                n_deleted += delete_response[0]

    # Delete
    try:
        s3_client.delete_bucket(Bucket= bucket_name)
        # Summary of deletion
        if verbose:
            print('ℹ️  Summary:')
            print(f'\tDeleted files: {n_deleted}')
            print(f'\tFiles that cannot be deleted due to errors: {n_errors}')
            print(f'🚮 Deleted {bucket_name} bucket')
    except ClientError as e:
        if 'BucketNotEmpty' in str(e):
            error_message = ("🛑 The bucket you're trying to delete still contains "
                             + "elements. Please use auto_empty= True.")
            print(error_message)
        else:
            raise e


def _uploadToBucket(paths: tuple[str, str], profile_name: str, 
                     bucket_name: str) -> bool:
    '''Upload file to S3 bucket only if the file was not already on it
    '''
    # Session
    session = boto3.Session(profile_name= profile_name)
    # Client
    s3client = session.client('s3')
    # Unzip paths
    localpath, s3path = paths[0], paths[1]
    # Upload
    try:
        # Verifies if file exists
        s3client.get_object(Bucket= bucket_name, Key= s3path)
    except ClientError as e:
        # File does not exists. Upload
        if e.response['Error']['Code'] == 'NoSuchKey':
            s3client.upload_file(localpath, bucket_name, s3path)
            return 1
        # Other exceptions
        else:
            raise
    else:
        return 0

    
def _downloadFromBucket(paths: tuple[str, str], profile_name: str, 
                         bucket_name: str) -> bool:
    '''Download file from S3 bucket only if the file was not downloaded before
    '''
    # Session
    session = boto3.Session(profile_name= profile_name)
    # Client
    s3client = session.client('s3')
    # Unzip paths
    localpath, s3path = paths[0], paths[1]

    # File does not exists so its downloaded
    if not os.path.exists(localpath):
        s3client.download_file(bucket_name, s3path, localpath)
        return 1
    # File is not downloaded
    else:
        return 0


def _deleteFromBucket(paths: list[str], profile_name: str,
                      bucket_name: str) -> tuple[int, int]:
    '''Delete up to 1000 files from an S3 bucket'''
    # Successfully deleted files, Errors in deleted files
    n_deleted, n_errors = 0, 0
    # Session
    session = boto3.Session(profile_name= profile_name)
    # Client
    s3client = session.client('s3')

    # Files to delete
    obj2delete = {'Objects': [{'Key': utils.formatXML(p)} for p in paths]}
    # Delete
    response = s3client.delete_objects(Bucket= bucket_name, 
                                        Delete= obj2delete)
    if 'Deleted' in response.keys():
        n_deleted += len(response['Deleted'])
    if 'Errors' in response.keys():
        n_errors += len(response['Errors'])
    return n_deleted, n_errors 
    
    
# -----------------------------------------------------------------------------
#                           S3 Class
# -----------------------------------------------------------------------------
class Bucket():
    def __init__(self, profile_name, bucket_name):
        self.profile_name = profile_name
        self.bucket_name = bucket_name

        print('🔮 Instanciating S3 Bucket')
        self._verifiyBucket()


    def _verifiyBucket(self):
        '''Verifies the existence of the bucket and gets the aws region'''
        # Session
        session = boto3.Session(profile_name= self.profile_name)
        # Connection
        s3client = session.client('s3')
        # Verifies bucket exists and get aws region
        try:
            s3client.head_bucket(Bucket= self.bucket_name)
            location_response = s3client.get_bucket_location(Bucket= self.bucket_name)
            self.region_name = 'us-east-1' if location_response['LocationConstraint'] is None else location_response['LocationConstraint']
            print(f'✅ S3 Bucket {self.bucket_name} can be accessed')
        except Exception as e:
            aux = ' '.join([f'❌ The bucket of name "{self.bucket_name}" cannot be',
                  f'verified. The following exception was rised: {e}'])
            raise Exception(aux)


    def uploadFiles(self, localpaths: list[str], s3paths: list[str], 
                   message: str= '') -> None:
        '''Multiprocess upload files to s3 only if the files were not 
        uploaded before'''
        # Set profile and bucket names
        aux_function = partial(_uploadToBucket,
                               profile_name= self.profile_name,
                               bucket_name= self.bucket_name)
        # Zip arguments
        paths = list(zip(localpaths, s3paths))
        # Upload
        tqdm_message = message if message else f'⬆️  Uploading files from {self.bucket_name}'
        uploaded = process_map(aux_function, paths, 
                               desc= tqdm_message,
                               chunksize= 1)
        # Quantity of uploaded images
        print(f'⬆️  {sum(uploaded)} files were uploaded to {self.bucket_name}.')
    

    def downloadFiles(self, localpaths: list[str], s3paths: list[str], 
                      message: str= '') -> None:
        '''Multiprocess download files from s3 only if the files were not 
        downloaded before
        '''
        # Set profile and bucket names
        aux_function = partial(_downloadFromBucket,
                               profile_name= self.profile_name,
                               bucket_name= self.bucket_name)
        # Zip arguments
        paths = list(zip(localpaths, s3paths))
        # Upload
        tqdm_message = message if message else f'⬇️  Downloading files from {self.bucket_name}'
        downloaded = process_map(aux_function, paths, 
                                 desc= tqdm_message,
                                 chunksize= 1)
        # Quantity of uploaded images
        print(f'⬇️  {sum(downloaded)} files were downloaded from {self.bucket_name}.')


    def deleteFiles(self, s3paths: list[str], message: str= '') -> None:
        '''Multiprocess deletion of files from s3 bucket'''
        # Set profile and bucket names
        aux_function = partial(_deleteFromBucket,
                               profile_name= self.profile_name,
                               bucket_name= self.bucket_name)
        
        tqdm_message = message if message else f'🚮  Deleting files from {self.bucket_name}'
        # Build chunks of max size 1000
        s3paths = [x for x in utils.splitList(s3paths, 1000)]
        # Delete
        deleted = process_map(aux_function, s3paths,
                              desc= tqdm_message,
                              chunksize= 1)
        n_errors = sum([x[1] for x in deleted])
        n_deleted = sum([x[0] for x in deleted])
        print(f'🚮  {n_errors} files cannot be deleted from {self.bucket_name}')
        print(f'🚮  {n_deleted} files were deleted from {self.bucket_name}')


    def listElements(self, prefix: str= '') -> list[str]:
        '''List files inside the bucket'''
        # Session
        session = boto3.Session(profile_name= self.profile_name)
        # Client
        s3_client = session.client('s3')
        # Paginator to iterate every 1000 elements
        s3_paginator = s3_client.get_paginator('list_objects_v2')

        # Get names
        elements = []
        operation_parameters = {'Bucket': self.bucket_name,
                                'Prefix': prefix}
        for page in s3_paginator.paginate(**operation_parameters):
            if 'Contents' not in page.keys():
                print('ℹ️  No elements were founded')
                return []
            contents = [key['Key'] for key in page['Contents']]
            elements.extend(contents)
        return elements