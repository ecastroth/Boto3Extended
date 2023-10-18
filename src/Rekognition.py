
# -----------------------------------------------------------------------------
#                           Libraries
# -----------------------------------------------------------------------------
# Default
from functools import partial

# pip
import boto3
import pandas as pd
from tqdm.contrib.concurrent import process_map

# Own
from S3 import S3Bucket


# -----------------------------------------------------------------------------
#                           Functions
# -----------------------------------------------------------------------------
def callRekognition(s3path: str, profile_name: str, bucket_name: str, 
                    region_name: str) -> dict:
    '''Unique call to AWS Rekognition'''
    # Session
    session = boto3.Session(profile_name= profile_name)
    # Client
    rekognition_client = session.client('rekognition', region_name)
    # Call
    response = rekognition_client.detect_text(
        Image= {'S3Object': {'Bucket': bucket_name, 
                             'Name': s3path}}
    )
    return response


# -----------------------------------------------------------------------------
#                           Classes
# -----------------------------------------------------------------------------
class Rekognition():
    def __init__(self, profile_name, bucket_name):
        # Profile and bucket name
        self.profile_name = profile_name
        self.bucket_name = bucket_name

        # Verify if bucket can be accessed and its region
        self.region_name = S3Bucket(profile_name, bucket_name).region_name


    def predict(self, s3paths: list[str]):
        '''Gets raw Rekognition responses'''
        # TODO: Upload files to S3 if not allready in there
        # Set profile, bucket and region
        aux_function = partial(callRekognition, 
                               profile_name= self.profile_name, 
                               bucket_name= self.bucket_name, 
                               region_name= self.region_name)
        # Get responses
        return process_map(aux_function, s3paths, 
                           desc= '📝 Obtaining Rekogntion predictions',
                           chunksize= 1)

    
    def standarizeResponses(self, responses):
        '''Returns a list of [[[text1, bbox1, conf1], [...]], ...]'''
        processed = []
        for response in responses:
            if response['TextDetections']:
                aux_df = pd.DataFrame([x for x in response['TextDetections'] 
                                       if x['Type'] == 'WORD'])
                aux_df['x'] = aux_df['Geometry'].str['BoundingBox'].str['Left']
                aux_df['y'] = aux_df['Geometry'].str['BoundingBox'].str['Top']
                aux_df['w'] = aux_df['Geometry'].str['BoundingBox'].str['Width'] 
                aux_df['h'] = aux_df['Geometry'].str['BoundingBox'].str['Height'] 
                processed.append([(t, b, c) for t, b, c 
                                  in zip(aux_df['DetectedText'].values.tolist(),
                                         aux_df[['x', 'y', 'w', 'h']].values.tolist(),
                                         aux_df['Confidence'].values.tolist())])
            else:
                processed.append(['', [], 0])