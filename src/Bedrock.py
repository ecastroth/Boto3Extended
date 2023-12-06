# -----------------------------------------------------------------------------
#                           Libraries
# -----------------------------------------------------------------------------
# Default
import json
from functools import partial
from typing import Tuple, Union

# pip
import boto3
from tqdm.contrib.concurrent import process_map


# -----------------------------------------------------------------------------
#                           Functions
# -----------------------------------------------------------------------------
def _bedrock_call(prompt: str, profile_name: str, region_name: str, 
                  call_conf: dict[str, str]) -> Tuple[dict, int, int]:
    # Build bedrock client
    if profile_name:
        session = boto3.Session(profile_name= profile_name)
        client = session.client(service_name='bedrock-runtime', 
                                region_name= region_name)
    else:
        client = boto3.client(service_name='bedrock-runtime', 
                              region_name= region_name)
    
    # Build body of the call
    body = json.dumps({
        "prompt": prompt,
        "max_tokens_to_sample": call_conf['max_tokens_to_sample'],
        "temperature": call_conf['temperature'],
        "top_p": call_conf['top_p'],
    })

    # Invoke model
    response = client.invoke_model(body=body, 
                                   modelId=call_conf['model_id'], 
                                   accept=call_conf['accept'], 
                                   contentType=call_conf['content_type'])

    # Save response body
    response_body = json.loads(response.get('body').read())

    approx_n_tokens_in = len(prompt) / 6
    approx_n_tokens_out = len(response_body.get('completion')) / 6

    return response_body, (int(approx_n_tokens_in), int(approx_n_tokens_out))


# -----------------------------------------------------------------------------
#                           Functions
# -----------------------------------------------------------------------------
class BedrockModel():
    def __init__(self, region_name: str, profile_name: str= '',
                 model_id: str= 'anthropic.claude-v2:1'):
        # Default invoke configuration
        self.default_call_conf = {'model_id': model_id,
                                  'max_tokens_to_sample': 4000,
                                  'temperature': 0,
                                  'top_p': 0.9,
                                  'accept': 'application/json',
                                  'content_type': 'application/json'}

        # Save user input variables
        self.model_conf = model_id
        self.region_name = region_name
        self.profile_name = profile_name


    def single_invoke(self, prompt: str, call_conf: dict= {}):
        # Restore call defaults
        self.call_conf = self.default_call_conf
        # Add defaults to configuration
        self.call_conf.update(call_conf)
        # Invoke
        return _bedrock_call(prompt, self.profile_name, self.region_name, self.call_conf)

    
    def invoke(self, prompts: list[str], 
               call_conf: dict[str, Union[int, float, str]]= {}) -> Tuple[dict, int, int]:
        # Restore call defaults
        self.call_conf = self.default_call_conf
        # Add defaults to configuration
        self.call_conf.update(call_conf)

        # Set client and call config
        aux_function = partial(_bedrock_call,
                               profile_name= self.profile_name,
                               region_name= self.region_name,
                               call_conf= self.call_conf)

        # Multiprocess invoke
        return process_map(aux_function, prompts, chunksize= 1)