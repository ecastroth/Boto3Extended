# Boto3 Extended Library

## Objective
This library contains some Classes and Functions that allow more simple use of the AWS Boto3 library. Some of the this functions/methods make use of multiprocessing with the objective of getting more fast code.

Currently, the supported AWS Services are:
- S3
- Rekognition

***
## Install
For this library to work you'll need to install it's dependencies included on the `requirements` directory.

**conda install:**
```
conda env create -f requirements/conda-requirements.yaml
```

**pip install:**
```
pip install -r requirements/pip-requirements.txt
```