# Airflow S3 XCom Backend

Custom XCom backend implementation for Airflow, with data serialization to S3.

## About

Servers as a simple custom implementation of XCom backend for Airflow. It's backward compatible, but if you return Pandas DataFrame from the task, it will be serialized and stored in S3. Only the S3 path is sent afterwards. The receiving task will retrieve deserialized object.

## Installation

1. Copy the `xcom_s3_backend.py` to somewhere on `PYTHONPATH` in your Airflow image.
2. Configure environment variable `AIRFLOW__CORE__XCOM_BACKEND` to `xcom_s3_backend.S3XComBackend`.
3. Configure environment variable `S3_XCOM_BUCKET_NAME` to the name of your S3 bucket.
4. Test the backend by returning Pandas DataFrame from your task.
