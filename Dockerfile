# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# Make sure Lambda can import data_fetchers
ENV PYTHONPATH="${LAMBDA_TASK_ROOT}:${LAMBDA_TASK_ROOT}/data_fetchers"

# Install system dependencies
RUN yum update -y && \
    yum install -y gcc gcc-c++ postgresql-devel && \
    yum clean all

# Copy requirements first (for layer caching)
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Lambda code
COPY lambda_function.py     ${LAMBDA_TASK_ROOT}/
COPY lambda_utils.py        ${LAMBDA_TASK_ROOT}/
COPY lambda_data_fetcher.py ${LAMBDA_TASK_ROOT}/

# Copy your new boxscore fetcher module
COPY data_fetchers/         ${LAMBDA_TASK_ROOT}/data_fetchers/

# (Optional) Copy config if needed
# COPY config/               ${LAMBDA_TASK_ROOT}/config/

# Set the Lambda handler
CMD ["lambda_function.lambda_handler"]