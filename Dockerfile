# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies if needed
RUN yum update -y && \
    yum install -y gcc gcc-c++ postgresql-devel && \
    yum clean all

# Copy requirements first for better caching
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/
COPY lambda_utils.py ${LAMBDA_TASK_ROOT}/
COPY lambda_data_fetcher.py ${LAMBDA_TASK_ROOT}/

# Optional: Copy config if you have it
# COPY config/ ${LAMBDA_TASK_ROOT}/config/

# Set the Lambda handler
CMD ["lambda_function.lambda_handler"]