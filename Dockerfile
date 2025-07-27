# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Force home (and thus pybaseball’s default cache dir) into /tmp
ENV HOME=/tmp

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

# Upgrade packaging tools and install only wheels
RUN pip install --upgrade pip setuptools wheel \
&& pip install --no-cache-dir --prefer-binary -r requirements.txt

# Copy your code scripts
COPY lambda_function.py    ${LAMBDA_TASK_ROOT}/
COPY lambda_utils.py       ${LAMBDA_TASK_ROOT}/
COPY utils/                ${LAMBDA_TASK_ROOT}/utils/
COPY data_fetchers/        ${LAMBDA_TASK_ROOT}/data_fetchers/
COPY aggregators/          ${LAMBDA_TASK_ROOT}/aggregators/

# (Optional) Copy config if needed
# COPY config/               ${LAMBDA_TASK_ROOT}/config/

# Set the Lambda handler
CMD ["lambda_function.lambda_handler"]