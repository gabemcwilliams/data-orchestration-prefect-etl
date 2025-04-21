# -----------------------------------------------------------------------------
# 🔹 Prefect.io - Build Container Image, Lint, Test, Push
# -----------------------------------------------------------------------------
#
#   This GitHub Actions workflow automates the process of building
#    the Prefect.io container image, running linting and tests,
#    and pushing the resulting image to a private Docker registry.
#
# -----------------------------------------------------------------------------
#   Triggering the Workflow:
# -----------------------------------------------------------------------------
#
#-  **Manual Trigger**: Workflow can be manually triggered via GitHub UI (`workflow_dispatch`).
#-  **Push Trigger**: The workflow also runs on every push to the `main` branch, specifically
#    for changes under the `.github/` and `src/` directories.
#
# -----------------------------------------------------------------------------
#   How the Workflow Works:
# -----------------------------------------------------------------------------
#
#   1. **Code Checkout**:
#      - The workflow starts by checking out the latest code from the repository.
#
#   2. **Dynamic Docker Tagging**:
#      - It generates a dynamic Docker image tag based on the **timestamp** to uniquely identify the build.
#      - The `latest` tag is also applied to keep the most recent build easily identifiable.
#
#   3. **Docker Image Build**:
#      - The Docker image is built using a multi-stage Dockerfile. The build process is split into several stages:
#         - **Base**: Sets up the base OS and environment.
#         - **Build**: Installs both production and test dependencies.
#         - **Test**: Runs unit tests and evaluates the results.
#         - **Production**: Prepares the image for production, excluding test dependencies.
#
#   4. **Push Docker Image**:
#        - Once the image is successfully built and tested, it's pushed to the private Docker

#          registry with both the **timestamp tag** and **latest tag**.
#
#   5. **Pull Latest Image**:
#        - To ensure you're always using the latest image,
#           use the `--pull always` flag with `docker-compose up -d` (ie. docker compose up -d --pull always )
#           to force Docker to pull the latest image from the registry, even if the image already exists locally.
#
# -----------------------------------------------------------------------------

# #############################################################################
# Stage 1: Base Stage - Shared environment setup for both test and prod
# #############################################################################
FROM python:3.12-slim AS base

# -----------------------------------------------------------------------------
# Labels
# -----------------------------------------------------------------------------
LABEL maintainer="builds@netcov.com"
LABEL version="0.6"
LABEL description="Custom Prefect Server & Worker Image - Includes both Test and Prod Stages"

# -----------------------------------------------------------------------------
# Set non-interactive mode for apt installs
# -----------------------------------------------------------------------------
ENV DEBIAN_FRONTEND=noninteractive

# -----------------------------------------------------------------------------
# Install essential dependencies
# -----------------------------------------------------------------------------

RUN apt-get update
RUN apt-get install -y openssh-server
RUN apt-get install -y iputils-ping
RUN apt-get install -y net-tools
RUN apt-get install -y nano
RUN apt-get install -y postgresql-client
RUN apt-get install -y curl

# Clean up apt cache to reduce image size
RUN rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# Working Directory Setup
# -----------------------------------------------------------------------------
WORKDIR /prefect

COPY src /prefect/src

# #############################################################################
# Stage 2: Build Stage - Install both test and production dependencies
# #############################################################################
FROM base AS build

# -----------------------------------------------------------------------------
# Copy shared requirements file and install dependencies
# -----------------------------------------------------------------------------
COPY requirements.txt /prefect/requirements.txt
RUN pip install --no-cache-dir -r /prefect/requirements.txt

# -----------------------------------------------------------------------------
# Expose necessary ports for potential test containers
# -----------------------------------------------------------------------------
EXPOSE 4200 8080

# #############################################################################
# Stage 3: Test Stage - Runs tests and evaluates results
# #############################################################################
FROM build AS test

# -----------------------------------------------------------------------------
# Copy test-specific requirements and test files
# -----------------------------------------------------------------------------
COPY requirements_tests.txt /prefect/requirements_tests.txt
COPY tests /prefect/tests
COPY pytest.ini /prefect/tests

# -----------------------------------------------------------------------------
# Run tests and evaluate results
# -----------------------------------------------------------------------------
RUN pytest tests/ --maxfail=3 --disable-warnings -q || (python3 /prefect/tests/eval_pytest_results.py && exit 1)

# #############################################################################
# Stage 4: Production Stage - Exclude test dependencies and prepare for production
# #############################################################################
FROM build AS production

# -----------------------------------------------------------------------------
# Copy everything from the build stage (excluding test dependencies)
# -----------------------------------------------------------------------------
COPY --from=build /prefect /prefect

# -----------------------------------------------------------------------------
# Remove test-specific dependencies and files
# -----------------------------------------------------------------------------
RUN rm -rf /prefect/tests /prefect/docs

# -----------------------------------------------------------------------------
# Install production dependencies and set entry point
# -----------------------------------------------------------------------------
RUN pip install --no-cache-dir -r /prefect/requirements.txt
