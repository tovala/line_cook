#!/bin/bash
set -e

CONTAINER_RUNTIME=$1
echo "Using $CONTAINER_RUNTIME runtime in build.sh"

# Generate the Dockerfiles from the templates.
# shellcheck source=/dev/null
source "../../../.venv/bin/activate"
python3 ../generate-dockerfiles.py
deactivate

# Build the base image.
${CONTAINER_RUNTIME} build -f ./Dockerfiles/Dockerfile.base -t amazon-mwaa-docker-images/airflow:3.0.6-base ./


# Build the derivatives.
for dev in "True" "False"; do
    for build_type in "standard" "explorer" "explorer-privileged"; do
        dockerfile_name="Dockerfile"
        tag_name="3.0.6"

        if [[ "$build_type" != "standard" ]]; then
            dockerfile_name="${dockerfile_name}-${build_type}"
            tag_name="${tag_name}-${build_type}"
        fi

        if [[ "$dev" == "True" ]]; then
            dockerfile_name="${dockerfile_name}-dev"
            tag_name="${tag_name}-dev"
        fi

        IMAGE_NAME="amazon-mwaa-docker-images/airflow:${tag_name}"
        ${CONTAINER_RUNTIME} build -f "./Dockerfiles/${dockerfile_name}" -t "${IMAGE_NAME}" ./
    done
done
