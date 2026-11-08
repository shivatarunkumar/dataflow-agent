# Flex template launcher image. Python version must match the Beam worker SDK container.
# Pinned release (the Makefile passes the same value); 20261006+ releases fail to unpack. Override: make docker BASE_IMAGE=...
ARG BASE_IMAGE=gcr.io/dataflow-templates-base/python312-template-launcher-base:flex_templates_base_image_release_20260901_RC00
FROM ${BASE_IMAGE}

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

COPY requirements.txt setup.py main.py ./
COPY pipeline pipeline
COPY contracts contracts

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -e .
