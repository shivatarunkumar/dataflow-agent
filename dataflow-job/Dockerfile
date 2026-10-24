# Flex template launcher image. Python version must match the Beam worker SDK container.
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base

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
