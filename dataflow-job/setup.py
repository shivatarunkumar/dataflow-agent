"""Packages the pipeline code so Dataflow workers can import it."""

import setuptools

setuptools.setup(
    name="gcs-to-bq-ingest",
    version="1.0.0",
    packages=setuptools.find_packages(include=["pipeline", "pipeline.*"]),
    install_requires=["PyYAML>=6.0"],
)
