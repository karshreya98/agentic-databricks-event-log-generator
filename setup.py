from setuptools import setup, find_packages

setup(
    name="process-mining-toolkit",
    version="0.1.0",
    description="Configuration-driven event log builder + AI-assisted enrichment for Databricks",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "requests",
    ],
    python_requires=">=3.10",
)
