"""
Setup script for SparkForge package.
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), "README.md")
    if os.path.exists(readme_path):
        with open(readme_path, "r", encoding="utf-8") as f:
            return f.read()
    return "A powerful data pipeline builder for Apache Spark and Databricks"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
    if os.path.exists(requirements_path):
        with open(requirements_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return []

setup(
    name="sparkforge",
    version="0.1.0",
    author="Odos Matthews",
    author_email="odosmatthew@gmail.com",
    description="A powerful data pipeline builder for Apache Spark and Databricks",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/eddiethedean/sparkforge",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Software Development :: Build Tools",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyspark==3.2.4",
        "pydantic>=1.8.0",
        "delta-spark>=1.2.0,<2.0.0",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.950",
            "isort>=5.10.0",
        ],
        "docs": [
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=1.0.0",
            "myst-parser>=0.17.0",
        ],
    },
    keywords=[
        "spark",
        "databricks", 
        "pipeline",
        "etl",
        "data-engineering",
        "data-lakehouse",
        "bronze-silver-gold",
        "delta-lake",
        "big-data",
        "data-processing"
    ],
    project_urls={
        "Bug Reports": "https://github.com/eddiethedean/sparkforge/issues",
        "Source": "https://github.com/eddiethedean/sparkforge",
        "Documentation": "https://sparkforge.readthedocs.io/",
    },
    include_package_data=True,
    zip_safe=False,
)
