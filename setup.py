#!/usr/bin/env python3
"""
Futures Data Manager
Self-contained package for downloading and updating futures price series using Interactive Brokers
"""
from setuptools import setup, find_packages
import os

# Read README file
readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as fh:
        long_description = fh.read()
else:
    long_description = __doc__

# Read requirements
requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
if os.path.exists(requirements_path):
    with open(requirements_path, "r", encoding="utf-8") as fh:
        requirements = [line.strip() for line in fh.readlines() if line.strip() and not line.startswith("#")]
else:
    requirements = [
        "ib_insync>=0.9.70",
        "pandas>=1.5.0", 
        "pyarrow>=10.0.0",
        "numpy>=1.24.0",
        "loguru>=0.7.0",
        "pydantic>=2.0.0",
        "python-dateutil>=2.8.0",
        "pytz>=2022.1",
        "pydantic-settings>=2.0.0",
        "typing-extensions>=4.0.0",
    ]

setup(
    name="futures-data-manager",
    version="1.0.0",
    author="Futures Data Manager Team",
    author_email="contact@example.com",
    description="Self-contained package for downloading and updating futures price series using Interactive Brokers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/futures-data-manager",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0", 
            "black>=22.0",
            "flake8>=5.0",
            "mypy>=1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "futures-data-manager=futures_data_manager.main:main",
        ],
    },
    package_data={
        "futures_data_manager": [
            "config/*.csv",
            "config/*.json", 
        ],
    },
    include_package_data=True,
    zip_safe=False,
)