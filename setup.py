#!/usr/bin/env python3
"""
Setup script for Confluence Visualization Project
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="confluence-visualization",
    version="0.1.0",
    author="Confluence Analysis Team",
    description="A comprehensive toolkit for analyzing, visualizing, and managing Confluence instances",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "optional": [
            "whoosh>=2.7.4",
            "playwright>=1.30.0",
            "pandas>=1.5.0",
            "seaborn>=0.12.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "flake8>=5.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "mypy>=0.991",
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "confluence-fetch=fetch_data:main",
            "confluence-analyze=semantic_analysis:main",
            "confluence-viz=viz:main",
            "confluence-explore=explore_clusters:main",
        ],
    },
)