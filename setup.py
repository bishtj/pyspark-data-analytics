import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh]

url = "https://github.com/bishtj/pyspark-data-analytics.git"

version = "0.0.1"

setuptools.setup(
    name="pyspark_data_analytics_package",
    version=version,
    author="Jaikrit Bisht",
    author_email="jaikrit_bisht@hotmail.com",
    description="Provides sample functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=url,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    extras_require={
        'build': ['pip',
                  'pytest==6.0.1',
                  'pytest-xdist==2.0.0',
                  'pytest-cov==2.10.1',
                  'pytest-html==2.1.1',
                  'pytest-flake8==1.0.7',
                  'setuptools==40.6.3',
                  'wheel',
                  'pandas==latest'
        ],
    }
)
