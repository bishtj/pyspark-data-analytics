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
                  'pytest==7.2.0',
                  'pytest-xdist==3.0.2',
                  'pytest-cov==4.0.0',
                  'pytest-html==3.2.0',
                  'pytest-flake8==1.1.1',
                  'setuptools==40.6.3',
                  'wheel',
        ],
    }
)
