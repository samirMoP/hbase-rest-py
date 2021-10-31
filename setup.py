import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hbase-rest-py",
    version="0.0.1",
    author="Samir Ahmic",
    author_email="ahmic.samir@gmail.com",
    description="HBase client based on HBase REST",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/samirMoP/hbase-rest-py",
    project_urls={
        "Bug Tracker": "https://github.com/samirMoP/hbase-rest-py/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Apache License Version 2",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "hbase"},
    packages=setuptools.find_packages(where="hbase"),
    python_requires=">=3.6",
)