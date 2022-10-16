from distutils.core import setup

with open("requirements/requirements.in") as f:
    requirements = [line for line in f.read().split("\n") if line]

setup(
    name="dbsvc",
    version="0.0.1",
    description="Very basic service for CRUD operations on SQL DB",
    author="Matthew Shaw",
    url="https://github.com/MattRickS/dbsvc",
    packages=["dbsvc"],
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "dbsvc = dbsvc.__main__:cli",
        ],
    },
)
