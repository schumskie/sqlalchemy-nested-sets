from setuptools import setup, find_packages

setup(
    package_dir={"":"src"},
    packages=find_packages(where='src', include=['nested_sets*'], exclude=['tests*']),
    include_package_data=True,
    install_requires=[
        "setuptools>=61.0",
        "sqlalchemy>=2.0.15",
    ]
)
