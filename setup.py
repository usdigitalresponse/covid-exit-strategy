import os

from setuptools import find_packages
from setuptools import setup


with open(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), "requirements.txt"), "r"
) as requirements_file:
    install_requires = requirements_file.read()

setup(
    name="covid-exit-strategy",
    description="This is a repository for producing data for the Covid Exit Strategy website.",
    long_description_content_type="text/markdown",
    version="0.0.0",
    author="Lucas Merrill Brown",
    license="GNU GPLv3",
    install_requires=install_requires,
    include_package_data=True,
    packages=find_packages(),
)
