from pathlib import Path

import setuptools


def get_requirements_from_file(file_path: Path | str) -> list[str]:
    with open(file_path) as f:
        requirements = f.read().strip().split("\n")
    return requirements


with open("README.md", "r") as f:
    long_description = f.read()


requirements = get_requirements_from_file("requirements.txt")


setuptools.setup(
    name="elipse_api",
    version="1.0",
    author="Elipse - PUCRS",
    author_email="p.pagnussat@edu.pucrs.br",
    long_description_content_type="text/markdown",
    url="https://github.com/macedoti13/HydraulicSystemForecasting",
    packages=setuptools.find_packages(),
    python_requires=">=3.11",
    install_requires=requirements,
)
