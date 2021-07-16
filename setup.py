from setuptools import setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="reagan",
    version="1.8.5",
    description="Package for streamlining credentials, connections, and data flow",
    url="https://github.com/schustda/reagan",
    author="Douglas Schuster",
    author_email="douglas.schuster@carat.com",
    packages=["reagan"],
    zip_safe=False,
    install_requires=requirements,
)

