import setuptools

setuptools.setup(
   name="mrms-api",
   version="1.0.0",
   description="Provides a Python interface to ingesting and reading MRMS GRIB2 files from HTTPS servers (NCEP, Iowa State) and local files (i.e. LDM, NOAAPort, EDEX).",
   url="https://github.com/HumphreysCarter/mrms-api",
   author="Carter Humphreys",
   packages=setuptools.find_packages(include=["mrms*"])
)
