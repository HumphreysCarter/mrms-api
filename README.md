# MRMS API
A Python interface to ingesting and reading MRMS GRIB2 files from HTTPS servers (NCEP, Iowa State) and local files (i.e. LDM, NOAAPort, EDEX).

## Installation

### Installing from Source
Install using the latest source code which can be obtained from the GitHub repository, [HumphreysCarter/mrms-api](https://github.com/HumphreysCarter/mrms-api). Stable releases can also be downloaded from the  [releases listing](https://github.com/HumphreysCarter/mrms-api/releases) in the repo.
```
git clone https://github.com/HumphreysCarter/mrms-api
cd mrms-api
python setup.py install
```

## Dependencies

If using Anaconda the dependencies can be installed from the environment.yml, which will install them to a new environment called `mrms_env`.

    conda env create -f environment.yml
    conda activate mrms_env

Otherwise, dependencies can be installed with pip or any package manager.

### Required
* Python 3.7 or higher
* xarray
* requests
* bs4

### Optional
* cfgrib or pygrib

## Usage

### Ingest Tools

Supported data feeds for ingest are the real-time [NCEP server](https://mrms.ncep.noaa.gov/data/), the [Iowa State Mtarchive](https://mtarchive.geol.iastate.edu), in addition to local files.

#### Ingesting from Data Feeds

##### NCEP

```
from mrms.ingest import ncep

ingest = ncep(dim='2D', vars=['ReflectivityAtLowestAltitude', 'PrecipRate'])
```

##### Iowa State Ingest


```
from mrms.ingest import iastate

ingest = iastate(date=datetime(2020, 12, 17), vars='SeamlessHSR')
```

##### Local Files
```
from mrms.ingest import ldm

ingest = ldm(ldm_dir='/local/path/to/mrms/files/', vars='PrecipRate')
```

#### Downloading Files
Only needed if files need to be downloaded from a web server. ```path``` to the directory where files will be downloaded to and is required; ```extract=True```, ```overwrite=False```, ```mp=True```, and ```cpu_pool=None``` are optional. ```mp``` enables the use of multiprocessing to download files simultaneously, ```cpu_pool``` is the number of cores, ```overwrite``` skips files that are already downloaded, and ```extract``` will decompress any .gz files.
```
ingest.download('/local/path/to/download/data/to/')
```

#### Retrieving Files
Local file paths of each file is then returned in a list.
```
ingest.files
```
### Dataset Tools

#### Loading a Dataset
MRMS GRIB2 files can be loaded into an xarray dataset using the dataset module. Either [cfgrib](https://github.com/ecmwf/cfgrib) or [pygrib](https://github.com/jswhit/pygrib) will need to be installed in order to use this function.

Dataset is initialized with a file path to a single MRMS GRIB2 file. The optional ```format='NCEP'``` flag can be passed to attempt to automatically gather metadata from the filename. Supported file naming convections are ```NCEP``` (ex: MRMS_PrecipRate_00.00_20210902-005200.grib2) and ```LDM``` (ex: 0052Z_F000_PrecipRate-YAUP02_KWNR_020052_12150163.grib2.2021090200)
```
from mrms.io import dataset

ds = dataset('/path/to/mrms.grib2')
```

To load the dataset, pass the desired engine (cfgrib or pygrib) along with the optional ```data_only=False``` and ```extent=None``` flags. Pygrib offers faster performance, especially when only returning a numpy data array. cfgrib offers better xarray dataset integration. ```data_only``` is set to False by default, but if set to True, a numpy data array of the MRMS dataset is returned. The ```extent``` flag can be used to cut the dataset to a lat-lon box by passing a tuple that contains ```(minLat, maxLat, minLon, maxLon)```, if None then the entire dataset is returned.
```
ds.load_dataset(engine='cfgrib')
```

#### Gridded Dataset Reterval
The gridded dataset can be reterived from the return value of ```load_dataset``` or by calling ```ds.dataset``` variable. The object returned is a xarray dataset or a numpy data array depending on what ```data_only``` was set to.
```
data = ds.dataset
```

#### Point Value Reterval
Point values from the dataset for a given latitude and logntiude pair can be retreived with the ```get_point_value``` function.
```
pt = ds.get_point_value(35.20, 248.36)
```
