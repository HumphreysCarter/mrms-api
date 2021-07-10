import os
import glob
import gzip
import shutil
import warnings
import requests
import xarray as xr
import urllib.request
from datetime import datetime
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
from pathlib import Path

def get_http_files(url, ext=''):
    """
    Returns list file listing from HTTP/HTTPS directory
    """
    # Get HTML page
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    # Get file listing
    files = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext) and 'latest' not in node.get('href')]

    return files

class ncep_ingest:
    def __init__(self, dim, vars):
        """
        doc string
        """
        # Get available files
        if isinstance(vars, list):
            self.files = []
            for var in vars:
                url = f'https://mrms.ncep.noaa.gov/data/{dim}/{var}/'
                self.files += get_http_files(url=url, ext='gz')
        else:
            url = f'https://mrms.ncep.noaa.gov/data/{dim}/{vars}/'
            self.files = get_http_files(url=url, ext='gz')

    def download(self, path, extract=True, mp=True, overwrite=False, cpu_pool=None):
        """
        doc string
        """
        localList = []

        # Find files that already exist
        if overwrite == True:
            downloadList = self.files
        else:
            downloadList = []
            for serverPath in self.files:
                # Set local file path
                fileName  = serverPath[serverPath.rfind('/')+1:]
                localPath = f'{path}/{fileName}'.replace('.gz', '')

                # Check if file exists, add to download list if False
                if Path(localPath).exists() == False:
                    downloadList.append(serverPath)
                else:
                    localList.append(localPath)

        # Create argument list
        args = [(file, path, extract) for file in downloadList]

        # Download with multiprocessing
        if mp == True:
            # Get CPU pool
            if cpu_pool == None or cpu_pool > cpu_count():
                cpu_pool = cpu_count()

            # Create multiprocessing processor pool
            pool = Pool(processes=cpu_pool)
            localList += pool.map(self.download_file, args)
            pool.close()

        # Download as single process
        else:
            for arg in args:
                file = self.__download_file(arg)
                localList.append(file)

        # Update file list with local paths
        self.files = localList

    def download_file(self, args):
        """
        doc string
        """
        # Get args if set
        serverPath, localPath, extract = args

        # Set local file path
        fileName  = serverPath[serverPath.rfind('/')+1:]
        localPath = f'{localPath}/{fileName}'

        # Download file from server
        urllib.request.urlretrieve(serverPath, localPath)

        # Extract Gzip file
        if extract:
            with gzip.open(localPath, 'rb') as f_in:
                localPath = localPath.replace('.gz', '')
                with open(localPath, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Remove compressed file
            os.remove(f'{localPath}.gz')

        return localPath


class iastate_ingest:
    def __init__(self, date, vars):
        """
        doc string
        """
        # Get available files
        if isinstance(vars, list):
            self.files = []
            for var in vars:
                url = f'https://mtarchive.geol.iastate.edu/{date:%Y}/{date:%m}/{date:%d}/mrms/ncep/{var}/'
                self.files += get_http_files(url=url, ext='gz')
        else:
            url = f'https://mtarchive.geol.iastate.edu/{date:%Y}/{date:%m}/{date:%d}/mrms/ncep/{vars}/'
            self.files = get_http_files(url=url, ext='gz')

    def download(self, path, extract=True, mp=True, overwrite=False, cpu_pool=None):
        """
        doc string
        """
        localList = []

        # Find files that already exist
        if overwrite == True:
            downloadList = self.files
        else:
            downloadList = []
            for serverPath in self.files:
                # Set local file path
                fileName  = serverPath[serverPath.rfind('/')+1:]
                localPath = f'{path}/MRMS_{fileName}'.replace('.gz', '')

                # Check if file exists, add to download list if False
                if Path(localPath).exists() == False:
                    downloadList.append(serverPath)
                else:
                    localList.append(localPath)

        # Create argument list
        args = [(file, path, extract) for file in downloadList]

        # Download with multiprocessing
        if mp == True:
            # Get CPU pool
            if cpu_pool == None or cpu_pool > cpu_count():
                cpu_pool = cpu_count()

            # Create multiprocessing processor pool
            pool = Pool(processes=cpu_pool)
            localList += pool.map(self.download_file, args)
            pool.close()

        # Download as single process
        else:
            for arg in args:
                file = self.__download_file(arg)
                localList.append(file)

        # Update file list with local paths
        self.files = localList

    def download_file(self, args):
        """
        doc string
        """
        # Get args if set
        serverPath, localPath, extract = args

        # Set local file path
        fileName  = serverPath[serverPath.rfind('/')+1:]
        localPath = f'{localPath}/MRMS_{fileName}'

        # Download file from server
        urllib.request.urlretrieve(serverPath, localPath)

        # Extract Gzip file
        if extract:
            with gzip.open(localPath, 'rb') as f_in:
                localPath = localPath.replace('.gz', '')
                with open(localPath, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Remove compressed file
            os.remove(f'{localPath}.gz')

        return localPath

class ldm_ingest:
    def __init__(self, ldm_dir, vars):
        """
        doc string
        """
        # Get available files
        files = glob.glob(f'{ldm_dir}/*')

        # Filter by desired products
        mrms_files = []
        if isinstance(vars, list):
            for var in vars:
                mrms_files += [file for file in files if var in file]
        else:
            mrms_files = [file for file in files if vars in file]

        # Set ingest vars
        self.files = mrms_files


class dataset:

	def __init__(self, path, format='NCEP'):
		"""
		Initializes an MRMS dataset object from file path. An attempt to automatically get product information and valid time based on naming convention of the file will be made, will warn if this fails and use GRIB reader for valid time and product. Supported naming conventions are 'NCEP' (data from https://mrms.ncep.noaa.gov/data/) and 'LDM' (NOAAPort and EDEX servers).
		"""
		# Check if path exists
		self.__check_files_exists(path)

		# Try to initialize vars
		try:
			self.path    = path
			self.product = self.__get_product_name(format.upper())
			self.valid   = self.__get_valid_time(format.upper())
		except:
			self.path    = path
			self.product = None
			self.valid   = None
			warnings.warn('Invalid File Format: Unable to discern product and valid time from filename, will set values to None. Run load_dataset() to import with grib reader.')

	def load_dataset(self, engine='pygrib', data_only=False, extent=None):
		"""
		Returns an xarray dataset of the MRMS dataset. Optionally to speed up performance, if data_only is set to True, a numpy data array of the MRMS dataset is returned. The value of MRMSdatset.dataset is also set to the return value.

		Supported GRIB reader engines are pygrib (default) and cfgrib. pygrib offers faster performance, especially when only returning a numpy data array. cfgrib offers better xarray dataset integration.
		"""
		# Read GRIB file with pygrib module
		if engine.lower() == 'pygrib':
			self.__load_with_pygrib(data_only, extent)

		# Read GRIB file with cfgrib module
		elif engine.lower() == 'cfgrib':
			self.__load_with_cfgrib(data_only)

		# Catch invalid engine
		else:
			raise ValueError('Only pygrib and cfgrib are supported for reading grib files.')

		# Return datset
		return self.dataset

	def __check_files_exists(self, path):
		if os.path.isfile(path) == False:
			raise FileNotFoundError(path)

	def __get_product_name(self, format):
		if format == 'LDM':
			return self.path[self.path.rfind('F000')+5:self.path.rfind('-')]
		if format == 'NCEP':
			if '_00' in self.path:
				return self.path[self.path.rfind('MRMS_')+5:self.path.rfind('_00')]
			elif 'scale' in self.path:
				return self.path[self.path.rfind('MRMS_')+5:self.path.rfind('_scale')]
			else:
				return None
		else:
			return None

	def __get_valid_time(self, format):
		if format == 'LDM':
			time = self.path[self.path.rfind('/'):self.path.find('Z_')]
			date = self.path[self.path.rfind('.'):len(self.path)-2]
			return datetime.strptime(time+date, '/%H%M.%Y%m%d')
		if format == 'NCEP':
			time = self.path[self.path.rindex('_')+1:self.path.rindex('.')]
			return datetime.strptime(time, '%Y%m%d-%H%M%S')
		else:
			return None

	def __load_with_pygrib(self, data_only, extent=None):
		# Import pygrib module
		import pygrib

		# Open GRIB file
		grbs = pygrib.open(self.path)

		# MRMS files contain only one GRIB message, so will return first GRIB message
		grb = grbs[1]

		# Get only data
		if data_only == True:
			self.dataset = grb.values

		# Get as xarray dataset
		else:
			# Get data, lat/lons, and valid date
			if extent != None:
				minLat, maxLat, minLon, maxLon = extent
				data, lats, lons = grb.data(lat1=minLat, lat2=maxLat, lon1=minLon, lon2=maxLon)
				valid = grb.validDate
			else:
				data = grb.values
				lats, lons = grb.latlons()
				valid = grb.validDate

			# Build xarray datset
			ds = xr.Dataset(data_vars=dict(data=(['x', 'y'], data)), coords=dict(longitude=(['x', 'y'], lons), latitude=(['x', 'y'], lats), time=valid), attrs=dict(description=self.product),)

			# Rename to use product name
			ds = ds.rename({'data':self.product})

			# Set datset var
			self.dataset = ds

        # Close the grib file
        grbs.close()

	def __load_with_cfgrib(self, data_only):
		# Load datset with xarray and cfgrib engine
		ds = xr.load_dataset(self.path, engine='cfgrib')

		# Get only data
		if data_only == True:
			self.dataset = ds.paramId_0.values

		# Get as xarray dataset
		else:
			# Rename to use product name
			ds = ds.rename({'paramId_0':self.product})

			# Set datset var
			self.dataset = ds
