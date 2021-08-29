import os
import warnings
import xarray as xr

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
			self.__load_with_cfgrib(data_only, extent)

		# Catch invalid engine
		else:
			raise ValueError('Only pygrib and cfgrib are supported for reading grib files.')

		# Return datset
		return self.dataset

	def get_point_value(self, lat, lon, method='nearest'):
		"""
		Returns a point value for given latitude, longitude
		"""
		# Get values at grid point for the lat/lon pair
		ds = self.dataset.sel(latitude=lat, longitude=lon, method=method)

		return ds

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

	def __load_with_cfgrib(self, data_only, extent=None):
		# Load datset with xarray and cfgrib engine
		ds = xr.load_dataset(self.path, engine='cfgrib')

		# Cut to extent
		if extent != None:
			minLat, maxLat, minLon, maxLon = extent
			ds = ds.sel(latitude=slice(maxLat, minLat), longitude=slice(minLon, maxLon))

		# Get only data
		if data_only == True:
			self.dataset = ds.paramId_0.values

		# Get as xarray dataset
		else:
			# Rename to use product name
			#ds = ds.rename({'paramId_0':self.product})

			# Set datset var
			self.dataset = ds
