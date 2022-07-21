import os
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

class iastate:

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
        
    def filterToTimeRange(self, start, end, inclusive=True):
        """
        doc string
        """
        updatedList = []
        
        for file in self.files:
            fileName = file[file.rfind('_')+1:file.rfind('.grib2')]
            fileTime = datetime.strptime(fileName, '%Y%m%d-%H%M%S')
            
            if inclusive:
                if fileTime >= start and fileTime <= end:
                    updatedList.append(file)
            else:
                if fileTime > start and fileTime < end:
                    updatedList.append(file)
                    
        self.files = updatedList

    def download_file(self, args):
        """
        doc string
        """
        try:
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
        except:
            return None
