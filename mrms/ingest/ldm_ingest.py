import glob

class ldm:

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
