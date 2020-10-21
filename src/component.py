'''
Template Component main class.

'''

import csv
import logging
import os
import shutil
import sys
from datetime import datetime

from kbc.client_base import HttpClientBase
from kbc.env_handler import KBCEnvHandler

# configuration variables
KEY_PRINT_HELLO = 'print_hello'

# #### Keep for debug
KEY_DEBUG = 'debug'

MANDATORY_PARS = []
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

        # setup client
        auth_header = {"Authorization": "test_token"}
        url = "https://run.mocky.io/v3/"
        self.client = HttpClientBase(base_url=url, max_retries=10, backoff_factor=0.3,
                                     status_forcelist=(429, 503, 500, 502, 504), default_http_header=auth_header)

        json_resp = self.client.get(self.client.base_url+'0dd8b338-8268-4cc3-80fa-f6d7c45a3d55')
        print(json_resp)


    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        self.tables_out_path

        # get state file
        last_state = self.get_state_file()
        date_updated = last_state.get("last_update")
        print("last_update")
        # store new state
        now_str = str(datetime.now().date())
        self.write_state_file({"last_update": now_str})

        table_defs = self.get_input_tables_definitions()
        logging.info(f'Available input files {[t.file_name for t in table_defs]}')
        first_table = table_defs[0]
        logging.info(f'Columns defined in  {first_table.file_name} '
                     f'manifest: {first_table.manifest["columns"]}')
        # get first table path
        source_file_path = first_table.full_path
        result_file_path = os.path.join(self.tables_out_path, 'output.csv')

        PARAM_PRINT_LINES = True
     
        print('Running...')
        with open(SOURCE_FILE_PATH, 'r') as input, open(RESULT_FILE_PATH, 'w+', newline='') as out:
        reader = csv.DictReader(input)
        new_columns = reader.fieldnames
        # append row number col
        new_columns.append('row_number')
        writer = csv.DictWriter(out, fieldnames=new_columns, lineterminator='\n', delimiter=',')
        writer.writeheader()
        for index, l in enumerate(reader):
        # print line
        if PARAM_PRINT_LINES:
            print(f'Printing line {index}: {l}')
        # add row number
        l['row_number'] = index
        writer.writerow(l)


        # ## store as sliced files
        # move to folder
        # shutil.move(source_file_path, os.path.join(source_file_path, 'source_file_path'))
        # create sliced tables - removes headers from files and creates manifest
        # self.create_sliced_tables(folder_name=result_file_path, pkey=['row_number'],
        #                          incremental=True)

        # # Create manifest for a single file (non-sliced)
        self.configuration.write_table_manifest(file_name=result_file_path, primary_key=['row_number'],
        incremental=True)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
