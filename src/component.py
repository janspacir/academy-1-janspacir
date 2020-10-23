'''
Template Component main class.

'''

import csv
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from kbc.client_base import HttpClientBase
from kbc.env_handler import KBCEnvHandler
from kbc.csv_tools import CachedOrthogonalDictWriter

# configuration variables
KEY_PRINT_ROWS = 'print_rows'
KEY_DEBUG = 'debug'

MANDATORY_PARS = []
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.4.7'

DATA_FOLDER = Path('/data')
SOURCE_FILE_PATH = DATA_FOLDER.joinpath('in/tables/input.csv')
RESULT_FILE_PATH = DATA_FOLDER.joinpath('out/tables/output.csv')

class Component(KBCEnvHandler):
    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)

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

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa

        print('Running...')
        state = self.get_state_file()
        print('Last update: %s' % state.get('last_update'))

        with open(self.get_input_tables_definitions()[0].full_path, 'r') as input:
            reader = csv.DictReader(input)
            new_columns = reader.fieldnames
            # append row number col
            new_columns.append('row_number')
        self.configuration.write_table_manifest(
                result_file_path = os.path.join(self.tables_out_path, 'output.csv'),
                destination='out.c-academy-1-janspacir.output',
                primary_key=['row_number'],
                incremental=True)
      with CachedOrthogonalDictWriter(new_columns, result_file_path = os.path.join(self.tables_out_path, 'output.csv')) as writer:
                for index, l in enumerate(reader):
                    # print line
                    if params.get(KEY_PRINT_ROWS):
                        print(f'Printing line {index}: {l}')
                    # add row number
                    l['row_number'] = index
                    writer.writerow(l)

                    # move to folder
        #shutil.move(source_file_path, os.path.join(source_file_path, 'source_file_path'))
        #result_file_path = os.path.join(self.tables_out_path, 'output.csv')
      
        
        state['last_update'] = datetime.utcnow().timestamp()
        self.write_state_file(state)


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
