#!/usr/bin/env python
from datetime import date
from urllib3 import PoolManager
from sys import exit
import json

# Global Variables with defaults
hostname = 'http://localhost:9200'
days_to_keep = 7
index_glob = 'filebeat' # Note only index name of form <INDEX-GLOB>-YYYY-MM-DD is supported!
index_list = []
log_level = 'error'
log_file = 'elasticprune.log'

# Functions
def get_current_date():
    return date.today()

def process_index_date(index):
    logging.debug('Taken index as  ' + index + ' and extracting date' )
    index_date_str = index.replace(index_glob + '-', '')
    try:
        year, month, day = index_date_str.split('.')
        logging.debug('Processed date as  ' + year + ', ' + month + ', ' + day )
        return date(int(year), int(month), int(day))
    except:
        logging.error('Something wrong with the file glob. Unable to unpack tuples. Exiting!' )
        exit(1)

def get_indices(index_glob):
    http = PoolManager()
    try:
        request = http.request(method='GET', url= hostname + '/_cat/indices?format=json' )
    except:
        logging.error('Error in calling Elasticsearch. Exiting!' )
        exit(1)
    if request.status != 200:
        logging.error('Non sucessful status for getting indices. Exiting!' )
        exit(1)
    else:
        dict_data = json.loads(request.data.decode('utf-8'))
        logging.debug('Successfully got the indices data')
        logging.debug(str([ entry['index'] for entry in dict_data if index_glob in entry['index']]))
        return [ entry['index'] for entry in dict_data if index_glob in entry['index']]

def delete_index(index):
    http = PoolManager()
    try:
        request = http.request(method='DELETE', url= hostname + '/' + index )
    except:
        logging.error('Error in calling Elasticsearch. Exiting!' )
        exit(1)
    if request.status != 200:
        logging.error('Non sucessful status for deleting index ' + index + '..Skipping!' )
        return 1
    else:
        return 0

def delete_old_indices():
    for index in get_indices(index_glob):
        logging.debug('Processing for index ' + index)
        try:
            delta = get_current_date() - process_index_date(index)
        except TypeError:
            logging.error('Cannot process the dates. Exiting!' )
        if delta.days > days_to_keep:
            delete_index(index)
            logging.debug('Deleted index ' + index)
        else:
            logging.debug('Did not delete the index as it is below the threshold date -- ' + index)

if __name__ == '__main__':
    from argparse import ArgumentParser
    import logging

    # Parsing command line Options
    parser = ArgumentParser(description='Prune Elasticsearch indices')
    parser.add_argument('-H', '--hostname', help="Elasticsearch Host with port (default is http://localhost:9200)", action='store')
    parser.add_argument('-d', '--days_to_keep', help="Days to keep the data (default is 7)", type=int, action='store')
    parser.add_argument('-i', '--index', help="Index search pattern (default is filebeat)", action='store')
    parser.add_argument('-l', '--log_level', help="Log level for the script (default is error)", action='store')
    parser.add_argument('-f', '--log_file', help="File to log for the script (default is elasticprune.log)", action='store')
    args = parser.parse_args()

    if args.hostname:
        hostname = args.hostname
    if args.days_to_keep:
        days_to_keep = args.days_to_keep
    if args.index:
        index_glob = args.index
    if args.log_level:
        log_level = args.log_level
    if args.log_file:
        log_file = args.log_file

    # Logging setup for the script
    try:
        logging.basicConfig(filename=log_file, level=getattr(logging, log_level.upper()), format='%(asctime)s %(levelname)s %(message)s')
    except AttributeError:
        logging.basicConfig(filename=log_file, level=getattr(logging, log_level.DEBUG), format='%(asctime)s %(levelname)s %(message)s')
        logging.error('Invalid log level given. Defaulting to debug mode!')
    logging.debug('Taken hostname as ' + hostname + ', days to keep as ' + str(days_to_keep) + ', index glob as ' + index_glob)
    
    # Main Function
    delete_old_indices()
    exit(0)

