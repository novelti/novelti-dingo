"""
Copyright 2016 Novelti

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

"""
Novelti DINGO (Data INGestiOn) is a set of Python scripts that simplify data ingestion of existing data into the
Novelti platform. For more information. For more information, please check the README.md file.

"""

import argparse
import sys
import logging
import logging.config
import os
import csv
from datetime import datetime
from datetime import timedelta
import json
import time
import random
import subprocess

VERSION = 0.2

logging.config.fileConfig('logging.conf')
logger = logging.getLogger("simple")

'''
Check whether pycurl is available or not.
'''
PYCURL_AVAILABLE = True
try:
    import pycurl
except:
    PYCURL_AVAILABLE = False



'''
Values of execution modes
'''
MODE_RT='realtime'
MODE_BATCH='batch'
MODE_BOTH='both'
MODE_EMULATE='emulate'

'''
Flag to indicate that we are running the emulated mode.
'''
RUNNING_EMULATION = False

class ConfigArgs:
    '''
    Wrapper class to generate a class with the configuration parameters.
    '''
    def __init__(self, **entries):
        self.__dict__.update(entries)


def ingestRow(args, names, values, timestamp=None):
    '''
    This function ingests one row from a given file using all the configuration indicated in the arguments. If the
    PyCurl module is available this library will be used. Otherwise a call to curl will be done using the command
    console. If timestamp is not indicated, the function simply sends the observations to the API.
    :param args: Configuration arguments.
    :param names: Names of observed variables.
    :param values: Values of the observed variables in the same order as indicated by names.
    :param timestamp: A timestamp in milliseconds when required.
    :return: No returned value.
    '''
    base_string = '%s/v0/ingest/?api_key=%s -X POST -H "Content-Type: application/json" -d "{"event":%s, "timestamp":%d}"'
    if timestamp is None:
        base_string = '%s/v0/ingest/?api_key=%s -X POST -H "Content-Type: application/json" -d "{"event":%s}"'

    observations = {}
    msg = {}
    index = 0
    try:
        for attr in names:
            observations[attr] = float(values[index])
            index += 1
    except Exception as e:
        logger.error("Problem when converting parameters from "+ str(values) + ". Ignore this entry.")
        logger.error(e)
        return
    msg['event'] = observations
    if timestamp is not None:
        request = base_string % (args.url, args.api_key, json.dumps(observations), timestamp)
        msg['timestamp'] = timestamp
    else:
        request = base_string % (args.url, args.api_key, json.dumps(observations))
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(request)
    else:
        if RUNNING_EMULATION:
            # Emulation is on, simply discard it.
            return

        if PYCURL_AVAILABLE:
            logger.debug(msg)
            c = pycurl.Curl()
            c.setopt(c.URL, "%s/v0/ingest/?api_key=%s" % (args.url, args.api_key))
            c.setopt(pycurl.POST, 1)
            c.setopt(pycurl.HTTPHEADER, ['Content-Type: application/json'])
            c.setopt(pycurl.POSTFIELDS, json.dumps(msg))
            # TODO get rid off the pycurl messages
            # Not appealing lambda trick to stop pycurl from console dumping
            # c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
            if logger.isEnabledFor(logging.DEBUG):
                c.setopt(c.VERBOSE, True)
            try:
                c.perform()
            except pycurl.error as error:
                logger.error('An error occurred: ' + str(error))
            c.close()
        else:
            logger.debug("curl -s %s" % request)
            devnull = open('/dev/null', 'w')
            subprocess.call("curl -s %s" % request, shell=True, stdout=devnull)



def get_csv_lines(input_file):
    '''
    Read a file and return the number of lines.
    :param input_file: the path of the file to process
    :return: the number of lines
    '''
    logger.info("Count the number of lines for files %s" % input_file)
    num_lines = 0
    with open(input_file) as input_file:
        for line in input_file:
            num_lines += 1
    logger.info("We finally found %d lines" % num_lines)
    return num_lines


def processBatch(args, delta=timedelta(0)):
    '''
    It takes an existing CSV file and ingests the contained data. The ingestion file is assumed to be sorted.
     The ingestion will stop as soon as a timestamp greater than the current one is found. A delta modifier can be set
     to offset the timestamps from the file. This is used to adequate the the timestamps of an existing dataset to stop
     just before the current time.
    :param args: Configuration arguments.
    :param delta: Delta offset to be applied to the timestamps in the CSV file.
    :return:
    '''
    logger.info("Start batch ingestion with delta %s" % delta)
    with open(args.input_file) as readFileDescriptor:
        logger.info("Start reading %s" % args.input_file)
        csv_reader = csv.reader(readFileDescriptor, delimiter=args.delimiter)
        header = csv_reader.__next__()
        logger.info("File header is %s" % header)
        if args.date_column not in header:
            logger.error("Column date was not found in %s" % header)
            sys.exit(-1)
        date_index = header.index(args.date_column)
        header.pop(date_index)
        value_names = header

        counter = 1
        for row in csv_reader:
            logger.debug(row)
            date_str = row.pop(date_index)
            original_timestamp = datetime.strptime(date_str, args.date_format)
            timestamp = original_timestamp + delta
            if timestamp >= datetime.now():
                logger.info("We pass current time with %s. We stop here." % timestamp)
                break
            assert original_timestamp.weekday() == timestamp.weekday(), '%s vs %s (%d vs %d)' % \
                (original_timestamp, timestamp, original_timestamp.weekday(), timestamp.weekday())
            assert original_timestamp.hour == timestamp.hour, '%s,  %s (%d vs %d)' % \
                (original_timestamp, timestamp, original_timestamp.hour, timestamp.hour)
            latest_ingested_date = timestamp
            ingestRow(args, value_names, row, timestamp.timestamp()*1000)
            time.sleep(args.sleep)
            if counter % 1000 == 0:
                logger.info("Ingested %d entries. Sleep 300 seconds." % counter)
                time.sleep(300)
            counter += 1
            if args.max_entries != -1 and counter >= args.max_entries:
                logger.info("Maximum number of entries %d reached. We stop batch ingestion here." % counter)
                break
        logger.info("The last timestamp ingested in batch was %s" % latest_ingested_date)


def processRT(args, start_date=None):
    '''
    Ingest a dataset in real time respecting the elapsed times indicated in the original dataset. We consider two
     scenarios:
    * No start_date is set. In this scenario, the ingestion starts in the nearest date we can find with the same
    day of the week, hour and minute smaller than the current time.
    * start_date is set. For this scenario, the ingestion starts at the given date.
    :param args: Configuration parameters.
    :param start_date: indicate the first date we should start ingesting from the original dataset.
    :return: Ingestion result, true whether the trace could be ingested or not. And the first ingested date. If no
    suitable date was found for ingestion, the first found date is returned.
    '''

    if start_date is None:
        logger.info("Run real time ingestion looking for the most suitable date...")
    else:
        logger.info("Run real time ingestion starting at %s" % start_date)


    # get the lenght of the file
    total_lines = get_csv_lines(args.input_file)

    ready_to_ingest = False
    processed_lines = 0
    discarded_lines = 0
    with open(args.input_file) as readFileDescriptor:
        logger.info("Start reading %s" % args.input_file)
        csv_reader = csv.reader(readFileDescriptor, delimiter=args.delimiter)
        header = csv_reader.__next__()
        logger.info("File header is %s" % header)
        if args.date_column not in header:
            logger.error("Column date was not found in %s" % header)
            sys.exit(-1)
        date_index = header.index(args.date_column)
        header.pop(date_index)
        value_names = header
        last_time = None
        first_ingested_date = None
        for row in csv_reader:
            date_str = row.pop(date_index)
            timestamp = datetime.strptime(date_str, args.date_format)
            # Check if we can start ingesting from this date
            if not ready_to_ingest:
                if first_ingested_date is None:
                    first_ingested_date = timestamp
                if start_date is None:
                    now = datetime.now()
                    if not args.random_start:
                        ready_to_ingest = (now.weekday() == timestamp.weekday() and now.hour == timestamp.hour)
                    else:
                        ready_to_ingest = (random.random() < ((processed_lines+1)/float(total_lines)))
                elif timestamp == start_date:
                    ready_to_ingest = True
                    logger.info("We start the RT ingestion from the indicated date %s" % timestamp)
                if ready_to_ingest:
                    logger.info("We will start the ingestion on %s" % timestamp)
                else:
                    discarded_lines += 1

            if ready_to_ingest:
                if last_time is not None:
                    time_to_sleep = (timestamp - last_time).seconds
                    logger.info("Sleep %d" % time_to_sleep)
                    time.sleep(time_to_sleep)
                else:
                    # Update the first ingested date
                    first_ingested_date = timestamp

                ingestRow(args, value_names, row)
                last_time = timestamp
            processed_lines += 1
            if processed_lines % 500 == 0:
                logger.info("Processed: %d, discarded: %d" % (processed_lines, discarded_lines))
    logger.info("A total number of %d observations were processed.")
    return ready_to_ingest, first_ingested_date


def processBoth(args):
    '''
    Process a dataset using an initial batch ingestion followed by a real time ingestion. In order to do so, we modify
    the dates indicated in the original dataset to ingest observations from the start of the file until the first
    suitable date. The first suitable date is the first date we find in the original dataset with the same day of the
    week, hour and less minutes than the current date. After that, we compute the elapsed time since the first ingested
    date until the suitable date and set this time as an offset to start the real time ingestion. The real time
    ingestion will be running forever until the program is killed.
    :param args: Configuration arguments.
    :return:
    '''
    with open(args.input_file) as readFileDescriptor:
        logger.info("Start reading %s" % args.input_file)
        csv_reader = csv.reader(readFileDescriptor, delimiter=args.delimiter)
        header = csv_reader.__next__()
        logger.info("File header is %s" % header)
        if args.date_column not in header:
            logger.error("Column date was not found in %s" % header)
            sys.exit(-1)
        date_index = header.index(args.date_column)
        header.pop(date_index)
        value_names = header
        last_time = None
        current_date = datetime.now()
        logger.info("Current time to be considered is %s" % current_date.strftime(args.date_format))
        # Store the closest date in the past
        closest_date = None
        smallest_delta = timedelta(1000)
        numlines = 0
        found_dates = 0
        for line in csv_reader:
            ingest_date = datetime.strptime(line[date_index], args.date_format)
            delta_time = current_date-ingest_date
            if ingest_date.weekday() == current_date.weekday()\
                and ingest_date.hour <= current_date.hour\
                and ingest_date.minute <= current_date.minute\
                and ingest_date < current_date:
                if delta_time < smallest_delta:
                    smallest_delta = delta_time
                    closest_date = ingest_date
                    logger.debug("Update update closest_date with %s" % closest_date)
                    found_dates += 1
                    # Remove this break if you want to continue until the last adequate observation is found.
                    # break
                else:
                    logger.info('Stop at %s' % line[date_index])
                    break
            numlines += 1

    logger.info('The batch process will be set to send %d observations' % numlines)
    logger.info('The final selected date is %s' % closest_date)
    logger.info('From %s to %s there are %d days' % (closest_date, current_date, smallest_delta.days))
    logger.info('Add a delta of %s days to every entry' % smallest_delta.days)
    processBatch(args, timedelta(days=smallest_delta.days))
    processRT(args, start_date=closest_date)
    while True:
        # Repeat forever
        processRT(args)


def process(args):
    '''
    Start processing the ingestion. The argument contains a dictionary with the corresponding
    configuration parameters.
    :param args: Configuration arguments.
    '''
    if args.input_file is None:
        logger.error("No input file specified.")
        sys.exit(-1)
    if not os.path.isfile(args.input_file):
        logger.error("Specified %s is not a file or is not available" % args.input_file)
        sys.exit(-1)
    if args.mode == MODE_RT:
        logger.info('Process %s in real time' % args.input_file)

        logger.info("Start real time ingestion")
        while True:
            processed, starting_date = processRT(args)
            if not processed:
                logger.warning("No adequate date was found to execute the ingestion in real time")
                logger.warning("Force the ingestion to start at %s" % starting_date)
                processRT(args, start_date=starting_date)
            logger.info("We are going to restart the ingestion. Wait for 10 seconds...")
            time.sleep(10)
    elif args.mode == MODE_BATCH:
        logger.info('Process %s in batch mode' % args.input_file)
        processBatch(args)
    elif args.mode == MODE_BOTH:
        logger.info('Process %s using both mode' % args.input_file)
        processBoth(args)
    elif args.mode == MODE_EMULATE:
        logger.info('Process %s using the emulation mode' % args.input_file)
        global RUNNING_EMULATION
        RUNNING_EMULATION = True
        processBoth(args)
    else:
        logger.error('Not recognized mode %s' % args.mode)
        sys.exit(-1)




def load_configuration(file_path):
    '''
    This function reads a JSON file available at file_path and stores the configurations of the available datasets.
    :param file_path: the file path for the json file.
    :return: Configuration dictionary, none if there was any problem.
    '''
    logger.info('Load configurations from %s' % file_path)
    if not os.path.isfile(file_path):
        logger.error('Impossible to read %s' % file_path)
        return None
    datasets = None
    with open(file_path) as reader:
        datasets = json.load(reader)
    logger.info("Configuration loaded.")
    return datasets

def print_configurations(configurations):
    '''
    It prints a dictionary of configurations.
    :param configurations:
    :return:
    '''
    logger.info('\n' + json.dumps(configurations, indent=4, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Novelti DINGO helps you to ingest data into the Novelti platform.')
    parser.add_argument('--file', dest="input_file", action='store', required=False,
                        help='CSV file containing the data to be ingested.')
    parser.add_argument('--mode', dest='mode', action='store', required=False, default='batch',
                        help='The ingestion mode. Four modes are available: batch, realtime, both and emulate.')
    parser.add_argument('--apikey', dest="api_key", action='store', required=False,
                        help='Remote API key.')
    parser.add_argument('--url', dest='url', action='store', required=False, default='input.novelti.io',
                        help='API endpoint URL.')
    parser.add_argument('--delimiter', dest='delimiter', action='store', default=',',
                        help='CSV field delimiter')
    parser.add_argument('--date_column', dest='date_column', action='store', default='date', required=False,
                        help='Name of the column indicating the datetime')
    parser.add_argument('--date_format', dest='date_format', action='store', required=False,
                        help='Date format indicated in the date column')
    parser.add_argument('--sleep', dest="sleep", action="store", default=0, type=int, required=False,
                        help='Time to sleep between posts in seconds')
    parser.add_argument('--max_entries', dest="max_entries", action="store", default=-1, type=int, required=False,
                        help='Maximum number of entries to be considered. For batch ingestion only.')
    parser.add_argument('--dataset', dest='dataset', action='store', default=None, required=False,
                        help='Indicate the dataset to be used. Use all to run all the available datasets.')
    parser.add_argument('--config_file', dest='config_file', action='store', default='config.json', required=False,
                        help='JSON file storing the available datasets and its configuration')
    parser.add_argument('--random_start', dest='random_start', action='store_true', default=False, required= False,
                        help='Makes ingestion to start in a random day when using batch ingestion.')
    parser.add_argument('--list', dest='list', action='store_true', required=False,
                        default=False, help='It prints a list of available configurations.')
    parser.add_argument('--version', dest='version', action='store_true', required=False,
                        default=False, help='Print the current version')

    args = parser.parse_args()

    # Print current version
    if args.version:
        logger.info('Current version: ' + str(VERSION))
        sys.exit(0)

    # List the available configurations
    if args.list:
        datasets = load_configuration(args.config_file)
        if datasets is not None:
            print_configurations(datasets)
            sys.exit(0)
        else:
            sys.exit(-1)

    # Check whether the pycurl module is available or not.
    if PYCURL_AVAILABLE:
        logger.info("PyCurl module was found.")
    else:
        logger.info("PyCurl module was not found.")

    # Proceed to ingest data.
    if args.dataset is not None:
        datasets = load_configuration(args.config_file)
        if datasets is not None:
            if args.dataset not in datasets:
                logger.error('Dataset %s not available in %s config file' % (args.dataset, args.config_file))
                sys.exit(-1)
            # process the dataset
            if 'max_entries' not in datasets[args.dataset]:
                # Add default max_entries
                datasets[args.dataset]['max_entries'] = -1
            if 'random_start' not in datasets[args.dataset]:
                # Add default value
                datasets[args.dataset]['random_start'] = False
            process(ConfigArgs(**datasets[args.dataset]))
        else:
           logger.error('Impossible to load configuration file.')
           sys.exit(-1)

    else:
        logger.info("Read parameters from input line.")
        process(args)
