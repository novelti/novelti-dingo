# Introduction

Novelti DINGO (Data INGestiOn) is a set of Python scripts that simplify 
data ingestion of existing data into the Novelti platform. Many times
existing data has to be imported into the platform for offline analysis.
In other scenarios, data may be required to be ingested in real time to 
emulate a realistic scenario or just for demonstration purposes.

## Input file

DINGO is designed to process CSV files with a header 
indicating the date, value and name of observed variables. Each
row in the file must contain the date and the values to be ingested 
in the system. For example:

```
date,var1,var2,var3
2016/01/01 10:00,1.0,2.0,3.0
2016/01/01 10:10,2.0,2.3,3.1
2016/01/01 10:15,3.0,3.0,4.1
...
```
 
## Ingestion modes

DINGO currently offers three ingestion modes:
* **Batch** Existing data is ingested row after row using the date
 indicated in the input file.
* **Real time** This mode permits the user to observe how the system 
would behave in a real time scenario. In order to do so, the system 
ingests each row waiting the time elapsed between observations. The
first observation to be ingested is the first timestamp found that 
shares the same day of the week and hour than the current time. Data 
before that date is ignored.
* **Both** This mode ingests the data discarded by the real time mode
in a batch manner and then starts a real time ingestion.
 

# How to use DINGO

This section, describes how to use DINGO.

## Prerequisites

* **Python version** DINGO is designed to run using **Python 3.5**. 
This means that versions lower than 3.5 are not supported. 
* **Dependencies** DINGO uses [libCurl](https://curl.haxx.se/libcurl).
We support two different Curl installations: those accessible through the 
command line and PyCurl. DINGO will look for PyCurl installations 
in your system. If no installation is found, it will use the command 
line. In the second case, be sure that curl is accessible through your
operating system command line.
  
  * **PyCurl** [PyCurl](http://pycurl.io/) is a wrapper library of Curl 
  for Python. In Ubuntu-like systems: `sudo apt-get install python-pycurl`
  
  * **Command line** To check if curl is available in your system type: 
  `curl --version`
The output shall be something like this:
```
curl 7.47.0 (x86_64-pc-linux-gnu) libcurl/7.47.0 GnuTLS/3.4.10 zlib/1.2.8 libidn/1.32 librtmp/2.3
Protocols: dict file ftp ftps gopher http https imap imaps ldap ldaps pop3 pop3s rtmp rtsp smb smbs smtp smtps telnet tftp 
Features: AsynchDNS IDN IPv6 Largefile GSS-API Kerberos SPNEGO NTLM NTLM_WB SSL libz TLS-SRP UnixSockets 
```
If you cannot see this output please install curl in your system. In
Ubuntu like operating systems type:
```
sudo apt-get install curl
```

## How to run
To see a complete list of the parameters used by the script type:
```
python3 dingo.py --help
usage: dingo.py [-h] [--file INPUT_FILE] [--mode MODE] [--apikey API_KEY]
                [--url URL] [--delimiter DELIMITER]
                [--date_column DATE_COLUMN] [--date_format DATE_FORMAT]
                [--sleep SLEEP] [--dataset DATASET]
                [--config_file CONFIG_FILE] [--list] [--version]

Novelti DINGO helps you to ingest data into the Novelti platform.

optional arguments:
  -h, --help            show this help message and exit
  --file INPUT_FILE     CSV file containing the data to be ingested.
  --mode MODE           The ingestion mode. Three modes are available: batch,
                        realtime or both.
  --apikey API_KEY      Remote API key.
  --url URL             API endpoint URL.
  --delimiter DELIMITER
                        CSV field delimiter
  --date_column DATE_COLUMN
                        Name of the column indicating the datetime
  --date_format DATE_FORMAT
                        Date format indicated in the date column
  --sleep SLEEP         Time to sleep between posts in seconds
  --dataset DATASET     Indicate the dataset to be used.
  --config_file CONFIG_FILE
                        JSON file storing the available datasets and its
                        configuration
  --list                It prints a list of available configurations.
  --version             Print the current version

```

Every configuration parameter is self-explained. To start an ingestion
we have to indicate the input file, how to read the date, the Novelti
api key, the URL and the execution mode (batch, realtime or both). These 
configurations can be read from a configuration file. We describe how
to run the program using both approaches.

### In-line configuration

Let's assume the following CSV file to be ingested:
```
date,temp,pot,vib
16/12/2015 19:33:31,30.62,67.9,3.9
16/12/2015 19:33:34,30.69,68.01,3.83
16/12/2015 19:33:37,30.69,69.75,3.9
16/12/2015 19:33:40,30.75,69.56,3.88
...
```
This file has three variables (temp, pot and vib) and the observation
date. In order to run in batch mode and send the data to the novelti
API located at input.novelti.io we execute the following:

```
python3 dingo.py --file=myfile.csv --url=input.novelti.io 
--apikey=12345 --date_format="%d/%m/%Y %H:%M:%S" --delimiter="," \
--date_column="date" --sleep=1
```
Where:
* **file** is the name of the dataset file.
* **url** is the Novelti API URL where the data is sent.
* **apikey** is the API key regarding the stream to be used.
* **date_format** is the format describing the datetime. Refer to the
[Python documentation](https://docs.python.org/3/library/datetime.html)
 for a complete description of the available formats.
* **delimiter** character used as a column delimiter.
* **sleep** number of seconds to wait after sending a row of observations.
This parameter is optional, by default there is no wait.

### File configuration

In this approach the configuration parameters are taken from a JSON
file that contains the same parameters. For example:
```
{
"example1" : {
    "input_file":"myfile.csv",
    "api_key": "60d331d3-e1e7-4dca-8081-463467c9c40b",
    "mode": "batch",
    "url": "input.novelti.io",
    "delimiter":",",
    "date_column": "date",
    "date_format": "%d/%m/%Y %H:%M:%S",
    "sleep":0
  },
"example2" : {
    "input_file":"myfile.csv",
    "api_key": "713d331d3-2127-451a-5781-463467c9c40b",
    "mode": "realtime",
    "url": "input.novelti.io",
    "delimiter":",",
    "date_column": "date",
    "date_format": "%d/%m/%Y %H:%M:%S",
    "sleep":0
  }
}
```
The example above contains two configurations for the same dataset
(myfile.csv) that will be sent to different streams using different
batch and real time modes. We can list the configurations available
in a file:
```
python3 dingo.py --config_file=config.json --list
```
And execute any of them:
```
python3 dingo.py --config_file=config.json --dataset=example2
```

# Troubleshooting
DINGO comes with a `logging.conf` file, be sure to have this file in the
same folder `dingo.py` is allocated.

