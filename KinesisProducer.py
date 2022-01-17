from netCDF4 import Dataset
from osgeo import gdal
import pandas as pd
import os
import csv
import json
import boto3
import uuid

"""
  convert nc file to csv
"""
def convertNCToCSV(downloadFile,csvFile):
    
    nc = Dataset(downloadFile, mode='r')
    df = pd.DataFrame()
    
    for var in nc.variables.keys():
        
        variable = gdal.open("NETCDF:{0}:{1}".format(downloadFile, var))
        variable_value = variable.ReadAsArray().flatten('C')
        df[var] = pd.Series(variable_value)
        
    df.to_csv(csvFile, encoding='utf-8', index = false)

"""
  read n records as one batch
""" 
def chunkit(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

"""
  read csv file and sent records to kinesis
"""
def produceRecords(csv_file):
    
    kinesis = boto3.client("kinesis",region_name='ap-southeast-1')
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        uuidstr = str(uuid.uuid1())
        records = chunkit([{"PartitionKey": uuidstr, "Data": json.dumps(row)} for row in reader], 500)
        for chunk in records:
            kinesis.put_records(StreamName="kinesis-datastream", Records=chunk)


def parseArgs(argv):

    file_name = ''
    
    try:
        opts, args = getopt.getopt(argv,"hf:",['help',"file_name="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-f", "--file_name"):
            file_name = arg

    if (file_name == ''):
        usage()
        sys.exit(2)
    else:
        return file_name
                           
def main(argv):
    
    downloadFile = parseArgs(argv)
    (file,filesuffix) = os.path.splitext(downloadFile)
    csv_file = file + ".csv"
    convertNCToCSV(downloadFile,csvFile)
    produceRecords(csvFile)

if __name__ == "__main__":
    exit(main(sys.argv[1:]))
