from fetcher import RemoteAccess
from loader import TstatParser
import argparse
import os
import sys
import re
import glob

def process(args):
    day = args.__dict__['day']
    if args.__dict__['flow'] != None:
        flowtype = args.__dict__['flow']
    else:
        flowtype = 'flow'
    remote_access = RemoteAccess()
    remote_access.connect()
    csv_file = 'tstat_analyze_' + day.replace('-', '_') + '.csv'
    remote_access.query_es(day, csv_file)
    esdump_file = remote_access.copy_file(csv_file)
    remote_access.close_conn()

    coltypes_file = os.path.join(os.environ['TSA_HOME'],'resources/column.types')
    col_types = {}
    with open(coltypes_file) as f:
        for line in f:
            kv = line.split('=')
            colname = re.sub('[@.#:]', '_', kv[0]) 
            colname = colname.replace('\n', '')
            colname = colname.replace('\r', '')
            if colname == 'interval':
                colname = '_interval'
            col_types[colname] = kv[1].replace('\n', '')

    tablename = 'tstat_analyze_' + day.replace('-', '_')
    parser = TstatParser(tablename, flowtype)
    parser.drop_create(esdump_file, col_types)
    if 'reload' in args.__dict__:
        if args.__dict__['reload'] == True:
            parser.extend_table(esdump_file, col_types)
            processed_log = parser.processed_log
            with open(processed_log, 'w') as f:
                f.write('')            
        
    parser.insert(esdump_file, col_types)
    parser.index()
    parser.extend(esdump_file, col_types)
    parser.close_conn()

def load(args):
    if args.__dict__['flow'] != None:
        flowtype = args.__dict__['flow']
    else:
        flowtype = 'flow'
    coltypes_file = os.path.join(os.environ['TSA_HOME'],'resources/column.types')
    col_types = {}
    with open(coltypes_file) as f:
        for line in f:
            kv = line.split('=')
            colname = re.sub('[@.#:]', '_', kv[0]) 
            colname = colname.replace('\n', '')
            colname = colname.replace('\r', '')
            if colname == 'interval':
                colname = '_interval'
            col_types[colname] = kv[1].replace('\n', '')

    tstatdump = glob.glob(args.tstatdump)
    tstat_files = []
    for dump in tstatdump:
        if os.path.isdir(dump):
            #print('FILES in {}: {}'.format(dump, os.listdir(dump)))
            for f in os.listdir(dump):
                fname = os.path.join(dump, f)
                if os.path.isfile(fname):                    
                    tstat_files.append(fname)
        else:
            tstat_files.append(dump)

    #print("Tstat_files: {}".format(tstat_files))
    parser = TstatParser('dummy', flowtype)
    reload = False
    if 'reload' in args.__dict__:
        if args.__dict__['reload'] == True:
            reload = True

    for esdump_file in tstat_files:
        tablename = os.path.basename(esdump_file).split('.')[0]
        tablename = tablename.replace('-', '_')
        if reload == True:
            parser.extend_table(esdump_file, col_types)
            processed_log = parser.processed_log
            with open(processed_log, 'w') as f:
                f.write('')            
            reload = False
        parser.set_table(tablename)
        parser.drop_create(esdump_file, col_types)
        parser.insert(esdump_file, col_types)
        parser.index()
        parser.extend(esdump_file, col_types)
    
    parser.close_conn()

def export(args):
    tablename = args.__dict__['tablename']
    outfile = args.__dict__['outfile']
    if outfile == None:
        outfile = tablename + '.csv'
    parser = TstatParser(tablename)
    parser.export_to_csv(outfile)
    parser.close_conn()
    
def truncate(args):
    tablename = args.__dict__['tablename']
    parser = TstatParser(tablename)
    parser.truncate()
    parser.close_conn()

def count(args):
    tablename = args.__dict__['tablename']
    parser = TstatParser(tablename)
    parser.count()
    parser.close_conn()

##################################

def _addProcessParser(subparsers):
    parser_worker = subparsers.add_parser('process',
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          help=""" Process tstat records from elasticsearch and puts them into a MySQL table """)

    parser_worker.set_defaults(func=main, action="process")
    parser_worker.add_argument('-d','--day', help='date from the elasticsearch data, format=YYYY-MM-DD', required=True)
    parser_worker.add_argument('-r','--reload', dest='reload', action='store_true')    
    parser_worker.add_argument('-f','--flow', help='flow type from Elastic')

def _addLoadParser(subparsers):
    parser_worker = subparsers.add_parser('load',
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          help=""" Load elasticsearch documents into a MySQL table """)

    parser_worker.set_defaults(func=main, action="load")
    parser_worker.add_argument('-d','--tstatdump', help='elasticsearch dump dir/files', required=True)
    parser_worker.add_argument('-r','--reload', dest='reload', action='store_true')    
    parser_worker.add_argument('-f','--flow', help='flow type from Elastic')

def _addTruncateParser(subparsers):
    parser_worker = subparsers.add_parser('truncate',
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          help=""" Truncates a MySQL table """)

    parser_worker.set_defaults(func=main, action="truncate")
    parser_worker.add_argument('-t','--tablename', help='table name', required=True)    

def _addCountParser(subparsers):
    parser_worker = subparsers.add_parser('count',
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          help=""" Counts records in a MySQL table """)

    parser_worker.set_defaults(func=main, action="count")
    parser_worker.add_argument('-t','--tablename', help='table name', required=True)    

def _addExportParser(subparsers):
    parser_worker = subparsers.add_parser('export',
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                          help=""" Export records from a MySQL table into csv file""")

    parser_worker.set_defaults(func=main, action="export")
    parser_worker.add_argument('-t','--tablename', help='table name', required=True)    
    parser_worker.add_argument('-o','--outfile', help='name of the output file (default is <tablename>.csv)')    

##################################
def main():
    parser = argparse.ArgumentParser(description="",
                                     prog="tstat_analyze",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    '''
    parser.set_defaults(func=main)
    #parser.add_argument('-t','--tablename', help='tstat table', required=True)
    #parser.add_argument('-l','--logfile', help='tstat elastic dump file', required=True)
    parser.add_argument('-d','--day', help='date from the elasticsearch data, format=YYYY-MM-DD', required=True)
    parser.add_argument('-r','--reload', dest='reload the mysql tables', action='store_true')    

    args = parser.parse_args()
    args.func(args)
    '''

    subparsers = parser.add_subparsers()
    _addProcessParser(subparsers)
    _addLoadParser(subparsers)
    _addTruncateParser(subparsers)
    _addCountParser(subparsers)
    _addExportParser(subparsers)

    args = parser.parse_args()
    if len(args.__dict__) == 0:
        print("Parser not yet implemented for this phase")
        sys.exit(0)

    action = args.action

    if action == 'process':
        process(args)
    elif action == 'load':
        load(args)
    elif action == 'export':
        export(args)
    elif action == 'truncate':
        truncate(args)
    elif action == 'count':
        count(args)
    else:
        print("Invalid action!")

##################################

if __name__ == '__main__':
    main()
