#!/usr/bin/env python

import MySQLdb as mdb
import time
import sys
import os
import fnmatch
import argparse
import datetime
import re
import csv
import math
import socket
from fetcher import PropertyReader

##############################################################################################

class TstatParser():
    def __init__(self, tablename):
        configfile = os.path.join(os.environ['TSA_HOME'], 'config/config.ini')
        config = PropertyReader(configfile)
        host = config.get_property_value('DATABASE', 'hostname')
        user = config.get_property_value('DATABASE', 'username')
        passwd = config.get_property_value('DATABASE', 'password')
        db = config.get_property_value('DATABASE', 'db')        

        self.tablename = tablename
        self.extended_table = 'tstat_analyze_extended'
        logdir = os.path.join(os.environ['TSA_HOME'], 'logdir')
        if not os.path.exists(logdir):
            os.makedirs(logdir)
        self.processed_log = os.path.join(logdir, tablename + '_processed.log')
        self.con = None

        try:
            self.con = mdb.connect(host, user, passwd, db);
        except mdb.Error, e:
            print "Error connecting to MySQL %d: %s" % (e.args[0],e.args[1])
            if self.con:    
                self.con.close()
            sys.exit(1)
        

    def truncate(self):
        tablename = self.tablename
        con = self.con
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()
            truncate_stmt = 'DELETE FROM '+ tablename
            num_rows = cur.execute(truncate_stmt)
            con.commit()

            '''
            processed_log = tablename + '_processed.log'
            with open(processed_log, 'w') as f:
                f.write('')
            '''

            print("Number of records deleted: {}".format(cur.rowcount))
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)

    def count(self):
        tablename = self.tablename
        con = self.con
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()
            count_stmt = 'SELECT COUNT(1) FROM '+ tablename
            num_rows = cur.execute(count_stmt)
            res = cur.fetchall()
            print("Number of records: {}".format(res[0][0]))
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)    

    def export_to_csv(self, filename):
        tablename = self.tablename
        con = self.con
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()

            sel_stmt = 'SELECT * FROM '+ tablename

            outdir = os.path.join(os.environ['TSA_HOME'], 'outdir')
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            outfile = os.path.join(outdir, filename)
            print("Exporting data to {}".format(outfile))

            start_time = time.time()
            num_rows = cur.execute(sel_stmt)
            result = cur.fetchall()

            c = csv.writer(open(outfile, "w"))
            columns = [col[0] for col in cur.description]
            header = tuple(columns)
            c.writerow(header)
            for row in result:
                c.writerow(row)

            end_time = time.time()
            print("Table `{}' exported to {} in {} secs".format(tablename, outfile, (end_time - start_time)))
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)    

##########################################################################
    def get_column_names(self, header):
        cols = header.split(',')
        columns = ['local', 'remote']
        excludes = ['BLOCK','BUFFER','CODE','DATE','DEST','DESTIP','FILE','HOST','NBYTES','START','STREAMS','STRIPES','TASKID','TYPE','USER','VOLUME','bandwidth_mbps','data','end_date','host','message','Type','LOCK']
        dups = ['type', 'dest']

        idx = 0
        column_ids = {}
        col_counts = {}
        for col in cols:
            colname = re.sub('[@.#:;]', '_', col)            
            colname = colname.replace('\n', '')
            colname = colname.replace('\r', '')
            '''
            if colname.lower() in col_counts:
                cnt = int(col_counts[colname])
                colname = colname + '_' + str(cnt)
                col_counts[colname.lower()] = cnt + 1
            else:
                col_counts[colname.lower()] = 0
            '''
            if colname == 'interval':
                colname = '_interval'
            if colname not in excludes:
                column_ids[colname] = idx
                if colname not in columns:
                    columns.append(colname)
            idx += 1

        return columns, column_ids

    '''
    '''

    def drop_create(self, logfile, col_types):
        tablename = self.tablename
        con = self.con
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()
            with open(logfile) as f:
                header = next(f)
                cols = header.split(',')
                table_columns = []
                columns, col_ids = self.get_column_names(header)
                idx = 0
                for col in columns:    
                    if col in col_types:
                        table_columns.append(col + ' ' + col_types[col].upper())
                    else:
                        table_columns.append(col + " VARCHAR(100)")
                    idx += 1

                column_list = ",".join(table_columns)                
                drop_table_stmt = "DROP TABLE IF EXISTS {}".format(tablename)
                create_table_stmt = "CREATE TABLE "+ tablename + "(Id INT PRIMARY KEY AUTO_INCREMENT," + column_list + ")"
                #print("{}".format(create_table_stmt))
                cur.execute(drop_table_stmt)
                cur.execute(create_table_stmt)
        except mdb.Error, e:
            print "Error creating table %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)    

    '''
    '''
    def extend_table(self, logfile, col_types):
        tablename = self.tablename
        con = self.con
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()
            table_columns = []
            header = ''
            with open(logfile) as f:
                header = next(f)
            columns, col_ids = self.get_column_names(header)
            idx = 0
            for col in columns:    
                if col in col_types:
                    table_columns.append(col + ' ' + col_types[col].upper())
                else:
                    table_columns.append(col + " VARCHAR(100)")
                idx += 1
            '''
            extended columns
            '''
            table_columns.append("hostpair_key VARCHAR(100)")
            table_columns.append("max_throughput FLOAT")
            table_columns.append("globus_key VARCHAR(100)")
            table_columns.append("num_streams INT")
            table_columns.append("agg_throughput FLOAT")

            extended_table = self.extended_table
            
            column_list = ",".join(table_columns)                
            drop_table_stmt = "DROP TABLE IF EXISTS {}".format(extended_table)
            create_table_stmt = "CREATE TABLE "+ extended_table + "(Id INT PRIMARY KEY AUTO_INCREMENT," + column_list + ")"
            #print("{}".format(create_table_stmt))
            cur.execute(drop_table_stmt)
            cur.execute(create_table_stmt)
            print('Extended table {} created'.format(extended_table))
        except mdb.Error, e:
            print "Error creating extended table %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)    

##########################################################################

    def insert(self, logfile, col_types):
        tablename = self.tablename
        con = self.con
        num_processed = 0
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()

            processed_files = []

            if os.path.exists(self.processed_log):
                with open(self.processed_log) as f:
                    processed_files = f.read().splitlines()

            f = open(self.processed_log, 'a') 
            num_records = 0
            start_time = time.time()
            if logfile not in processed_files:
                num_records = self.__insert_records__(tablename, logfile, cur, col_types)
                con.commit()                
                f.write(logfile + '\n')
                num_processed += 1
            else:
                print("Logfile {} already processed".format(logfile))

            f.close()

            end_time = time.time()
            print("Total time to insert {} records: {} seconds".format(num_records, (end_time - start_time)))
            #print("Number of records deleted: {}".format(cur.rowcount))
        except mdb.Error as e:
            print("Error while inserting records: {}".format(e))
            print("Number of files processed: {}".format(num_processed))
            if con:    
                con.close()
            sys.exit(1)    
        
##########################################################################
    def __insert_records__(self, tablename, logfile, cur, col_types):
        #print "Connecting to MySql..."
        print("Inserting data for log: {}".format(logfile))
        with open(logfile) as f:
            header = next(f)
            columns, col_ids = self.get_column_names(header)            

            pattern = re.compile('dtn[0-9]+[-]*[a-z]*\.nersc\.gov')
            data = []
            col_list = []
            val_list = []
            for line in f:
                values = line.split(',')
                idx = 0
                rec = {}
                
                flow_type = values[col_ids['type']]
                '''
                hostaddr1 = ''
                if flow_type == 'flow':
                    hostaddr1 = values[col_ids['source']]
                elif flow_type == 'gridftp':
                    hostaddr1 = values[col_ids['host']]

                hostaddr2 = ''
                if flow_type == 'flow':
                    hosts = [values[col_ids[k]] for k in col_ids if k.startswith('dest')]
                    for host in hosts:
                        hostaddr2 = host
                elif flow_type == 'gridftp':
                    host_ips = [values[col_ids[k]] for k in col_ids if k.startswith('dest')]
                    try:
                        for host_ip in host_ips:
                            if host_ip != '':
                                hostaddr2 = socket.gethostbyaddr(host_ip)[0]
                    except socket.herror:
                        print('Unable to resolve IP ({}) to hostname!'.format(host_ip))

                '''
                hostaddr1 = values[col_ids['source']]
                hostaddr2 = values[col_ids['dest']]
                if pattern.match(hostaddr1):
                    rec['local'] = hostaddr1
                    rec['remote'] = hostaddr2
                else:
                    rec['remote'] = hostaddr1
                    rec['local'] = hostaddr2                        
                    
                idx = 0
                row = []
                if flow_type == 'flow':
                    for col in columns:
                        if col == 'local':
                            if pattern.match(hostaddr1):
                                rec['local'] = hostaddr1
                            else:
                                rec['local'] = hostaddr2
                        elif col == 'remote':
                            if pattern.match(hostaddr1):
                                rec['remote'] = hostaddr2
                            else:
                                rec['remote'] = hostaddr1
                    #elif col == 'source':
                    #    rec['source'] = hostaddr1
                    #elif col == 'dest':
                    #    rec['dest'] = hostaddr2                    
                    #elif col == 'start':
                    #    if flow_type == 'flow':
                    #        rec['start'] = int(values[col_ids['start_0']])
                    #    elif flow_type == 'gridftp':
                    #        tm = datetime.datetime.strptime(values[col_ids['start']], '%Y%m%d%H%M%S.%f')
                    #        epoch = datetime.datetime.utcfromtimestamp(0)
                    #        time_since_epoch = (tm - epoch).total_seconds()
                    #        rec['start'] = int(round(time_since_epoch))
                    #    #print("{} Start: {}".format(flow_type, rec['start']))
                        else:
                            val = values[col_ids[col]].replace('\n', '').replace('\r', '')
                            if val == '':
                                rec[col] = None
                            elif col in col_types:
                                if col_types[col] == 'int':                                
                                    #print("{} {}".format(col, val))
                                    rec[col] = int(val)
                                elif col_types[col] == 'float':
                                    rec[col] = float(val)
                                elif col_types[col] == 'datetime':
                                    tm = datetime.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%fZ')
                                    rec[col] = tm.strftime('%Y-%m-%d %H:%M:%S')
                                else:                            
                                    rec[col] = val
                            else:
                            #print("{}: {}, {}".format(col, len(val), val))
                                rec[col] = val
                        row.append(rec[col])
                        idx += 1
                    data.append(tuple(row))
    

            for col in columns:
                col_list.append(col)
                val_list.append("%s")
            colnames = ','.join(col_list)
            valstr = ','.join(val_list) 
            insert_stmt = 'insert into ' + tablename + '(' + colnames + \
                ') values (' + valstr + ')' 
            #print(insert_stmt)
            #print(data[0])
            #print(len(col_list), len(val_list))
            #for d in data:
            #    print(len(d))
            total_recs = len(data)
            start_time = time.time()
            if total_recs > 0:
                subset_size = 1000.0
                niter = int(math.ceil(total_recs/subset_size)) # partition total data into chunks
                start = 0
                for i in range(niter):
                    end = int((i + 1) * subset_size)
                    if end > total_recs:
                        end = total_recs
                    data_subset = data[start:end]
                    num_rows = cur.executemany(insert_stmt, data_subset)
                    #con.commit()
                    #print("Number of rows inserted: {}".format(num_rows))
                    start = end                    
                
            end_time = time.time()
            print("Time to insert: %s seconds" % (end_time - start_time))
            
            return total_recs

##################################
    def index(self):
        tablename = self.tablename
        index_stmt1 = 'CREATE INDEX tstat_local_remote ON ' + tablename + ' (local, remote)'
        index_stmt2 = 'CREATE INDEX tstat_globus ON ' + tablename + ' (source, dest, _timestamp)'
        con = self.con
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()
            start_time = time.time()
            cur.execute(index_stmt1)
            cur.execute(index_stmt2)
            end_time = time.time()
            print("Indexes created in {} secs".format((end_time - start_time)))
             
        except mdb.Error, e:
            print "Error creating index %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)

##################################
    def extend(self, logfile):
        tablename = self.tablename
        con = self.con

        header = ''
        with open(logfile) as f:
            header = next(f)
        columns, col_ids = self.get_column_names(header) 

        extended_columns = ['hostpair_key', 
                            'max_throughput',
                            'globus_key',
                            'num_streams',
                            'agg_throughput']
        columns.extend(extended_columns)
        colnames = ','.join(columns)

        result_columns = []
        for col in columns:
            if col == 'hostpair_key' or col == 'max_throughput':
                result_columns.append('T.' + col)
            else:
                result_columns.append('B.' + col)
        subquery_columns = ','.join(result_columns)
        
        try:
            #con = mdb.connect(self.host, self.user, self.passwd, self.db);
            cur = con.cursor()

            extended_table = self.extended_table

            throughput_query_table = '(SELECT local, remote, concat(local, ":", remote) as hostpair_key,' \
                + ' max(throughput_Mbps) as max_throughput FROM ' + tablename + ' GROUP BY local, remote)'

            globus_query_table = '(SELECT source, dest, _timestamp, concat(source, ":", dest, ":", _timestamp)' \
                + ' as globus_key, count(1) as num_streams, sum(throughput_Mbps) as agg_throughput FROM ' \
                + tablename + ' GROUP BY source, dest, _timestamp)'

            select_stmt = 'SELECT ' + subquery_columns + ' FROM ' \
                + ' (SELECT A.*, G.globus_key, G.num_streams, G.agg_throughput FROM ' + tablename \
                + ' A, ' + globus_query_table + ' G WHERE G.source=A.source and G.dest=A.dest and' \
                + ' G._timestamp=A._timestamp) B, ' + throughput_query_table + ' T WHERE'\
                + ' B.local=T.local and B.remote=T.remote'

            insert_stmt = 'INSERT INTO ' + extended_table + '(' + colnames + ') ' + select_stmt
            #print(insert_stmt)
            print("Inserting into extended table...")

            start_time = time.time()
            num_rows = cur.execute(insert_stmt)
            con.commit()
            end_time = time.time()
            print("Table `{}' inserted with {} rows in {} secs".format(extended_table, num_rows, (end_time - start_time)))
             
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            if con:    
                con.close()
            sys.exit(1)

    def close_conn(self):
        if self.con:
            self.con.close()

##########################################################################
def main(args):
    tablename = args.__dict__['tablename']
    logfile = args.__dict__['logfile']

    coltypes_file = 'column.types'
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

    parser = TstatParser(tablename)

    parser.drop_create(logfile, col_types)
    if 'reload' in args.__dict__:
        if args.__dict__['reload'] == True:
            parser.extend_table(logfile, col_types)
            with open(self.processed_log, 'w') as f:
                f.write('')            
        
    parser.insert(logfile, col_types)
    parser.index()
    parser.extend(logfile)
    parser.export_to_csv()
    parser.close_conn()
    
##################################
def is_dir(file):
    """
    check if a specified file/path is a directory or not

    :param file: file/path to check, type: string
    :return: if the file is a directory or not, type: boolean
    """
    if os.path.exists(file):
        return os.path.isdir(file)
    else:
        print("Path {} does not exist!".format(file))
        sys.exit(-1)

def get_file_list(path):
    """
    get the list of files in a directory

    :param path: directory name, type: string
    :return: list of files, type: list of string
    """
    files = []
    pattern = "log_tcp_complete"
    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, pattern):
            files.append(os.path.join(root, filename))
    print("Num-files: {}".format(len(files)))
    return files

##################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="",
                                     prog="tstat_analyze",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.set_defaults(func=main)
    parser.add_argument('-s','--hostname', help='mysql host', default='localhost')
    parser.add_argument('-d','--db', help='tstat database', default='tstat')
    parser.add_argument('-u','--user', help='username', default='root')
    parser.add_argument('-c','--passwd', help='password', default='root123')    
    parser.add_argument('-p','--port', help='mysql port', default=27017)
    parser.add_argument('-t','--tablename', help='tstat table', required=True)
    parser.add_argument('-l','--logfile', help='tstat elastic dump file', required=True)
    parser.add_argument('-r','--reload', dest='reload', action='store_true')    

    args = parser.parse_args()
    args.func(args)
