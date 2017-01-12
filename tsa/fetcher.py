import ConfigParser
import paramiko
import os
import sys

class PropertyReader():
    def __init__(self, properties_file):
        self.__properties_file__ = properties_file
    
    def get_properties(self, section):
        properties = ConfigParser.ConfigParser()
        properties.read(self.__properties_file__)
        items = dict(properties.items(section))
        return items

    def get_property_value(self, section, key):
        properties = ConfigParser.ConfigParser()
        properties.read(self.__properties_file__)
        value = properties.get(section, key)
        return value

class RemoteAccess():
    def __init__(self):
        configfile = os.path.join(os.environ['TSA_HOME'],'config/config.ini')
        config = PropertyReader(configfile)
        self.hostname = config.get_property_value('CONNECTION', 'hostname')
        self.username = config.get_property_value('CONNECTION', 'username')
        self.auth = config.get_property_value('CONNECTION', 'auth_mechanism')
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.auth == 'password':
            self.passwd = config.get_property_value('AUTH', 'password')
        elif self.auth == 'key':
            key_file = os.path.expanduser(config.get_property_value('AUTH', 'key'))
            self.key = paramiko.RSAKey.from_private_key_file(key_file)
        else:
            print('Authentication mechanism {} not implemented!'.format(self.auth))
            sys.exit(0)

        self.query_type = config.get_property_value('QUERY', 'type')
        self.query = config.get_property_value('QUERY', 'query')
        self.tags = config.get_property_value('QUERY', 'tags')
        self.limit = config.get_property_value('QUERY', 'limit')
        self.fields = config.get_property_value('QUERY', 'fields')
        #self.delimiter = config.get_property_value('QUERY', 'delimiter')
        self.index_prefix = config.get_property_value('QUERY', 'index_prefix')
        
    def connect(self):
        if self.auth == 'password':
            self.ssh.connect(self.hostname, username=self.username, password=self.passwd)
        elif self.auth == 'key':
            self.ssh.connect(self.hostname, username=self.username, pkey=self.key)
        
        print('Connected to {}'.format(self.hostname))

    def query_es(self, dt, outfile):
        print('Querying Elasticsearch')
        suffix = dt.replace('-', '.')
        if self.index_prefix:
            index_prefix = self.index_prefix
        else:
            index_prefix = 'logstash-' + suffix

        extra_args = ''
        if self.limit:
            extra_args += ' -m ' + self.limit

        command = "es2csv -q '" + self.query + "' -i " + index_prefix + extra_args \
            +" -o " + outfile
        
        stdin,stdout,stderr=self.ssh.exec_command(command)
        outlines=stdout.readlines()
        resp=''.join(outlines)
        print(resp)

    def copy_file(self, outfile):
        transfer = self.ssh.open_sftp()
        esdump_dir = os.path.join(os.environ['TSA_HOME'], 'esdump')
        if not os.path.exists(esdump_dir):
            #print('{} does not exist, created'.format(esdump_dir))
            os.makedirs(esdump_dir)
        dest_file = os.path.join(esdump_dir, os.path.basename(outfile))
        transfer.get(outfile, dest_file)
        transfer.close()
        print('{} transferred to {}'.format(outfile, dest_file))
        
        return dest_file

    def close_conn(self):
        self.ssh.close()
    
if __name__ == '__main__':
    remote_access = RemoteAccess()
    remote_access.connect()
    remote_access.query_es('2016-07-21', 'test_out.csv')
    remote_access.copy_file('test_out.csv')
    remote_access.close_conn()
