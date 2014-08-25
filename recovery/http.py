import sys
import json
import urllib
import urllib2
import logging
import socket
from recovery.configparse import ParseArgs

class HttpRecovery:
    def __init__(self,
                 statsd,
                 wsp_file,
                 qcountall):
        self.wsp_file = wsp_file
        self.qcountall = qcountall
        self.sc = statsd

    def http_get(self):

        parseargs = ParseArgs()
        option = parseargs.parse_args()

        if option.config is None:
            logging.error('No -c or --config option specified, for more use -h',
                          exc_info=True)
            exit(1)
        else:
            config_opt = option.config
        # Load config file
        result = None
        try:
            config = ConfigParser.RawConfigParser()
            result = config.read(config_opt)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            logging.error('Failed to open config file %s' % (config),
                          exc_info=True)
            exit(1)

        # Get config option
        http_port = int(config.get('http', 'port'))
        http_url = config.get('http', 'url')
        http_location = config.get('http', 'location')
        http_keepalive = config.getboolean('http', 'keepalive')
        recovery = config.get('main', 'recovery')
        graphite_dir = config.get('main', 'graphite_dir')
        hosts = json.loads(config.get('main', 'hosts'))
        carbon_creates = json.loads(config.get('main', 'carbon_creates'))
        # get mirror whisper file from remote HTTP instance
        rslt = []
        for host in self.hosts:
            copy_start = time.time()

            whisper = self.wsp_file.replace(self.graphite_dir,"")

            endpoint = 'http://' + host + ':' + self.http_port + '/' + self.http_location + '/' + whisper
            print endpoint
            try:
                src_file = urllib2.urlopen(endpoint, timeout=5)
                wsp_buffer = src_file.read()
                 
                if os.path.exists(os.path.dirname(self.wsp_file)) == False:
                   os.makedirs(os.path.dirname(self.wsp_file))
                    
                dst_file = open(self.wsp_file, 'wb')
                dst_file.write(wsp_buffer)
                dst_file.close()
                
            except Exception as err:
                      raise
            try:
                result = json.load(src_file)
                src_file.close()
            except Exception:
                      raise
            
            if result['success']:
                rslt.append(True)
            else:
                rslt.append(False)

        return rslt.count(True)
