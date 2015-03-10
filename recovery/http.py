import json
import urllib2
import logging
import ConfigParser
import errno
import os
import time
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
            logging.error('No -c or --config option specified, \
for more use -h', exc_info=True)
            exit(1)
        else:
            config_opt = option.config
        # Load config file
#        result = None
        try:
            config = ConfigParser.RawConfigParser()
            config.read(config_opt)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            logging.error('Failed to open config file %s' % (config_opt),
                          exc_info=True)
            exit(1)

        # Get config option
        http_port = config.get('http', 'port')
        http_location = config.get('http', 'location')
        graphite_dir = config.get('main', 'graphite_dir')
        hosts = json.loads(config.get('main', 'hosts'))
        # get mirror whisper file from remote HTTP instance
#        rslt = []
        full_time = float()
        empty_hosts = []
        full_start = time.time()
        for host in hosts:
            copy_start = time.time()

            whisper = self.wsp_file.replace(graphite_dir, "")

            endpoint = 'http://' + host + ':' + http_port + '/' + http_location + '/' + whisper
            try:
                src_file = urllib2.urlopen(endpoint, timeout=5)
                self.sc.incr('SuccessHost')
                wsp_buffer = src_file.read()

                if os.path.exists(os.path.dirname(self.wsp_file)) is False:
                    os.makedirs(os.path.dirname(self.wsp_file))

                dst_file = open(self.wsp_file, 'wb')
                dst_file.write(wsp_buffer)
                print self.wsp_file
                dst_file.close()
                copy_elapsed = (time.time() - copy_start)
                self.sc.timing('SuccessTime', copy_elapsed * 1000)
#            except urllib2.HTTPError, e:
#                      if e.code == 404:
#                          print "404"
            except IOError as e:
                if e.errno == errno.ENOENT:
                    empty_hosts.append(host)
                    self.sc.incr('EmptyHost')
                    empty_elapsed = (time.time() - copy_start)
                    self.sc.timing('EmptyTime', empty_elapsed * 1000)
                    pass
            self.sc.incr('FullCount')
            full_time = (time.time() - full_start)
            self.sc.gauge('QueueTasksPut', self.qcountall)
            self.sc.timing('FullTime', full_time * 1000)
#            try:
#                result = json.load(src_file)
#                src_file.close()
#            except Exception:
#                      pass
#
#            if result['success']:
#                rslt.append(True)
#            else:
#                rslt.append(False)
#
#        return rslt.count(True)
