import json
import urllib2
import logging
import ConfigParser
import errno
import os
import sys
import time
from recovery.configparse import ParseArgs

log = logging.getLogger()
log.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

class HttpRecovery:
    def __init__(self,
                 statsd,
                 wsp_file,
                 qcountall,
                 http_port,
                 http_location,
                 graphite_dir,
                 hosts):
        self.wsp_file = wsp_file
        self.qcountall = qcountall
        self.sc = statsd
        self.http_port = http_port
        self.http_location = http_location
        self.graphite_dir = graphite_dir
        self.hosts = hosts

    def http_get(self):

        # get mirror whisper file from remote HTTP instance
#        rslt = []
        full_time = float()
        empty_hosts = []
        full_start = time.time()
        for host in self.hosts:
            copy_start = time.time()

            whisper = self.wsp_file.replace(self.graphite_dir, "")

            endpoint = 'http://' + host + ':' + self.http_port + '/' + self.http_location + '/' + whisper
            log.info(endpoint)
            try:
                #src_file = urllib2.urlopen(endpoint, timeout=5)
                self.sc.incr('SuccessHost')
                #wsp_buffer = src_file.read()

                if os.path.exists(os.path.dirname(self.wsp_file)) is False:
                    #os.makedirs(os.path.dirname(self.wsp_file))
                    print os.path.dirname(self.wsp_file)

                #dst_file = open(self.wsp_file, 'wb')
                #dst_file.write(wsp_buffer)
                #print self.wsp_file
                #dst_file.close()
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
