import backoff
import socket
import json
import urllib2
import logging
import ConfigParser
import errno
import os
import sys
import time
import requests
import subprocess
from pwd import getpwuid
from grp import getgrgid
from recovery.configparse import ParseArgs

class HttpRecovery:
    def __init__(self,
                 log,
                 statsd,
                 wsp_file,
                 qcountall,
                 http_port,
                 http_location,
                 graphite_dir,
                 hosts,
                 session):
        self.wsp_file = wsp_file.rstrip('\r\n')
        self.qcountall = qcountall
        self.log = log
        self.sc = statsd
        self.http_port = http_port
        self.http_location = http_location
        self.graphite_dir = graphite_dir
        self.hosts = hosts
        self.session = session


    def download_file(self, session, endpoint):
        copy_start = time.time()
        wsp_file = self.recovery_tmp()
        # NOTE the stream=True parameter
        r = session.get(endpoint, stream=True, timeout=0.3)
        if r.status_code == requests.codes.ok:
           self.dir_create(wsp_file)
           copy_elapsed = (time.time() - copy_start)
           # get file from remote count and time reporting
           self.sc.incr('recovery.remote_get.count')
           self.sc.timing('recovery.remote_get.time', copy_elapsed * 1000)
           with open(wsp_file, 'wb') as f:
              for chunk in r.iter_content(chunk_size=2548576):
                  if chunk:
                   f.write(chunk)
                   #f.flush() commented by rec
           f.close()
           copy_elapsed = (time.time() - copy_start)
           return wsp_file, copy_elapsed, r
        elif r.status_code == requests.codes.not_found:
           empty_elapsed = (time.time() - copy_start)
           return None, empty_elapsed, r


    def dir_create(self, wsp_file):
        if not os.path.isdir(os.path.dirname(wsp_file)):
           try:
              os.makedirs(os.path.dirname(wsp_file))
           except OSError as exc:
                  if exc.errno == errno.EEXIST and os.path.isdir(os.path.dirname(wsp_file)):
                     pass
                  else:
                     raise
           os.chown(os.path.dirname(wsp_file), 2001, 2001)
        else:
           os.chown(os.path.dirname(wsp_file), 2001, 2001)


    def wsp_permissions(self, wsp_file):
        try:
           if os.stat(wsp_file):
              if getpwuid(os.stat(wsp_file).st_uid) != 2001:
                 os.chown(os.path.dirname(wsp_file), 2001, 2001)
              elif getgrgid(os.stat(wsp_file).st_gid) != 2001:
                   os.chown(os.path.dirname(wsp_file), 2001, 2001)
        except:
           raise


    def sparsify(self, wsp_file, environ):
        command = "sudo /usr/bin/bucky-sparsify %s" % (wsp_file.rstrip('\r\n'))
        self.log.debug(command)
        sparsify = subprocess.Popen(
                       command,
                       env=environ,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       shell=True
                       #preexec_fn=self.set_permissions()
                  )
        return sparsify


#    def set_permissions(self):
#        os.seteuid(2001)
#        #os.setegid(2001)


    def prepare_http(self, host):
        whisper = self.wsp_file.replace(self.graphite_dir, "")
        endpoint = 'http://' + host + ':' + self.http_port + '/' + self.http_location + '/' + whisper
        return endpoint.rstrip('\r\n')


    def recovery_tmp(self):
        return self.wsp_file.replace(".wsp", ".wsp_recovery").rstrip('\n\r')


    def http_get(self):
        environ = os.environ.copy()
        full_time = float()
        empty_hosts = []
        full_start = time.time()
        # try to get whisper files from all other graphite stores
        for host in self.hosts:
            copy_start = time.time()
            # prepare full request string
            endpoint = self.prepare_http(host)
            # hide some verbosity from urlib3 and requests
            logging.getLogger("requests").setLevel(logging.WARNING)
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            try:
                # making GET request with timeout for whisper file
                temp_wsp, get_elapsed, r = self.download_file(self.session, endpoint)
                #self.log.info(r.raise_for_status())
                self.log.debug("%s GET %s %s in %s" % (host, endpoint, r.status_code, get_elapsed * 1000))
                #self.log.info("%s GET %s in %s [ms]" % (host, r.status_code, get_elapsed * 1000))
                # create recovery file and backfill from remote hosts whisper files
                if temp_wsp:
                  # report ok response for host to statsd
                  self.sc.incr('recovery.getfile.count')
                  copy_elapsed = (time.time() - copy_start)
                  # get file from remote count and time reporting
                  self.sc.incr('recovery.remote_get.count')
                  self.sc.timing('recovery.remote_get.time', copy_elapsed * 1000)
                  # use golang bucky-fill to backfill from downloaded recovered fiwsp_file is None
                  backfill_start = time.time()
                  #self.log.info("sudo -u carbon /usr/bin/bucky-fill %s %s" % (temp_wsp.rstrip('\r\n'), self.wsp_file.rstrip('\r\n')))
                  command = "sudo /usr/bin/bucky-fill %s %s" % (temp_wsp.rstrip('\r\n'), self.wsp_file.rstrip('\r\n'))
                  self.log.debug(command)
                  backfill = subprocess.Popen(
                       command,
                       env=environ,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       shell=True
                       #preexec_fn=self.set_permissions()
                       )
                  self.log.debug(backfill.communicate())
                  backfill_elapsed = (time.time() - backfill_start)
                  if backfill.returncode == 0:
                       self.log.info("[BackFill OK] data to %s in %s [ms] code: %s" % (self.wsp_file, backfill_elapsed * 1000, backfill.returncode))
                       os.unlink(temp_wsp)
                  elif backfill.returncode in (1, 2):
                        if not os.path.isfile(self.wsp_file):
                           rename_start = time.time()
                           try:
                              self.dir_create(self.wsp_file)
                              self.wsp_permissions(self.wsp_file)
                           except OSError:
                              pass
                           try:
                              os.rename(temp_wsp, self.wsp_file)
                              self.wsp_permissions(self.wsp_file)
                              sparsify = self.sparsify(self.wsp_file, environ)
                           except:
                              raise
                           rename_elapsed = (time.time() - rename_start)
                           if sparsify.returncode in (1, 2):
                              self.log.info("[MV WSP] Rename no sparsify from recovery to %s in %s [ms]" % (self.wsp_file, rename_elapsed * 1000))
                              self.sc.incr('recovery.rename.no_sparsify.success.count')
                           self.log.info("[MV WSP] Rename and sparsify from recovery to %s in %s [ms]" % (self.wsp_file, rename_elapsed * 1000))
                           self.sc.incr('recovery.rename.success.count')
                           self.sc.timing('recovery.rename.success.time', rename_elapsed * 1000)
                        else:
                           os.unlink(temp_wsp)
                           self.sc.incr('recovery.backfill.fail.count')
                           self.log.info("[BackFill FAIL] %s in %s [ms] code: %s" % (self.wsp_file, backfill_elapsed * 1000, backfill.returncode))
                  # successTime for each file backfill
                  self.sc.incr('recovery.backfill_rename.success.count')
                  self.sc.timing('recovery.backfill.time', backfill_elapsed * 1000)
                elif temp_wsp is None:
                     empty_hosts.append(host)
                     self.sc.incr('recovery.empty.count')
                     self.log.debug("Empty hosts: %s " % empty_hosts)
                     empty_elapsed = (time.time() - copy_start)
                     self.sc.timing('recovery.empty.time', empty_elapsed * 1000)
                     continue
                # report failed hosts other then 404 and 200 response
                elif r.status_code != requests.codes.not_found or r.status_code != requests.codes.ok:
                     # write recovery error path's to file
                     self.sc.incr('recovery.error.count')
                     self.log.debug("Error on hosts: %s %s" % (host, r.status_code))
                     empty_elapsed = (time.time() - copy_start)
                     continue
            except requests.Timeout as err:
                self.log.debug(err.message)
                self.sc.incr('recovery.exceptions.requests_timeout.count')
                continue
            except socket.timeout as err:
                self.log.debug(err.message)
                self.sc.incr('recovery.exceptions.socket_timeout.count')
                continue
            except requests.exceptions.RequestException as err:
                self.log.debug(err.message)
                self.sc.incr('recovery.exceptions.other.count')
                continue
            # report full count - means all recovery processed for this particular whisper
            self.sc.incr('recovery.full.count')
            full_time = (time.time() - full_start)
            self.sc.gauge('recovery.queue.put', self.qcountall)
            self.sc.timing('recovery.full.time', full_time * 1000)
