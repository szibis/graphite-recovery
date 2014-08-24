import json
import os
import time
import paramiko
import statsd
import errno
from recovery.configparse import ParseArgs

class SshRecovery:
    def __init__(self,
                 queue,
                 qcountall):
        self.queue = queue
        self.qcountall = qcountall

    def ssh_get(self, wsp_file):

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
        ssh_privkey = config.get('ssh', 'private_key')
        ssh_user = config.get('ssh', 'user')
        ssh_port = int(config.get('ssh', 'port'))
        hosts = json.loads(config.get('main', 'hosts'))
        statsd_host = config.get('statsd', 'host')
        statsd_port = config.get('statsd', 'port')
        statsd_prefix = config.get('statsd', 'prefix')

        # initialize statsd
        sc = statsd.StatsdClient(str(statsd_host),
                                 int(statsd_port),
                                 prefix=statsd_prefix,
                                 sample_rate=None)

        mpkey = os.path.expanduser(ssh_privkey)
        sftp = None
        full_time = float()
        empty_hosts = []
        full_start = time.time()
        okhost = None
        for host in hosts:
            copy_start = time.time()
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            mykey = paramiko.RSAKey.from_private_key_file(mpkey)
            ssh.connect(host, port=ssh_port,
                        username=ssh_user,
                        pkey=mykey,
                        timeout=6.0)
            sftp = ssh.open_sftp()
            copy_elapsed = (time.time() - copy_start)
            try:
                sftp.get(wsp_file, wsp_file)
                sc.incr('SuccessHost')
                okhost = host
                sftp.close()
                copy_elapsed = (time.time() - copy_start)
                sc.timing('SuccessTime', copy_elapsed * 1000)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    empty_hosts.append(host)
                    sc.incr('EmptyHost')
                    empty_elapsed = (time.time() - copy_start)
                    sc.timing('EmptyTime', empty_elapsed * 1000)
                    sftp.close()
                    pass
        sc.incr('FullCount')
        full_time = (time.time() - full_start)
        sc.gauge('QueueTasksPut', self.qcountall)
        sc.timing('FullTime', full_time * 1000)
        print '{0}->local (Empty: {1} Time: {2:.3f}[sec]) - Success - \
(Time: {3:.3f}[sec]): {4}'.format(okhost,
                                  ', '.join(empty_hosts),
                                  full_time,
                                  copy_elapsed,
                                  wsp_file)
