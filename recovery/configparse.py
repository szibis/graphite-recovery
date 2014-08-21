import argparse
import logging
import ConfigParser


class ParseArgs:
    def parse_args(self):
        """ Parse args from console"""
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config",
                            help="Config file for your recovery",
                            action="store")
        option = parser.parse_args()
        return option


    def getconfig():
        # Parse option args
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
        return result
