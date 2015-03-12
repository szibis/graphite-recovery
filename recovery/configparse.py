import argparse


class ParseArgs:
    def parse_args(self):
        """ Parse args from console"""
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config",
                            help="Config file for your recovery",
                            action="store")
        option = parser.parse_args()
        return option
