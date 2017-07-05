# -*- encoding: utf-8 -*-
"""

"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, open,str,super, range,
                      zip, round, input, int, pow, object, map, zip)


import os
import argparse



from cdci_data_analysis.flask_app.app import run_app
from cdci_data_analysis.configurer import ConfigEnv




def main(argv=None):
	parser = argparse.ArgumentParser()
	parser.add_argument('-conf_file',type=str,default=None)

	args = parser.parse_args()

	conf_file=args.conf_file

	conf= ConfigEnv.from_conf_file(conf_file)

	run_app(conf)

	if __name__ == "__main__":
		#$port = int(os.environ.get("PORT", 5000))
		main()
