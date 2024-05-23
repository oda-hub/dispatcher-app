from . import empty_instrument
from . import empty_semi_async_instrument
from . import empty_async_instrument
from . import empty_development_instrument
from . import empty_async_return_progress_instrument
from . import empty_instrument_with_conf
from . import conf_file
import yaml

try:
    with open(conf_file, 'r') as fd:
        config = yaml.full_load(fd)
except Exception as e:
    config = {'instruments': []}

instr_factory_list = []

for instr in config['instruments']:
    instr_factory_list.append( getattr(globals()[instr], 'my_instr_factory') )
