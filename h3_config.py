import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/h3/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/h3/h3-extension.cpp', 'extension/h3/h3_common.cpp', 'extension/h3/h3_functions/h3_cell_to_parent.cpp', 'extension/h3/h3_functions/h3_valid.cpp']]
