import sys 
import os

sys.path += [ f"./.." , f"./../Framework" , ]

os.environ['PYSPARK_PYTHON']='python'
os.environ['PYARROW_IGNORE_TIMEZONE']='1'

# If this code runs in MACOS this is needed for numpy C Headers
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
