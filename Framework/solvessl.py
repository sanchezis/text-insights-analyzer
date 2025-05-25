
from urllib import request
import certifi
import ssl

context = ssl.create_default_context(cafile=certifi.where())
https_handler = request.HTTPSHandler(context=context)
opener = request.build_opener(https_handler)
request.install_opener(opener)

