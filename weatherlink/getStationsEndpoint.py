import collections
import hashlib
import hmac
import time

parameters = {
    "api-key": "aeviyukrsmi1yafiucfflf2elmymrqha",
    "api-secret": "myi3edklxeym6px9kbmro4blseygfjwj",
    "t": int(time.time())
}

parameters = collections.OrderedDict(sorted(parameters.items()))

for key in parameters:
    print("Parameter name: \"{}\" has value \"{}\"".format(key, parameters[key]))

apiSecret = parameters["api-secret"];
parameters.pop("api-secret", None);

data = ""
for key in parameters:
    data = data + key + str(parameters[key])

print("Data string to hash is: \"{}\"".format(data))

apiSignature = hmac.new(
    apiSecret.encode('utf-8'),
    data.encode('utf-8'),
    hashlib.sha256
).hexdigest()

print("API Signature is: \"{}\"".format(apiSignature))

print("v2 API URL: https://api.weatherlink.com/v2/stations?api-key={}&api-signature={}&t={}".format(parameters["api-key"], apiSignature, parameters["t"]))
