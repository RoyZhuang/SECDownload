import os
import time
import warnings
import yaml
from munch import munchify, unmunchify

ENV_VAR_ROOT = 'SEC_Data'
DEFAULT_CONFIG_FILENAME = '~/sec_data.yml'

def from_env(key, default_value=None, root=ENV_VAR_ROOT):

    if root != "":
        ENV_VAR_KEY = root + "_" + key.upper()
    else:
        ENV_VAR_KEY = key.upper()
    if default_value == '' or default_value is None:
        try:
            return(os.environ[ENV_VAR_KEY])
        except Exception:
            warnings.warn("You should pass %s using --%s or using environment variable %r" % (key, key, ENV_VAR_KEY))
            return(default_value)

    else:
        return(default_value)

DEFAULT = munchify({
    "EDGAR_DATA_DIR": from_env("EDGAR_DATA_DIR", "~/SEC_Data/EDGAR"),
    "CIK_DIR": from_env("CIK_DIR", "~/SEC_Data/CIK"),
    "CIK_MERGED_DIR": from_env("CIK_MERGED_DIR", "~/SEC_Data"),
    "XBRL_LIST_DIR": from_env("XBRL_LIST_DIR", "~/SEC_Data"),
    "XBRL_FILE_DIR": from_env("XBRL_FILE_DIR", "~/SEC_Data/XBRLFile")
})


TEST = munchify({
    "EDGAR_DATA_DIR": "/Users/robertchuang/SEC_Data/EDGAR",
    "CIK_DIR": "/Users/robertchuang/SEC_Data/CIK",
    "CIK_MERGED_DIR": "/Users/robertchuang/SEC_Data",
    "XBRL_LIST_DIR": "/Users/robertchuang/SEC_Data",
    "XBRL_FILE_DIR": "/Users/robertchuang/SEC_Data/XBRLFile"
})

def from_file(fname=DEFAULT_CONFIG_FILENAME, testing=False):
    if testing:
        return TEST
    try:
        print('enter from_file')
        with open(os.path.expanduser(fname)) as fd:
            conf = yaml.load(fd)
        conf = munchify(conf)
        return conf
    except IOError:
        print("A configuration file named '%s' is missing" % fname)
        s_conf = yaml.dump(unmunchify(DEFAULT), explicit_start=True, indent=True, default_flow_style=False)
        print("""
Creating this file

%s

You still have to create directories with data and put your data in!
""" % s_conf)
        time.sleep(3)
        try:
            with open(os.path.expanduser(fname), "w") as fd:
                fd.write(s_conf)
        except IOError:
            print("Can create '%s'" % fname)
    print("Trying anyway with default configuration")
    return DEFAULT
