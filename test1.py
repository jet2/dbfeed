import json
from tools import make_filegroups
sss='''
{"timestamp":1584902411,"source_name":"WINCC","tagname":"@RM_MASTER","new_value":true,"old_value":1}
'''

if __name__ == '__main__':
    # json.loads(sss)
    print ("%Y%Y-%m-%d-%H-%M"[14:16])
    print(make_filegroups())
