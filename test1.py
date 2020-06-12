import json

sss='''
{"timestamp":1584902411,"source_name":"WINCC","tagname":"@RM_MASTER","new_value":true,"old_value":1}
'''

if __name__ == '__main__':
    json.loads(sss)