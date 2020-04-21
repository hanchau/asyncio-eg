import json
import yaml
import gearman

with open('config.yml', 'r') as cnfg:
    config = yaml.load(cnfg, Loader=yaml.FullLoader)

gearman_client = gearman.GearmanClient([config.get('GJS_1').get('job_server_url')])

def get_task():
    return json.dumps({"job_info": [1,2,3]})


gearman_client.submit_job(config.get('worker_ids').get('async_governer'), get_task(), background=True)
