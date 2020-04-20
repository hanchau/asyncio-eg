import json
import yaml
import gearman
import asyncio
import pyfiglet


class MainWorker(object):

    def __init__(self, task_limit=10):
        with open('config.yml', 'r') as cnfg:
            self.config = yaml.load(cnfg, Loader=yaml.FullLoader)
        self._task_limit = task_limit
        self.gearman_client = gearman.GearmanClient([self.config.get('GJS_1').get('job_server_url')])
        self.gearman_worker = gearman.GearmanWorker([self.config.get('GJS_1').get('job_server_url')])
        self.gearman_worker.register_task(self.config.get('worker_ids').get('async_downloader'), self.async_downloader)
        ready_message = pyfiglet.figlet_format("Worker Ready!!")
        print(ready_message)

    def add_task(self, task_id):
        return url + "/downloaded"

    def remove_task(self, task_id):
        pass


    def async_downloader(self, worker, task):
        print("task ", task)
        print("worker ", worker)


        print(dir(task))
        print("---------")
        print(dir(worker))
        # for i in range(5):
        #     new_task = get_task(i)
        return json.dumps({})


if __name__ == '__main__':
    worker = MainWorker(task_limit=5)
    worker.gearman_worker.work()
