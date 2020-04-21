import json
import yaml
import gearman
import asyncio
import pyfiglet
import time

class MainWorker(object):

    def __init__(self, task_limit=10):
        with open('config.yml', 'r') as cnfg:
            self.config = yaml.load(cnfg, Loader=yaml.FullLoader)
        self._task_limit = task_limit
        self.gearman_client = gearman.GearmanClient([self.config.get('GJS_1').get('job_server_url')])
        self.gearman_worker = gearman.GearmanWorker([self.config.get('GJS_1').get('job_server_url')])
        self.gearman_worker.register_task(self.config.get('worker_ids').get('async_governer'), self.governer)
        ready_message = pyfiglet.figlet_format("Worker Ready!!")
        print(ready_message)

    def add_task(self, task_id):
        return url + "/downloaded"

    def remove_task(self, task_id):
        pass

    async def helper_downloader(self,task):
        await asyncio.sleep(1)
        print("[{}] Download complete!!".format(task))

    async def async_downloader(self,tasks):
        await asyncio.gather(*(self.helper_downloader(i) for i in tasks))

    def sync_downloader(self,tasks):
        for i in tasks:
            time.sleep(2)
            print("[{}] Download complete!!".format(i))

    def governer(self, worker, task):
        # import pdb; pdb.set_trace()
        print(task.data)
        start_time = time.time()
        self.sync_downloader([1,2,2,4])
        # asyncio.run(self.async_downloader([1,2,3,4]))
        print("[Time Taken]: [{}]".format(time.time()-start_time))

        start_time = time.time()
        # self.sync_downloader([1,2,2,4])
        asyncio.run(self.async_downloader([1,2,3,4]))
        print("[Time Taken]: [{}]".format(time.time()-start_time))

        return json.dumps({})


if __name__ == '__main__':
    worker = MainWorker(task_limit=5)
    worker.gearman_worker.work()
