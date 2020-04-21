import json
import yaml
import gearman
import asyncio
import pyfiglet
import time
import subprocess


global event_loop
event_loop = asyncio.get_event_loop()

class MainWorker(object):

    def __init__(self, task_limit=10):
        with open('config.yml', 'r') as cnfg:
            self.config = yaml.load(cnfg, Loader=yaml.FullLoader)
        self._task_limit = task_limit
        self._count_current_tasks = 0
        self._current_tasks = []
        self._timer = 0
        self.gearman_client = gearman.GearmanClient([self.config.get('GJS_1').get('job_server_url')])
        self.gearman_worker = gearman.GearmanWorker([self.config.get('GJS_1').get('job_server_url')])
        self.gearman_worker.register_task(self.config.get('worker_ids').get('async_governer'), self.governer)
        ready_message = pyfiglet.figlet_format("Worker Ready!!")
        print(ready_message)

    def get_task_limit(self):
        return self._task_limit

    def get_current_tasks(self):
        return self._current_tasks

    def get_count_current_tasks(self):
        return self._count_current_tasks

    def add_task(self, task_id, task):
        self._current_tasks.append({task_id: task})
        self._count_current_tasks += 1

    def remove_task(self, task_id):
        for task_ob in self._current_tasks:
            if task_ob.get(task_id):
                task = self._current_tasks.remove(task_ob, None)
                self._count_current_tasks -=1
                return task

    async def helper_downloader(self,task):
        await asyncio.sleep(4)
        print("[{}] Download complete!!".format(task))

    async def async_downloader(self,tasks):
        await asyncio.gather(*(self.helper_downloader(i) for i in tasks))

    def sync_downloader(self,tasks):
        for i in tasks:
            time.sleep(10)
            print("[{}] Download complete!!".format(i))

    async def async_handler(self, event_loop):
        await asyncio.gather(*(self.helper_downloader(task_ob) for task_ob in self._current_tasks))
        # self.add_task(task_info.unique, task_info.data)
        # time.sleep(1)
        # try:
        #     loop = asyncio.get_running_loop()
        # except:
        # if not self.loop:
        #     self.loop = asyncio.get_event_loop()
        # global event_loop
        # task = event_loop.create_task(self.helper_downloader(self._count_current_tasks))
        # if self._count_current_tasks < self._task_limit:
        #     self.loop.run_untill_complete()
        # await task
        # loop.run_forever()
        # loop.close()
        # print("[current tasks]: ", self._current_tasks)
        # print("[count of tasks]: ", self._count_current_tasks)
        # await task


    def governer(self, worker, task_info):
        # import pdb; pdb.set_trace()
        print(task_info.data)
        print("task received!!")
        time.sleep(1)
        self.add_task(task_info.unique, task_info.data)
        print("[current tasks]: ", self.get_current_tasks())
        print("[count of tasks]: ", self.get_count_current_tasks())

        if self.get_count_current_tasks() == self.get_task_limit():
            event_loop.run_until_complete(self.async_handler(event_loop))
            self._count_current_tasks = 0
            self._current_tasks = []
        return json.dumps({})


if __name__ == '__main__':
    worker = MainWorker(task_limit=5)
    worker.gearman_worker.work()
