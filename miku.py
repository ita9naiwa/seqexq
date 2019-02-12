import time
import logging
import subprocess
import os
import signal

num_tot_jobs = 2
curr_jobs = 0

pending_jobs = []
running_jobs = {}

class Job:
    def __init__(self, dir, fname):
        self.dir = dir
        self.fname = fname
        self.state = 'pending'

    def run(self,):
        output_dir = os.path.join(self.dir, '%s_output.txt' % self.fname)
        self.f = self.stdout_f = open(output_dir, 'w')
        self.proc = subprocess.Popen(
            os.path.join(self.dir, self.fname),
            cwd=self.dir,
            stdout=self.f)
        self.state = 'running'

    def close(self,):
        self.f.flush()
        self.f.close()
        self.f = 'finished'


class worker:

    def __init__(self, foreground, log_file=None):
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.logger = logging.getLogger("worker")
        self.log_file = log_file

        if not foreground and log_file:
            self.log_handler = logging.FileHandler(self.log_file)
            self.logger.addHandler(self.log_handler)

        self.__stop = False

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def main(self):
        self.logger.info("Start Singing, PID {0}".format(os.getpid()))
        while not self.__stop:
            self.read_job()
            while len(pending_jobs) > 0 and curr_jobs < num_tot_jobs:
                job = pending_jobs[0]
                self.run_pending_job(job)
                
            self.logger.info("%d" % len(pending_jobs))
            time.sleep(1)
        return 0

    def spawn_worker(self, job):
        dir_path, fname = job
        self.logger.info(dir_path)
        self.logger.info(job)
        output_dir = os.path.join(dir_path, '%s_output.txt' % fname)
        j = job(dir_path, fname)

    def stop(self, signum, frame):
        self.__stop = True
        self.logger.info("Receive Signal {0}".format(signum))
        self.logger.info("Stop Singing")

    def read_job(self, ):
        fname = '/tmp/seqexq/list'
        exists = os.path.isfile(fname)
        works = []
        if exists:
            with open(fname, 'r') as f:
                l = f.readline().strip()
                dir_path, filename = os.path.split(l)
                new_job = Job(dir_path, fname)
                pending_jobs.append(new_job)
                #f.truncate(0)
        return works

    def run_pending_job(self, job):
        job.run()
