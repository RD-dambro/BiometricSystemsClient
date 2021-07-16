from rabbit import Consumer
from subprocess import Popen, PIPE
from signal import SIGTERM, SIGKILL, SIGINT
from dotenv import load_dotenv
import os
import sys

class ClientProcess:
    def __init__(self, cmd, proc=None):
        self._cmd = cmd
        self._proc = proc

    def start(self):
        if self._proc == None:
            print('starting...')
            self._proc = Popen(self._cmd)
            print(f'Process started with PID: {self._proc.pid}')
    def stop(self):
        if self._proc != None:
            print("terminating...")
            self._proc.terminate()
            self._proc = None
            print("Process terminated correctly")
class BioClientManager:
    def __init__(self, loop):
        # self._cmd = ['/home/dambro/Documents/Heroku/testa-coda/test/venv/bin/python',loop]
        self._keys = self.get_keys()
        self.proc = {k: ClientProcess(['python3', loop, k], None) for k in self._keys}

        # for k in self.get_keys():
        #     procs[k] = ClientProcess(['python', loop, k], None)
        # self._cmd = ['python',loop]
        # self._proc = None

    def get_keys(self):
        load_dotenv()
        return [key.strip() for key in os.environ.get('keys').split(',')]
        
    def quit(self):
        print("quitting..")
        [self.proc[k].stop() for k in self._keys]
        sys.exit(0)

    def handler(self, channel, method, header, body):
        # TODO put this in the header
        k = method.routing_key.split('.')[0]

        if body == b'start':
            self.proc[k].start()
        elif body == b'stop':
            self.proc[k].stop()
        elif body == b'quit':
            self.quit()
        else:
            print(f'Cannot execute {body}')
    
    def run(self):
        Consumer().run(exchange='message', exchange_type='topic', queue='message', keys=[f'{k}.state' for k in self._keys], callback=self.handler)

if __name__ == '__main__':
    BioClientManager('bio_client.py').run()