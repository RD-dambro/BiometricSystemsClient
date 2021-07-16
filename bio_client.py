import asyncio
import cv2 as cv
import os
import sys
import numpy as np
# from keras.preprocessing import image
# from rabbit_layer import RabbitProducer
from dotenv import load_dotenv

from rabbit import Producer

class BioClient:
    def __init__(self, source, topic='uid1'):
        self._staged_tasks = []
        self._d = {
            'stream-plain': self.sendPlain,
            'stream-face': self.sendFace,
            'loop': self.getFrame
        }
        self._src = self.init_plain_source(source)
        self._producer = self.init_face_producer(topic)

    def init_plain_source(self, src):
        self._target_size = (152, 152)
        self._face_rate = 1/5.0
        
        return cv.VideoCapture(src)
    
    def init_plain_stream(self):
        pass
    def init_face_producer(self, topic):
        haar_weights = 'haarcascade_frontalface_alt.xml'
        self._face_cascade = cv.CascadeClassifier(haar_weights)
        self._topic = topic
        self._routing_key = self._topic
        self._staged_tasks.append('stream-face')
        p = Producer()
        p.run(exchange='message', exchange_type='topic')
        return p
        # return RabbitProducer(queue=self._topic, routing_key=self._routing_key)
    
    async def getFrame(self):
        ret, img = self._src.read()
        return img
    
    async def sendPlain(self):
        pass
    async def sendFace(self, f):
        if f is None:
            return None
        faces = self._face_cascade.detectMultiScale(f, 1.3, 5)
    
        if len(faces) > 0:
            x,y,w,h = faces[0]	
            margin = 0
            x_margin = w * margin / 100
            y_margin = h * margin / 100
            
            if y - y_margin > 0 and y+h+y_margin < f.shape[1] and x-x_margin > 0 and x+w+x_margin < f.shape[0]:
                    detected_face = f[int(y-y_margin):int(y+h+y_margin), int(x-x_margin):int(x+w+x_margin)]
            else:
                detected_face = f[int(y):int(y+h), int(x):int(x+w)]
            
            # # opzione plain
            detected_face = cv.resize(detected_face, self._target_size)
            res, target = cv.imencode('.jpeg', detected_face)
            try:
                # self._producer.send(detected_face.tobytes())
                self._producer.publish(exchange='message', routing_key=f'{self._topic}.face', msg=target.tobytes())
            except:
                print('Error on send, probably disconnected by timeout')
            # # opzione processata
            # img_pixels = image.img_to_array(detected_face)
            # img_pixels = np.expand_dims(img_pixels, axis = 0)
            
            # #normalize in [0, 1]
            # img_pixels /= 255
            # self._producer.send(img_pixels.tobytes())
            
            await asyncio.sleep(self._face_rate)
    
    async def main(self):
        f = None
        tasks = {}
        loop_task = asyncio.create_task(self._d['loop']())
        for t in self._staged_tasks:
            tasks[t] = asyncio.create_task(self._d[t](f))

        try: 
            while True:
                f = await loop_task
                for t in self._staged_tasks:
                    if tasks[t].done():
                        tasks[t] = asyncio.create_task(self._d[t](f))

                loop_task = asyncio.create_task(self._d['loop']())
        except:
            print("terminated")
    def run(self):
        asyncio.run(self.main())


if __name__ == '__main__':
    def get_url(k):
        load_dotenv()
        host = os.environ.get('mediaHost')
        port=os.environ.get('mediaPort')
        app = os.environ.get('mediaApp')
        return f'http://{host}:{port}/{app}/{k}.flv'
    # TODO arg parser instead of argv
    topic = sys.argv[1]
    BioClient(get_url(topic), topic).run()