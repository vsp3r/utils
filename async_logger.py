import threading
import queue
import time

class AsynchronousLogger:
    def __init__(self, log_file):
        self.log_queue = queue.Queue()
        self.log_file = f"./logs/{log_file}"
        self.running = True
        self.thread = threading.Thread(target=self.process_log_queue)
        self.thread.start()

    def log(self, message):
        """ Non-blocking log function """
        self.log_queue.put(message)

    def process_log_queue(self):
        """ Process log messages from the queue """
        print("STARTED LOGGER", self.log_file)
        with open(self.log_file, 'a') as file:
            while self.running:
                try:
                    # Timeout ensures the thread doesn't block indefinitely
                    message = self.log_queue.get(timeout=1)
                    # print(message)
                    file.write(f'{message}\n')
                    file.flush()
                except queue.Empty:
                    continue

    async def stop(self):
        """ Stop the logging thread """
        self.running = False

        self.thread.join()

