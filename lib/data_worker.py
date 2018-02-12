import time
import threading


class DataWorker:
    data_store = None
    update_frequency = None
    last_update = 0

    def __init__(self, data_store, update_manager=False):
        self.data_store = data_store

        if self.data_store is None:
            raise Exception("DataProcessor 'data_store' is required")

        if self.update_frequency is None:
            raise Exception("DataProcessor 'update_frequency' is required")


    def run(self):
        def background():
            while True:
                time_start = time.time()
                self.fetch_data()

                # Sleep until next time window
                time_end = time.time()
                process_time = time_end - time_start

                if process_time < self.update_frequency:
                    time.sleep(self.update_frequency - process_time)


        background_thread = threading.Thread(
            target=background,
        )

        background_thread.daemon = True
        background_thread.start()
        self.thread = background_thread

    def is_active(self):
        return self.thread.is_alive()

    def ping(self):
        '''
        Check if the background process is alive and restart if not
        :return:
        '''

        if not self.is_active():
            # process crashed!
            # TODO add remote metric to send notification
            self.run()

        return True

    def fetch_data(self):
        '''
        Should implement data retrieval with self.on_data call per data item
        :return:
        '''
        raise Exception("Not implemented")

    def on_data(self, data):
        '''
        Should implemente data retrival with self.on_data call per data item
        :return:
        '''
        raise Exception("Not implemented")

    def data_item_prepare(self, item_raw):
        raise Exception("Not implemented")

    def save(self, partition, items):
        self.data_store.append(partition, items)