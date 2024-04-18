import asyncio
import time
import redis
import os
import atexit

REDIS_SERVER_URL = os.getenv("REDIS_SERVER_URL")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

class SorterMachineRedis:
    def __init__(self, redis_server_url: str, redis_port:int, username: str, password: str, db:int=0) -> None:
        self._redis_server_url = redis_server_url
        self._redis_port = redis_port
        self._username = username
        self._password = password
        self._db = db
        self.redis_connection = redis.Redis(host=self._redis_server_url,port=self._redis_port,username=self._username,password=self._password,db=self._db)
        self._machine_redis_heartbeat = 1

    def check_redis_connection(self) -> bool:
        return self.redis_connection.ping()
    
    def read_cloud_heartbeat_from_redis(self) -> str:
        pass

    def write_machine_heartbeat_to_redis(self, payload):
        if payload is not None:
            '''write data to topic'''
        pass

    def write_local_command_to_redis(self, payload):
        if payload is not None:
            '''write data to topic'''
        pass
    
    def write_machine_feedback_to_redis(self, payload):
        if payload is not None:
            '''write data to topic'''
        pass

    def write_batch_info_to_redis(self, payload):
        if payload is not None:
            '''write data to topic'''
        pass

    def write_aggr_out_of_frame_to_redis(self, payload):
        if payload is not None:
            '''write data to topic'''
        pass

    def write_box_completed_count_to_redis(self, payload):
        if payload is not None:
            '''write data to topic'''
        pass

class SorterMachineDataGenerator:
    def __init__(self) -> None:
        self._local_command = None
        self._local_command_duration = None
        self._local_command_generated_time = None
        self._batch_id = None
        self._batch_start_time = None
        self._batch_end_time = None
        self._plc_feedback = None
        self._ros_vision_feedback = None
        self._ros_ejector_feedback = None
        self._plc_feedback_error_duration = None
        self._ros_vision_feedback_error_duration = None
        self._ros_ejector_feedback_error_duration = None
        self._ros_vision_feedback_warn_duration = None
        self._ros_ejector_feedback_warn_duration = None


    async def generate_local_command(self) -> str:
        if self._local_command_generated_time is None:
            '''randomly select command from given list and its duration'''
            self._local_command = 0 # get random values from []
            self._local_command_duration = 0 # get random values from []
            self._local_command_generated_time = time.monotonic()
            await asyncio.sleep(1)
            await self.start_batch(self._local_command)
        else:
            '''check if the command duration is timeup'''
            if time.monotonic() > (self._local_command_generated_time + self._local_command_duration):
                self._local_command_generated_time = None
                self._local_command = 0 # set to off
                await asyncio.sleep(1)
                await self.end_batch()

    async def start_batch(self, command):
        '''check if the command is run if run get the command and longan size then create batch'''
        self._batch_start_command = command
        self._longan_size = 0 # process the command to get the longan size
        self._is_inverse = False # process the command to get the invers flag value
        self._batch_id = 'get uniqe id for batch'
        self._batch_total_longan = 0
        self._batch_defect_longan = 0
        self._batch_sorted_box = 0

    async def end_batch(self):
        '''end batch if the active batch exist'''

    def generate_batch_info(self) -> str:
        pass

    def generate_machine_feedback(self) -> str:
        self._plc_feedback = self.generate_plc_feedback()
        self._ros_vision_feedback = self.generate_ros_vision_feedback()
        self._ros_ejector_feedback = self.generate_ros_ejector_feedback()

    def generate_plc_feedback(self) -> str:
        '''will only generate feedback randomly when command is aa,a,b,iv-aa,iv-a,iv-b'''
        pass

    def generate_ros_vision_feedback(self) -> str:
        '''will only generate feedback randomly when command is aa,a,b,iv-aa,iv-a,iv-b'''
        pass

    def generate_ros_ejector_feedback(self) -> str:
        '''will only generate feedback randomly when command is aa,a,b,iv-aa,iv-a,iv-b'''
        pass

    def generate_out_of_frame(self) -> str:
        '''will only generate randomly when batch_id is not None'''
        pass

    def generate_box_completed_count(self) -> str:
        '''will only generate randomly when batch_id is not None'''
        pass

class SorterMachineSimulator(SorterMachineDataGenerator, SorterMachineRedis):
    def __init__(self, redis_server_url: str, redis_port:int, username: str, password: str, db:int=0) -> None:
        SorterMachineRedis.__init__(self, redis_server_url, redis_port, username, password)
        SorterMachineDataGenerator.__init__(self)
        self._current_local_command = None
        

    def write_to_redis(self):
        self.write_machine_heartbeat_to_redis(payload=self.read_cloud_heartbeat_from_redis())
        self.write_local_command_to_redis(self.generate_local_command())
        self.write_machine_feedback_to_redis(self.generate_machine_feedback())
        self.write_batch_info_to_redis(self.generate_batch_info())
        self.write_aggr_out_of_frame_to_redis(self.generate_out_of_frame())
        self.write_box_completed_count_to_redis(self.generate_box_completed_count())

    def cleanup(self):
        pass

    @classmethod
    def register_cleanup(cls, instance):
        atexit.register(instance.cleanup)


async def main():
    sorter_machine_simulator = SorterMachineSimulator(redis_server_url=REDIS_SERVER_URL, redis_port=REDIS_PORT, username=USERNAME, password=PASSWORD)
    SorterMachineSimulator.register_cleanup(sorter_machine_simulator)
    try:
        while True:
            await sorter_machine_simulator.generate_local_command()
            sorter_machine_simulator.write_to_redis()
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        sorter_machine_simulator.cleanup()

if __name__=="__main__":
    sm = SorterMachine(redis_server_url=REDIS_SERVER_URL, redis_port=REDIS_PORT, username=USERNAME, password=PASSWORD)
    print(sm.check_redis_connection())