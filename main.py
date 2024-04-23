import asyncio
from enum import Enum
import json
import random
import time
import redis
import os
import atexit
import uuid

from datetime import datetime, timezone, timedelta
from random import randrange

REDIS_SERVER_URL = os.getenv("REDIS_SERVER_URL")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
MACHINE_ID = '123456'
MAIN_LOOP_TIME_SEC = 3

class NodeStatuses(Enum):
    READY = 0
    PLAYING = 1
    PAUSE = 2
    WARN = 3
    ERROR = 4
    INPROGRESS = 5

class PlcHeartbeats(Enum):
    OK = "ok"
    ERROR = "error"


class SorterMachineRedis:
    def __init__(self, redis_server_url: str, redis_port:int, username: str, password: str, db:int=0) -> None:
        self._redis_server_url = redis_server_url
        self._redis_port = redis_port
        self._username = username
        self._password = password
        self._db = db
        self._machine_id = MACHINE_ID
        self.redis_connection = redis.Redis(host=self._redis_server_url,port=self._redis_port,username=self._username,password=self._password,db=self._db)
        self._machine_redis_heartbeat = 1
        self._cloud_redis_heartbeat = 0

        self._feedbacks_holder = []
        self._local_command_holder = []
        self._batch_info_holder = []
        self._box_cound_info_holder = []
        self._aggr_longan_info_holder = []

    def check_redis_connection(self) -> bool:
        return self.redis_connection.ping()
    
    def check_heartbeat_from_redis(self, _reading_heartbeat_value: str) -> str:
        redis_topic = f"heartbeat:{self._machine_id}"
        if self.check_redis_connection():
            result = self.redis_connection.get(redis_topic) == _reading_heartbeat_value.encode()
            return result
        return False
    
    def read_redis_payloads(self, r_topic:str):
        _return_payloads = []
        if self.check_redis_connection():
            while True:
                payload = self.redis_connection.lpop(r_topic)
                if payload is not None:
                    # print(f"{r_topic} payload - {payload}")
                    _return_payloads.append(payload.decode())
                else:
                    break
        return _return_payloads

    def write_heartbeat_to_redis(self, payload: str):
        redis_topic = f"heartbeat:{self._machine_id}"
        # print(f"func write_heartbeat_to_redis: Redis_connection - {self.check_redis_connection()}")
        if self.check_redis_connection():
            '''write data to topic'''
            self.redis_connection.set(redis_topic, payload)
        print(f"func write_heartbeat_to_redis ({payload})")
        pass

    def write_event_stream_data_to_redis(self, r_topic:str, r_payload:str=None, backup_holder:list=[]) -> list:
        if r_payload is not None:
            print(f"writing to {r_topic}...")
            backup_holder.append(r_payload)
            if self.check_redis_connection():
                while len(backup_holder) > 0:
                    try:
                        self.redis_connection.rpush(r_topic, backup_holder[0])
                        backup_holder.pop(0)
                        print("Successfully wrote to redis server.")
                    except Exception as e:
                        print(f"func write_event_stream_data_to_redis: error occure - {e}")
                        break
        return backup_holder


class SorterMachineDataGenerator:
    def __init__(self) -> None:
        self._machine_id = MACHINE_ID
        self._local_command = None
        self._local_command_duration = None
        self._local_command_duration_endtime = None
        self._local_command_generated_time = None
        self._batch_id = None
        self._batch_start_time = None
        self._batch_end_time = None
        self._plc_feedback = None
        self._ros_vision_feedback = None
        self._ros_ejector_feedback = None
        self._current_plc_status = None
        self._current_ros_vision_status = None
        self._current_ros_ejector_status = None
        self._ros_vision_feedback_warn_timeup_at = None
        self._ros_ejector_feedback_warn_timeup_at = None
        self._batch_records = []
        self._is_batch_record_updated = False
        self._prev_out_of_frame_end_time = None
        self._prev_box_completed_time = None
        
    def datetime_to_str(self, dt_object: datetime) -> str:
        _str_format = "%Y-%m-%dT%H:%M:%S.%f%z"
        return dt_object.strftime(_str_format)

    async def generate_local_command(self) -> str:
        if self._local_command_generated_time is None:
            '''randomly select command from given list and its duration'''
            self._local_command = randrange(7) # random int 0-7
            self._local_command_duration = randrange(20,30) # get random values from []
            self._local_command_generated_time = datetime.now(timezone.utc)
            self._local_command_duration_endtime = self._local_command_generated_time + timedelta(seconds=self._local_command_duration)
            await asyncio.sleep(1)
            await self.start_batch(self._local_command)
        else:
            '''check the status has any error, send stop cammand if there's error'''
            if self.check_error_status():
                self._local_command_duration_endtime = datetime.now(timezone.utc)
            '''check if the command duration is timeup'''
            if datetime.now(timezone.utc) >= self._local_command_duration_endtime:
                self._local_command_generated_time = None
                self._local_command = 0 # set to off
                await asyncio.sleep(1)
                self.end_batch(self._batch_id)

    def check_error_status(self):
        _is_ros_error = NodeStatuses.ERROR.value in [self._current_ros_vision_status, self._current_ros_ejector_status]
        _is_plc_error = PlcHeartbeats.ERROR == self._plc_feedback
        return _is_ros_error or _is_plc_error
    
    async def start_batch(self, command):
        '''check if the command is run if run get the command and longan size then create batch'''
        if command > 0:
            self._batch_start_command = command
            self._batch_id = str(uuid.uuid4())
            self._batch_start_time = datetime.now(timezone.utc)
            if self._batch_id not in list(each["local_batch_id"] for each in self._batch_records):
                '''by using list to hold the batch info, it makes sure that the latest info of the prev batch info has been send'''
                self._batch_records.append({
                    "payload_name": "longan_batch",
                    "machine_id": self._machine_id,
                    "local_batch_id": self._batch_id,
                    "command": self._batch_start_command,
                    "batch_started_at": self.datetime_to_str(self._batch_start_time),
                    "batch_ended_at": None,
                })
                self._is_batch_record_updated = True

    def end_batch(self, batch_id):
        '''update the batch record according to the batch id'''
        for each in self._batch_records:
            if each["local_batch_id"] == batch_id:
                each["batch_ended_at"] = self.datetime_to_str(datetime.now(timezone.utc))
                self._is_batch_record_updated = True

    def get_current_local_command(self) -> str:
        return json.dumps({"payload_name": "local_command","command": self._local_command})

    def generate_batch_info(self) -> str:
        if len(self._batch_records) > 0:
            if self._batch_records[0]["batch_ended_at"] is not None:
                return json.dumps(self._batch_records.pop(0))
            else:
                self._is_batch_record_updated = True
                return json.dumps(self._batch_records[0])
        return None        

    def generate_machine_feedback(self) -> str:
        self._plc_feedback = self.generate_plc_feedback()
        self._ros_vision_feedback = self.generate_ros_vision_feedback()
        self._ros_ejector_feedback = self.generate_ros_ejector_feedback()
        machine_feedback = {
            'payload_name': 'machine_feedback',
            'plc': self._plc_feedback,
            'ros_vision': self._ros_vision_feedback,
            'ros_ejector': self._ros_ejector_feedback,
            'timestamp': self.datetime_to_str(datetime.now(timezone.utc))
        }
        return json.dumps(machine_feedback)

    def generate_plc_feedback(self) -> str:
        '''will only generate feedback randomly when command is aa,a,b,iv-aa,iv-a,iv-b'''
        if self._local_command in [2,3,4,5,6,7]:
            _plc_status_choice = [PlcHeartbeats.OK.value, PlcHeartbeats.OK.value, PlcHeartbeats.OK.value, PlcHeartbeats.OK.value, PlcHeartbeats.ERROR.value]
            self._current_plc_status = random.choice(_plc_status_choice) if self._current_plc_status == PlcHeartbeats.OK.value else PlcHeartbeats.OK.value
        _info_msg = ""
        _error_msg = "plc error" if self._current_plc_status == PlcHeartbeats.ERROR.value else ""
        _plc_feedback = {
            "plc_command": "plc_command",
            "status": self._current_plc_status,
            "info": _info_msg,
            "error": _error_msg
        }
        return _plc_feedback

    def generate_ros_vision_feedback(self) -> str:
        '''will only generate feedback randomly when command is aa,a,b,iv-aa,iv-a,iv-b'''
        _info_msg = ""
        _error_msg = ""
        if self._local_command in [2,3,4,5,6,7]:
            _valid_choices = [NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.WARN.value, NodeStatuses.ERROR.value]
            _node_status = random.choice(_valid_choices) if self._current_ros_vision_status in [NodeStatuses.READY.value, NodeStatuses.PAUSE.value] else NodeStatuses.INPROGRESS.value 
            if _node_status != self._current_ros_vision_status:
                if self._current_ros_vision_status in [NodeStatuses.READY.value, NodeStatuses.PAUSE.value, None]:
                    self._current_ros_vision_status = NodeStatuses.INPROGRESS.value
                elif self._current_ros_vision_status == NodeStatuses.INPROGRESS.value:
                    self._current_ros_vision_status = NodeStatuses.PLAYING.value
                elif self._current_ros_vision_status == NodeStatuses.PLAYING.value:
                    self._current_ros_vision_status = _node_status
                    if _node_status == NodeStatuses.WARN.value:
                        self._ros_vision_feedback_warn_timeup_at = datetime.now(timezone.utc) + timedelta(seconds=randrange(2,5))
                else:
                    if self._current_ros_vision_status == NodeStatuses.WARN.value and self._ros_vision_feedback_warn_timeup_at < datetime.now(timezone.utc):
                        self._current_ros_vision_status = NodeStatuses.ERROR.value
                    else:
                        self._current_ros_vision_status = _node_status     
                _info_msg = "vision warning" if self._current_ros_vision_status == NodeStatuses.WARN.value else ""
                _error_msg = "vision error" if self._current_ros_vision_status == NodeStatuses.ERROR.value else ""           
        else:
            _node_status = NodeStatuses.PAUSE.value
            if _node_status != self._current_ros_vision_status:
                self._current_ros_vision_status = _node_status

        _ros_vision = {
            "ros_command": "start",
            "config_hash": {"hello": "wow"},
            "status": self._current_ros_vision_status,
            "info": _info_msg,
            "error": _error_msg
        }
        return _ros_vision

    def generate_ros_ejector_feedback(self) -> str:
        '''will only generate feedback randomly when command is aa,a,b,iv-aa,iv-a,iv-b'''
        _info_msg = ""
        _error_msg = ""
        if self._local_command in [2,3,4,5,6,7]:
            _valid_choices = [NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.PLAYING.value, NodeStatuses.WARN.value, NodeStatuses.ERROR.value]
            _node_status = random.choice(_valid_choices) if self._current_ros_ejector_status in [NodeStatuses.READY.value, NodeStatuses.PAUSE.value] else NodeStatuses.INPROGRESS.value 
            if _node_status != self._current_ros_ejector_status:
                if self._current_ros_ejector_status in [NodeStatuses.READY.value, NodeStatuses.PAUSE.value, None]:
                    self._current_ros_ejector_status = NodeStatuses.INPROGRESS.value
                elif self._current_ros_ejector_status == NodeStatuses.INPROGRESS.value:
                    self._current_ros_ejector_status = NodeStatuses.PLAYING.value
                elif self._current_ros_ejector_status == NodeStatuses.PLAYING.value:
                    self._current_ros_ejector_status = _node_status
                    if _node_status == NodeStatuses.WARN.value:
                        self._ros_ejector_feedback_warn_timeup_at = datetime.now(timezone.utc) + timedelta(seconds=randrange(2,5))
                else:
                    if self._current_ros_ejector_status == NodeStatuses.WARN.value and self._ros_ejector_feedback_warn_timeup_at < datetime.now(timezone.utc):
                        self._current_ros_ejector_status = NodeStatuses.ERROR.value
                    else:
                        self._current_ros_ejector_status = _node_status
                _info_msg = "ejector warning" if self._current_ros_ejector_status == NodeStatuses.WARN.value else ""
                _error_msg = "ejector error" if self._current_ros_ejector_status == NodeStatuses.ERROR.value else ""                
        else:
            _node_status = NodeStatuses.PAUSE.value
            if _node_status != self._current_ros_ejector_status:
                self._current_ros_ejector_status = _node_status
                
        _ros_vision = {
            "ros_command": "start",
            "config_hash": {"hello": "wow"},
            "status": self._current_ros_ejector_status,
            "info": _info_msg,
            "error": _error_msg
        }
        return _ros_vision

    def generate_out_of_frame(self) -> str:
        '''will only generate randomly when batch_id is not None'''
        if self._batch_id is not None:
            _out_of_frame_start_time = self._batch_start_time if self._prev_out_of_frame_end_time is None else self._prev_out_of_frame_end_time
            _out_of_frame_end_time = datetime.now(timezone.utc)
            self._prev_out_of_frame_end_time = _out_of_frame_end_time
            _out_of_frame_count = randrange(400,1000)
            _broke = randrange(int(_out_of_frame_count/10))
            _hole = randrange(int(_out_of_frame_count/10))
            _dent = randrange(int(_out_of_frame_count/10))
            _dirty = randrange(int(_out_of_frame_count/10))
            _seed = randrange(int(_out_of_frame_count/10))
            _fracture = randrange(int(_out_of_frame_count/10))
            _fungus = randrange(int(_out_of_frame_count/10))
            _defect_count = _broke + _hole + _dent + _dirty + _seed + _fracture + _fungus
            _avg_height = randrange(500, 1000)/10
            _avg_width = randrange(500, 1000)/10
            _stdv_height = randrange(20,80)/10
            _stdv_width = randrange(20,80)/10
            return json.dumps(
                {
                    "payload_name": "longan_out_of_frame",
                    "machine_id": self._machine_id,
                    "local_batch_id": self._batch_id,
                    "out_of_frame_start_time": self.datetime_to_str(_out_of_frame_start_time),
                    "out_of_frame_end_time": self.datetime_to_str(_out_of_frame_end_time),
                    "out_of_frame_count": _out_of_frame_count,
                    "defect_count": _defect_count,
                    "broke_count": _broke,
                    "hole_count": _hole,
                    "dent_count": _dent,
                    "dirty_count": _dirty,
                    "seed_count": _seed,
                    "fracture_count": _fracture,
                    "fungus_count": _fungus,
                    "avg_height": _avg_height,
                    "avg_width": _avg_width,
                    "stdv_height": _stdv_height,
                    "stdv_width": _stdv_width
                }
            )
        return None

    def generate_box_completed_count(self) -> str:
        '''will only generate randomly when batch_id is not None'''
        if self._batch_id is not None:
            _box_started_time = self._batch_start_time if self._prev_box_completed_time is None else self._prev_box_completed_time
            _box_completed_time = datetime.now(timezone.utc)
            return json.dumps(
                {
                    "payload_name": "longan_box_completed",
                    "machine_id": self._machine_id,
                    "local_batch_id": self._batch_id,
                    "box_started_time": self.datetime_to_str(_box_started_time),
                    "box_completed_time": self.datetime_to_str(_box_completed_time)
                }
            )
        pass

class SorterMachineSimulator(SorterMachineDataGenerator, SorterMachineRedis):
    def __init__(self, redis_server_url: str, redis_port:int, username: str, password: str, db:int=0) -> None:
        SorterMachineRedis.__init__(self, redis_server_url, redis_port, username, password)
        SorterMachineDataGenerator.__init__(self)
        self._current_local_command = None
        self._machine_id = MACHINE_ID
        self._machine_feedbacks_record_holder = []
        self._local_commands_record_holder = []
        self._batches_info_record_holder = []
        self._aggr_out_of_frames_record_holder = []
        self._box_counds_info_holder = []
        

    def write_to_redis(self):
        if self.check_heartbeat_from_redis(str(self._cloud_redis_heartbeat)):
            self.write_heartbeat_to_redis(str(self._machine_redis_heartbeat))
        self.write_event_stream_data_to_redis(f"machineData:{self._machine_id}", self.generate_machine_feedback(), self._machine_feedbacks_record_holder)
        self.write_event_stream_data_to_redis(f"machineData:{self._machine_id}", self.get_current_local_command(), self._local_commands_record_holder)
        self.write_event_stream_data_to_redis(f"machineData:{self._machine_id}", self.generate_batch_info(), self._batches_info_record_holder)
        self.write_event_stream_data_to_redis(f"machineData:{self._machine_id}", self.generate_out_of_frame(), self._aggr_out_of_frames_record_holder)
        self.write_event_stream_data_to_redis(f"machineData:{self._machine_id}", self.generate_box_completed_count(), self._box_counds_info_holder)

    def cleanup(self):
        print("cleanup func get called.")
        self.end_batch(self._batch_id)
        self.write_event_stream_data_to_redis(f"machineData:{self._machine_id}", self.generate_batch_info(), self._batches_info_record_holder)
        pass

    @classmethod
    def register_cleanup(cls, instance):
        atexit.register(instance.cleanup)


async def main():
    sorter_machine_simulator = SorterMachineSimulator(redis_server_url=REDIS_SERVER_URL, redis_port=REDIS_PORT, username=USERNAME, password=PASSWORD)
    SorterMachineSimulator.register_cleanup(sorter_machine_simulator)
    try:
        sorter_machine_simulator.write_heartbeat_to_redis(str(sorter_machine_simulator._machine_redis_heartbeat))
        while True:
            await sorter_machine_simulator.generate_local_command()
            sorter_machine_simulator.write_to_redis()
            await asyncio.sleep(MAIN_LOOP_TIME_SEC)
    except KeyboardInterrupt:
        sorter_machine_simulator.cleanup()

if __name__=="__main__":
    asyncio.run(main())