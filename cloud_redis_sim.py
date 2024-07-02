import asyncio
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import logging
import random
import time
import redis
import os

from main import PASSWORD, REDIS_PORT, REDIS_SERVER_URL, USERNAME, SorterMachineRedis
from cryptography.fernet import Fernet
import base64


def generate_key_from_string(custom_string):
    """
    Generate a Fernet key from a custom string.
    The custom string is hashed to ensure it's 32 bytes long.
    """
    custom_string_bytes = custom_string.encode('utf-8')
    # Pad or truncate the custom string to ensure it's 32 bytes long
    custom_string_bytes = custom_string_bytes.ljust(32, b'\0')[:32]
    return base64.urlsafe_b64encode(custom_string_bytes)

def encrypt_dict(data_dict, custom_key):
    """
    Encrypt a dictionary using a custom key.
    """
    key = generate_key_from_string(custom_key)
    fernet = Fernet(key)
    json_data = json.dumps(data_dict).encode('utf-8')
    encrypted_data = fernet.encrypt(json_data)
    return encrypted_data

def decrypt_dict(encrypted_data, custom_key):
    """
    Decrypt a dictionary using a custom key.
    """
    key = generate_key_from_string(custom_key)
    fernet = Fernet(key)
    decrypted_data = fernet.decrypt(encrypted_data)
    return json.loads(decrypted_data.decode('utf-8'))


def print_completed_batch_info(machine_data:dict={}):
    _boxes_infos = machine_data.get("longan_batch",[])
    _boxes_completed = machine_data.get("longan_box_completed",[])
    _aggr_out_of_frame = machine_data.get("longan_out_of_frame",[])
    _remain_batch_infos = []
    _remain_boxes_completed = []
    _remain_aggr_out_of_frame = []
    for _batch in _boxes_infos:
        if _batch.get("batch_ended_at") is not None:
            _batch_id = _batch.get("local_batch_id")
            _batch_out_of_frames_total = 0
            _batch_defected_out_of_frames = 0
            _batch_boxes_completed_count = 0
            for _oof in _aggr_out_of_frame:
                if _oof.get("local_batch_id") == _batch_id:
                    _batch_out_of_frames_total += _oof.get("out_of_frame_count")
                    _batch_defected_out_of_frames += _oof.get("defect_count")
                else:
                    _remain_aggr_out_of_frame.append(_oof)
            machine_data["longan_out_of_frame"] = _remain_aggr_out_of_frame
            for _box in _boxes_completed:
                if _box.get("local_batch_id") == _batch_id:
                    _batch_boxes_completed_count += 1
                else:
                    _remain_boxes_completed.append(_box)
            machine_data["longan_box_completed"] = _remain_boxes_completed
            print(f"Batch {_batch_id} info: total_longan({_batch_out_of_frames_total}), defect_longan({_batch_defected_out_of_frames}), boxes({_batch_boxes_completed_count})")
        else:
            _remain_batch_infos.append(_batch)
    machine_data["longan_batch"] = _remain_batch_infos
    return machine_data

def machine_redis_data_handler(_data_holder:dict, _new_data:list):
    for each in _new_data:
        _dict_data = json.loads(each)
        try:
            _payload_name = _dict_data.pop("payload_name")
            if _data_holder.get(_payload_name, False):
                _data_holder[_payload_name].append(_dict_data)
            else:
                _data_holder[_payload_name] = [_dict_data]
        except Exception as e:
            print(e)
    return _data_holder

async def main():
    rc_instance = SorterMachineRedis(redis_server_url=REDIS_SERVER_URL, redis_port=REDIS_PORT, username=USERNAME, password=PASSWORD)
    _machine_redis_data = {}
    # _batches = []
    # _boxesCompleted = []
    # _outOfFrames = []
    try:
        while True:
            if rc_instance.check_heartbeat_from_redis(str(rc_instance._machine_redis_heartbeat)):
                rc_instance.write_heartbeat_to_redis(str(rc_instance._cloud_redis_heartbeat))
            _temp_list = rc_instance.read_redis_payloads(f"machineData:{rc_instance._machine_id}")
            _machine_redis_data = machine_redis_data_handler(_machine_redis_data, _temp_list)
            _machine_redis_data = print_completed_batch_info(_machine_redis_data)

            # rc_instance.read_redis_payloads(f"feedback:{rc_instance._machine_id}")
            # rc_instance.read_redis_payloads(f"localCommand:{rc_instance._machine_id}")
            # _batches.extend(rc_instance.read_redis_payloads(f"batch:{rc_instance._machine_id}"))
            # _boxesCompleted.extend(rc_instance.read_redis_payloads(f"boxCompleted:{rc_instance._machine_id}"))
            # _outOfFrames.extend(rc_instance.read_redis_payloads(f"outOfFrame:{rc_instance._machine_id}"))
            # print_completed_batch_info(_batches, _boxesCompleted, _outOfFrames)
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        rc_instance.cleanup()

DATETIME_STR_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

def datetime_to_str(dt_object: datetime) -> str:
    _str_format = DATETIME_STR_FORMAT
    _str_dt = None
    try:
        _str_dt = dt_object.strftime(_str_format)
    except Exception as e:
        logging.error(f"(f)datetime_to_str - {e}")
    return _str_dt

def str_to_datetime(str_dt) -> datetime:
    _str_format = DATETIME_STR_FORMAT
    _dt_str = None
    try:
        _dt_str = datetime.strptime(str_dt, _str_format)
    except Exception as e:
        logging.error(f"(f)str_to_datetime - {e}")
    return _dt_str

class ManualCloudRedisDataGenerator(SorterMachineRedis):
    def __init__(self, redis_server_url: str, redis_port:int, username: str, password: str, db:int=0) -> None:
        super().__init__(redis_server_url, redis_port, username, password)
        self._machine_id = "24438387"

    def controller_disable(self):
        _topic = f"controllerDisable:{self._machine_id}"
        _payload = str(False)
        if self.check_redis_connection():
            _current_val = self.redis_connection.get(_topic)
            _current_val = eval(_current_val.decode()) if _current_val is not None else None
            print(f"{_topic} current value - {_current_val}")
            self.redis_connection.set(_topic, _payload)
            print(f"{_topic} value set - {_payload}")
            _updated_val = self.redis_connection.get(_topic)
            _updated_val = eval(_updated_val.decode()) if _updated_val is not None else None
            print(f"{_topic} updated value - {_updated_val}")

    def machine_config(self):
        _topic = f"machineConfig:{self._machine_id}"
        _payload = {}
        self.write_event_stream_data_to_redis()

    def remote_session(self):
        _session_token = "12e0d9b2-9dea-4b07-bc52-8156f681c372"
        # _session_token = None
        _topic = f"machineRemoteSession:{self._machine_id}"
        _payload = json.dumps({
            "remote_session_token": _session_token,
            "session_request_at": datetime_to_str(datetime.now(timezone.utc)),
            "session_request_minute": 25,
            "remote_user_role": "technician",
            "end_session": False
        })
        self.write_event_stream_data_to_redis(_topic, _payload)

    def cloud_command(self):
        _session_token = "12e0d9b2-9dea-4b07-bc52-8156f681c372"
        _topic = f"cloudCommand:{self._machine_id}"
        _payload = {
            # "remote_session_token": _session_token,
            "command_code": "10",
            "user_role": "technician",
            "config": None,
            "commanded_at": datetime_to_str(datetime.now(timezone.utc))
        }
        encrypted_value = encrypt_dict(_payload, self._machine_id)
        self.write_event_stream_data_to_redis(_topic, encrypted_value)
    
    def machine_data(self):
        _topic = "machineData"
        _current_available_data = self.read_redis_payloads(_topic)
        print(f"{_topic} current available data ...")
        for _each in _current_available_data:
            print(_each)


if __name__=="__main__":
    # asyncio.run(main())
    _instance = ManualCloudRedisDataGenerator(redis_server_url=REDIS_SERVER_URL, redis_port=REDIS_PORT, username=USERNAME, password=PASSWORD)
    # _instance.controller_disable()
    # _instance.remote_session()
    _instance.cloud_command()
    _instance.machine_data()