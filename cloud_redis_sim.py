import asyncio
from enum import Enum
import json
import random
import time
import redis
import os

from main import PASSWORD, REDIS_PORT, REDIS_SERVER_URL, USERNAME, SorterMachineRedis


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


if __name__=="__main__":
    asyncio.run(main())