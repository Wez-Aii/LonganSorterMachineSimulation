import asyncio
from enum import Enum
import json
import random
import time
import redis
import os

from main import PASSWORD, REDIS_PORT, REDIS_SERVER_URL, USERNAME, SorterMachineRedis


async def main():
    rc_instance = SorterMachineRedis(redis_server_url=REDIS_SERVER_URL, redis_port=REDIS_PORT, username=USERNAME, password=PASSWORD)
    try:
        while True:
            if rc_instance.check_heartbeat_from_redis(str(rc_instance._machine_redis_heartbeat)):
                rc_instance.write_heartbeat_to_redis(str(rc_instance._cloud_redis_heartbeat))
            rc_instance.read_redis_payloads(f"feedback:{rc_instance._machine_id}")
            _commands = rc_instance.read_redis_payloads(f"localCommand:{rc_instance._machine_id}")
            _batch = rc_instance.read_redis_payloads(f"batch:{rc_instance._machine_id}")
            _boxCount = rc_instance.read_redis_payloads(f"boxCount:{rc_instance._machine_id}")
            _outOfFrame = rc_instance.read_redis_payloads(f"outOfFrame:{rc_instance._machine_id}")
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        rc_instance.cleanup()


if __name__=="__main__":
    asyncio.run(main())