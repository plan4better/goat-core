from functools import wraps
from src.crud.crud_job import job as crud_job
from src.db.models.job import Job
from src.schemas.job import JobType, job_mapping
from datetime import datetime
from sqlalchemy import text
import json


def job_log(job_step_name: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get async_session
            self = args[0] if args else None
            async_session = self.async_session if self else None

            # Get job
            job_id = kwargs["job_id"]
            job = await async_session.execute(
                text(
                    f"""
                SELECT status FROM customer.job WHERE id = '{str(job_id)}' """
                )
            )
            status_dict = job.all()[0][0]

            # Label job as running
            status_dict[job_step_name]["status"] = "running"
            status_dict[job_step_name]["timestamp_start"] = str(datetime.now())

            # TODO: Use the models instead of text. Note: The models are with the dict as a payload in this case. It simply did not execute without any error.
            # It worked only when passing two out of three key-value pairs in the case of layer_upload job.

            # Update job status
            job = await async_session.execute(
                text(
                    f"""
                UPDATE customer.job SET status = '{json.dumps(status_dict)}'::jsonb WHERE id = '{str(job_id)}' 
                RETURNING status"""
                )
            )
            await async_session.commit()
            job_status = job.all()[0][0]

            # Execute function
            result = await func(*args, **kwargs)

            # Get relevant job step
            job_step = job_status[job_step_name]
            # Update job step
            job_step["status"] = result["status"]
            job_step["timestamp_end"] = str(datetime.now())
            # TODO: Get rid of this ugly type conversion
            job_step["msg"] = {
                "type": result["msg"].type.value,
                "text": result["msg"].text,
            }

            # Update job
            job_status[job_step_name] = job_step
            job = await async_session.execute(
                text(
                    f"""
                UPDATE customer.job SET status = '{json.dumps(job_status)}'::jsonb WHERE id = '{str(job_id)}'
                RETURNING status 
                """
                )
            )
            await async_session.commit()
            job_status = job.all()[0][0]

            return result

        return wrapper

    return decorator
