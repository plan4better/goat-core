from functools import wraps
from src.crud.crud_job import job as crud_job
from src.schemas.job import JobStatusType
import inspect
import asyncio
from src.utils import sanitize_error_message


def job_init():
    def decorator(func, timeout: int = 1):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            self = args[0]
            # Check if job_id and async_session are provided in kwards else search them in the class
            if kwargs.get("async_session"):
                async_session = kwargs["async_session"]
            else:
                async_session = self.async_session

            if kwargs.get("job_id"):
                job_id = kwargs["job_id"]
            else:
                job_id = self.job_id

            # Get job id
            job = await crud_job.get(db=async_session, id=job_id)
            job = await crud_job.update(
                db=async_session,
                db_obj=job,
                obj_in={"status_simple": JobStatusType.running.value},
            )

            # Execute function
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                # Update job status simple to failed
                job = await crud_job.update(
                    db=async_session,
                    db_obj=job,
                    obj_in={"status_simple": JobStatusType.failed.value},
                )
                return

            # Update job status to finished in case it is not killed, timeout or failed
            if result["status"] not in [
                JobStatusType.killed.value,
                JobStatusType.timeout.value,
                JobStatusType.failed.value,
            ]:
                job = await crud_job.update(
                    db=async_session,
                    db_obj=job,
                    obj_in={"status_simple": JobStatusType.finished.value},
                )
            return result

        return wrapper

    return decorator


async def run_failure_func(instance, func, *args, **kwargs):
    # Get failure function
    failure_func_name = f"{func.__name__}_fail"  # Construct the failure function name
    failure_func = getattr(
        instance, failure_func_name, None
    )  # Get the failure function
    # Run failure function if exists
    if failure_func:
        # Merge args and kwargs
        args_dict = vars(args[0]) if args else {}
        args_check = {**args_dict, **kwargs}
        # Check for valid args
        valid_args = inspect.signature(failure_func).parameters.keys()
        func_args = {k: v for k, v in args_check.items() if k in valid_args}
        try:
            await failure_func(**func_args)
        except Exception as e:
            print(f"Failure function {failure_func_name} failed with error: {e}")
    else:
        print(f"Failure function {failure_func_name} does not exist.")

def job_log(job_step_name: str, timeout: int = 120):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get async_session
            self = args[0] if args else None
            # Check if job_id and async_session are provided in kwards else search them in the class
            if kwargs.get("job_id"):
                job_id = kwargs["job_id"]
            else:
                job_id = self.job_id

            # Get async_session
            if kwargs.get("async_session"):
                async_session = kwargs["async_session"]
            else:
                async_session = self.async_session

            # Update job status to indicate running
            job = await crud_job.update_status(
                async_session=async_session,
                job_id=job_id,
                status=JobStatusType.running.value,
                msg_text="Job is running.",
                job_step_name=job_step_name,
            )

            # Exit if job is killed before starting
            if job.status_simple == JobStatusType.killed.value:
                await run_failure_func(self, func, **kwargs)
                return {"status": JobStatusType.killed.value, "msg": "Job was killed."}

            # Execute function
            try:
                result = await asyncio.wait_for(func(*args, **kwargs), timeout)
            except asyncio.TimeoutError:
                # Handle the timeout here. For example, you can raise a custom exception or log it.
                await run_failure_func(self, func, *args, **kwargs)
                # Update job status to indicate timeout
                msg_text = f"Job timed out after {timeout} seconds."
                job = await crud_job.update_status(
                    async_session=async_session,
                    job_id=job_id,
                    status=JobStatusType.timeout.value,
                    msg_text=msg_text,
                    job_step_name=job_step_name,
                )
                return {"status": JobStatusType.timeout.value, "msg": msg_text}
            except Exception as e:
                # Run failure function if exists
                await run_failure_func(self, func, *args, **kwargs)
                # Update job status simple to failed
                job = await crud_job.update_status(
                    async_session=async_session,
                    job_id=job_id,
                    status=JobStatusType.failed.value,
                    msg_text=str(e),
                    job_step_name=job_step_name,
                )
                return {
                    "status": JobStatusType.failed.value,
                    "msg": sanitize_error_message(str(e)),
                }

            # Check if job was killed. The job needs to be expired as it was fetching old data from cache.
            async_session.expire(job)
            job = await crud_job.get(db=async_session, id=job_id)

            if job.status_simple == JobStatusType.killed.value:
                status = JobStatusType.killed.value
                msg_text = "Job was killed."
            # Else use the status provided by the function
            else:
                if result["status"] == JobStatusType.failed.value:
                    status = JobStatusType.failed.value
                    msg_text = result["msg"]
                elif result["status"] == JobStatusType.finished.value:
                    status = JobStatusType.finished.value
                    msg_text = "Job finished successfully."
                else:
                    raise ValueError(
                        f"Invalid status {result['status']} returned by function {func.__name__}."
                    )

            # Update job status if successful
            job = await crud_job.update_status(
                async_session=async_session,
                job_id=job_id,
                status=status,
                job_step_name=job_step_name,
                msg_text=msg_text,
            )
            # Check if job is killed and run failure function if exists
            if job.status_simple in [
                JobStatusType.killed.value,
                JobStatusType.failed.value,
            ]:
                await run_failure_func(self, func, *args, **kwargs)
                return {
                    "status": job.status_simple,
                    "msg": job.status[job_step_name]["msg"]["text"],
                }

            return result

        return wrapper

    return decorator


def run_background_or_immediately(settings):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get background tasks either from class or from function kwargs

            if kwargs.get("background_tasks"):
                background_tasks = kwargs["background_tasks"]
            else:
                background_tasks = args[0].background_tasks

            if settings.RUN_AS_BACKGROUND_TASK is False:
                return await func(*args, **kwargs)
            else:
                return background_tasks.add_task(func, *args, **kwargs)

        return wrapper

    return decorator
