from functools import wraps
from src.crud.crud_job import job as crud_job
import concurrent.futures
import functools
from src.schemas.job import JobStatusType
import inspect
import asyncio
from src.utils import sanitize_error_message

def job_init():
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get async_session
            async_session = kwargs["async_session"]

            # Get job id
            job_id = kwargs["job_id"]
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
            if result["status"] not in [JobStatusType.killed.value, JobStatusType.timeout.value, JobStatusType.failed.value]:
                job = await crud_job.update(
                    db=async_session,
                    db_obj=job,
                    obj_in={"status_simple": JobStatusType.finished.value},
                )
            return result

        return wrapper
    return decorator

async def run_failure_func(instance, func, **kwargs):
    # Get failure function
    failure_func_name = f"{func.__name__}_fail"  # Construct the failure function name
    failure_func = getattr(instance, failure_func_name, None)  # Get the failure function
    # Run failure function if exists
    if failure_func:
        valid_args = inspect.signature(func).parameters.keys()
        func_args = {k: v for k, v in kwargs.items() if k in valid_args}
        await failure_func(instance.folder_path, **func_args)

def job_log(job_step_name: str, timeout: int = 120):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get async_session
            self = args[0] if args else None
            async_session = self.async_session if self else None

            # Get job
            job_id = kwargs["job_id"]
            job = await crud_job.get(db=async_session, id=job_id)


            # Update job status if not killed
            job = await crud_job.update_status(
                async_session=async_session,
                job_id=job_id,
                status_simple=JobStatusType.running.value,
                job_step_name=job_step_name,
            )

            # Exit if job is killed before starting
            if job.status_simple == JobStatusType.killed.value:
                await run_failure_func(self, func, **kwargs)
                return

            # Execute function
            try:
                result = await asyncio.wait_for(func(*args, **kwargs), timeout)
            except asyncio.TimeoutError:
                # Handle the timeout here. For example, you can raise a custom exception or log it.
                await run_failure_func(self, func, **kwargs)
                # Update job status to indicate timeout
                msg_text = f"Job timed out after {timeout} seconds."
                job = await crud_job.update_status(
                    async_session=async_session,
                    job_id=job_id,
                    status_simple=JobStatusType.timeout.value,
                    msg_text=f"Job timed out after {timeout} seconds.",
                    job_step_name=job_step_name,
                )
                return {
                    "status": JobStatusType.timeout.value,
                    "msg": msg_text
                }
            except Exception as e:
                # Run failure function if exists
                await run_failure_func(self, func, **kwargs)
                # Update job status simple to failed
                job = await crud_job.update_status(
                    async_session=async_session,
                    job_id=job_id,
                    status_simple=JobStatusType.failed.value,
                    msg_text=str(e),
                    job_step_name=job_step_name,
                )
                return  {
                    "status": JobStatusType.failed.value,
                    "msg": sanitize_error_message(str(e))
                }

            status_simple = result["status"]
            if status_simple == JobStatusType.killed.value:
                msg_text = f"Job was killed after {timeout} seconds."
            elif status_simple == JobStatusType.failed.value:
                msg_text = "Job failed."
            elif status_simple == JobStatusType.finished.value:
                msg_text = "Job finished successfully."

            # Update job status if successful
            job = await crud_job.update_status(
                async_session=async_session,
                job_id=job_id,
                status_simple=status_simple,
                job_step_name=job_step_name,
                msg_text=msg_text,
            )
            # Check if job is killed and run failure function if exists
            if job.status_simple in [JobStatusType.killed.value, JobStatusType.failed.value]:
                await run_failure_func(self, func, **kwargs)
                return

            return result

        return wrapper

    return decorator


def timeout(seconds):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(
                        f"Function {func.__name__} exceeded {seconds} seconds timeout."
                    )

        return wrapper

    return decorator

def run_background_or_immediately(settings):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            background_tasks = kwargs["background_tasks"]
            if settings.RUN_AS_BACKGROUND_TASK is False:
                return await func(*args, **kwargs)
            else:
                return background_tasks.add_task(func, *args, **kwargs)
        return wrapper
    return decorator
