import logging
from functools import wraps
from src.crud.crud_job import job as crud_job
from src.schemas.job import JobStatusType
import inspect
import asyncio
from sqlalchemy import text
from src.schemas.error import TimeoutError, JobKilledError
from src.core.config import settings
from src.schemas.layer import LayerType, UserDataTable
from src.schemas.error import ERROR_MAPPING

# Create a logger object for background tasks
background_logger = logging.getLogger("Background task")
background_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "\033[92m%(levelname)s\033[0m: %(asctime)s %(name)s %(message)s"
)
handler.setFormatter(formatter)
background_logger.addHandler(handler)


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
        # Get the delete orphan, delete temp tables function from class
        delete_orphan_func = getattr(instance, "delete_orphan_data", None)
        delete_temp_tables_func = getattr(instance, "delete_temp_tables", None)
        delete_created_layers = getattr(instance, "delete_created_layers", None)
        # Run delete orphan function
        await delete_orphan_func()
        # Run delete temp tables function
        await delete_temp_tables_func()
        # Delete all layers created by the job
        await delete_created_layers()


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
            background_logger.info(f"Job {str(job_id)} started.")
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
                # Roll back the transaction
                await async_session.rollback()
                # Run failure functions for cleanup
                await run_failure_func(self, func, *args, **kwargs)
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

            background_logger.info(f"Job {job_id} finished.")
            return result

        return wrapper

    return decorator


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
                msg_text = "Job was killed."
                background_logger.error(msg_text)
                return {"status": JobStatusType.killed.value, "msg": msg_text}

            # Execute function
            try:
                result = await asyncio.wait_for(func(*args, **kwargs), timeout)
            except asyncio.TimeoutError:
                # Roll back the transaction
                await async_session.rollback()
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
                background_logger.error(msg_text)
                raise TimeoutError(msg_text)
            except Exception as e:
                # Roll back the transaction
                await async_session.rollback()
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
                background_logger.error(f"Job failed with error: {e}")
                raise e

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
                # Roll back the transaction
                await async_session.rollback()
                # Run failure function if exists
                await run_failure_func(self, func, *args, **kwargs)
                msg_txt = "Job was killed"
                print(msg_txt)
                raise JobKilledError(msg_txt)

            background_logger.info(f"Job step {job_step_name} finished successfully.")
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


class CRUDFailedJob:
    """CRUD class that bundles functions for failed jobs"""

    def __init__(self, job_id, background_tasks, async_session, user_id):
        self.job_id = job_id
        self.background_tasks = background_tasks
        self.async_session = async_session
        self.user_id = user_id

    async def delete_orphan_data(self):
        """Delete orphan data from user tables"""

        # Get user_id
        user_id = self.user_id

        for table in UserDataTable:
            table_name = f"{table.value}_{str(user_id).replace('-', '')}"

            # Build condition for layer filtering
            if table == UserDataTable.no_geometry:
                condition = f"WHERE l.type = '{LayerType.table.value}'"
            else:
                condition = f"WHERE l.feature_layer_geometry_type = '{table.value}'"

            # Delete orphan data that don't exists in layer table and check for data not older then 30 minuts
            sql_delete_orphan_data = f"""
            WITH layer_ids_to_check AS (
                SELECT DISTINCT layer_id
                FROM {settings.USER_DATA_SCHEMA}."{table_name}"
                WHERE created_at > CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '30 minutes'
            ),
            to_delete AS (
                SELECT x.layer_id
                FROM layer_ids_to_check x
                LEFT JOIN
                (
                    SELECT l.id
                    FROM {settings.CUSTOMER_SCHEMA}.layer l
                    {condition}
                    AND l.user_id = '{str(user_id)}'
                ) l
                ON x.layer_id = l.id
                WHERE l.id IS NULL
            )
            DELETE FROM {settings.USER_DATA_SCHEMA}."{table_name}" x
            USING to_delete d
            WHERE x.layer_id = d.layer_id;
            """
            await self.async_session.execute(text(sql_delete_orphan_data))
            await self.async_session.commit()
        return

    async def delete_temp_tables(self):
        # Get all tables that end with the job id
        sql = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'temporal'
            AND table_name LIKE '%{str(self.job_id).replace('-', '')}'
        """
        res = await self.async_session.execute(text(sql))
        tables = res.fetchall()
        # Delete all tables
        for table in tables:
            await self.async_session.execute(
                f"DROP TABLE IF EXISTS temporal.{table[0]}"
            )
        await self.async_session.commit()

    async def delete_created_layers(self):
        # Delete all layers with the self.job_id
        sql = f"""
            DELETE FROM {settings.CUSTOMER_SCHEMA}.layer
            WHERE job_id = '{str(self.job_id)}'
        """
        await self.async_session.execute(text(sql))
        await self.async_session.commit()
