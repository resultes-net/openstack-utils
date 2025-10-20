import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import contextlib as _ctx
import logging as _log
import pathlib as _pl
import threading as _thread
import types as _tps
import typing as _tp

import resultes_openstack_utils.swift as _swift
import resultes_pydantic_models.runner as _mrunner
import swiftclient as _sclient

_LOGGER = _log.getLogger(__name__)


class _QueueShutDown:
    pass


_ChunksQueue = _asyncio.Queue[bytes | _QueueShutDown]


class Swift(_ctx.AbstractAsyncContextManager["Swift"]):
    def __init__(
        self, clouds_yaml_file_path: _pl.Path, executor: _cf.Executor, max_workers: int
    ) -> None:
        self._clouds_yaml_file_path = clouds_yaml_file_path
        self._executor = executor
        self._max_workers = max_workers
        self._shutdown_event = _thread.Event()

    async def __aenter__(self) -> _tp.Self:
        self._connections_contexts = {
            _swift.create_connection(self._clouds_yaml_file_path)
            for _ in range(self._max_workers)
        }
        self._free_connections = _asyncio.Queue[_sclient.Connection](
            maxsize=self._max_workers
        )
        for context in self._connections_contexts:
            connection = context.__enter__()
            await self._free_connections.put(connection)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: _tps.TracebackType | None,
    ) -> bool:
        self._shutdown_event.set()

        self._executor.shutdown(wait=True)

        for context in self._connections_contexts:
            context.__exit__(exc_type, exc_value, traceback)

        return False

    @_ctx.asynccontextmanager
    async def _free_connection(self) -> _cabc.AsyncIterator[_sclient.Connection]:
        connection = await self._free_connections.get()
        yield connection
        await self._free_connections.put(connection)

    async def download(
        self,
        object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
        output_file_path: _pl.Path,
    ) -> None:
        _LOGGER.info(
            "Downloading %s to %s...", object_storage_input_file_path, output_file_path
        )

        await self._run_in_executor_with_connection(
            self._download,
            object_storage_input_file_path,
            output_file_path,
        )

        _LOGGER.info("Done.")

    def _download(
        self,
        object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
        output_file_path: _pl.Path,
        connection: _sclient.Connection,
    ) -> None:
        chunks = _swift.download_object_storage_chunks(
            object_storage_input_file_path,
            connection,
        )

        with output_file_path.open("wb") as output_file:
            for chunk in chunks:
                if self._shutdown_event.is_set():
                    break

                output_file.write(chunk)

                _LOGGER.debug("Wrote chunk of size %i byte(s).", len(chunk))

    async def download_chunks(
        self,
        object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
    ) -> _cabc.AsyncIterator[bytes]:
        _LOGGER.info("Downloading %s to chunks...", object_storage_input_file_path)

        queue = _ChunksQueue()
        loop = _asyncio.get_running_loop()

        reader_task = self._create_reader_task(
            object_storage_input_file_path, queue, loop
        )

        while True:
            item = await queue.get()

            match item:
                case bytes():
                    _LOGGER.debug("Got chunk of size %i byte(s).", len(item))
                    yield item
                    queue.task_done()
                case _QueueShutDown():
                    queue.task_done()
                    break

        await reader_task

        _LOGGER.info("Done.")

    def _create_reader_task(
        self,
        input_object_storage_path: _mrunner.ObjectStorageInputFilePath,
        queue: _ChunksQueue,
        loop: _asyncio.AbstractEventLoop,
    ) -> _asyncio.Task[None]:
        coroutine = self._run_in_executor_with_connection(
            self._download_chunks,
            input_object_storage_path,
            queue,
            loop,
        )

        task = _asyncio.create_task(coroutine)

        return task

    def _download_chunks(
        self,
        input_object_storage_path: _mrunner.ObjectStorageInputFilePath,
        queue: _ChunksQueue,
        loop: _asyncio.AbstractEventLoop,
        connection: _sclient.Connection,
    ) -> None:
        chunks = _swift.download_object_storage_chunks(
            input_object_storage_path,
            connection,
        )

        for chunk in chunks:
            _LOGGER.debug("Putting chunk of %i byte(s) onto queue.", len(chunk))
            loop.call_soon_threadsafe(queue.put_nowait, chunk)

        loop.call_soon_threadsafe(queue.put_nowait, _QueueShutDown())

    async def upload(
        self,
        input_file_path: _pl.Path,
        object_storage_output_file_path: _mrunner.ObjectStorageOutputFilePath,
    ) -> None:
        _LOGGER.info(
            "Uploading %s to %s...", input_file_path, object_storage_output_file_path
        )

        await self._run_in_executor_with_connection(
            _swift.upload_storage_object,
            input_file_path,
            object_storage_output_file_path,
        )

        _LOGGER.info("Done.")

    async def _run_in_executor_with_connection[*S, T](
        self,
        func: _cabc.Callable[[*S, _sclient.Connection], T],
        *args: *S,
    ) -> T:
        async with self._free_connection() as connection:
            loop = _asyncio.get_running_loop()
            result = await loop.run_in_executor(self._executor, func, *args, connection)
            return result
