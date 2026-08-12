import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import contextlib as _ctx
import functools as _ft
import logging as _log
import pathlib as _pl
import threading as _thread
import types as _tps
import typing as _tp

import resultes_openstack_utils.swift as _swift
import resultes_pydantic_models.runner as _mrunner
import swiftclient as _sclient

_LOGGER = _log.getLogger(__name__)

ClientException = _sclient.ClientException

type AsyncChunks = _cabc.AsyncIterable[bytes]


class _CustomStopIteration(Exception):
    pass


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
        try:
            yield connection
        finally:
            await self._free_connections.put(connection)

    async def get_size_in_bytes(
        self, object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath
    ) -> int:
        return await self._run_in_executor_with_connection(
            _swift.get_size_in_bytes, object_storage_input_file_path
        )

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
        _, chunks = _swift.download_object_storage_chunks(
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
    ) -> tuple[_swift.Headers, AsyncChunks]:
        connection = await self._free_connections.get()
        try:
            headers, chunks = await self._run_in_executor(
                _swift.download_object_storage_chunks,
                object_storage_input_file_path,
                connection,
            )
        except Exception:
            await self._free_connections.put(connection)
            raise

        async_chunks = self._download_chunks(
            chunks, object_storage_input_file_path, connection
        )

        return headers, async_chunks

    async def _download_chunks(
        self,
        chunks: _swift.Chunks,
        object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath,
        connection: _sclient.Connection,
    ) -> AsyncChunks:
        try:
            _LOGGER.info("Downloading %s to chunks...", object_storage_input_file_path)

            iterator = await self._run_in_executor(iter, chunks)
            while True:
                try:
                    chunk = await self._run_in_executor(self._next, iterator)
                    _LOGGER.debug("Got chunk of size %i byte(s).", len(chunk))
                    yield chunk
                except _CustomStopIteration:
                    _LOGGER.info("Done.")
                    break
        except:
            _LOGGER.exception("An error occurred.")
            raise
        finally:
            await self._free_connections.put(connection)

    @staticmethod
    def _next[T](iterator: _cabc.Iterator[T]) -> T:
        try:
            return next(iterator)
        except StopIteration:
            raise _CustomStopIteration()

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

    async def delete(
        self, object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath
    ) -> None:
        _LOGGER.info("Deleting object %s.", object_storage_input_file_path)

        await self._run_in_executor_with_connection(
            _swift.delete_storage_object, object_storage_input_file_path
        )

    async def delete_folder(
        self, object_storage_input_file_path: _mrunner.ObjectStorageInputFilePath
    ) -> None:
        _LOGGER.info("Deleting folder %s.", object_storage_input_file_path)

        await self._run_in_executor_with_connection(
            _swift.delete_folder, object_storage_input_file_path
        )

    async def _run_in_executor_with_connection[*S, T](
        self,
        func: _cabc.Callable[[*S, _sclient.Connection], T],
        *args: *S,
    ) -> T:
        async with self._free_connection() as connection:
            return await self._run_in_executor(func, *args, connection)

    async def _run_in_executor[*S, T](
        self,
        func: _cabc.Callable[[*S], T],
        *args: *S,
    ) -> T:
        loop = _asyncio.get_running_loop()

        @_ft.wraps(func)
        def wrapper(*args: *S) -> T:
            try:
                return func(*args)
            except:
                _LOGGER.exception("An error occurred.")
                raise

        return await loop.run_in_executor(self._executor, wrapper, *args)
