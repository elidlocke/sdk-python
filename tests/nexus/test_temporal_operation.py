import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import nexusrpc
import pytest
from nexusrpc import HandlerErrorType, Operation, service
from nexusrpc.handler import (
    StartOperationContext,
    service_handler,
)


import temporalio.exceptions
from temporalio import nexus, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from tests.helpers import EventType, assert_event_subsequence
from tests.helpers.nexus import make_nexus_endpoint_name


@dataclass
class Input:
    value: str
    task_queue: str


@workflow.defn
class EchoWorkflow:
    @workflow.run
    async def run(self, input: Input) -> str:
        return input.value


@service
class TestService:
    echo: Operation[Input, str]
    blocking: Operation[None, None]
    double_start: Operation[Input, None]
    sync_result: Operation[Input, str]


@service_handler(service=TestService)
class EchoServiceHandler:
    @nexus.temporal_operation
    async def echo(
        self,
        _ctx: StartOperationContext,
        client: nexus.TemporalNexusClient,
        input: Input,
    ) -> nexus.TemporalOperationResult[str]:
        return await client.start_workflow(
            EchoWorkflow.run, input, id=f"echo-{input.value}"
        )

    @nexus.temporal_operation
    async def blocking(
        self,
        _ctx: StartOperationContext,
        client: nexus.TemporalNexusClient,
        _input: None,
    ) -> nexus.TemporalOperationResult[None]:
        return await client.start_workflow(
            BlockingWorkflow.run, id=f"blocking-{uuid.uuid4}"
        )

    @nexus.temporal_operation
    async def double_start(
        self,
        _ctx: StartOperationContext,
        client: nexus.TemporalNexusClient,
        input: Input,
    ) -> nexus.TemporalOperationResult[None]:
        await client.start_workflow(
            EchoWorkflow.run, input, id=f"double-start-{uuid.uuid4}"
        )
        await client.start_workflow(
            EchoWorkflow.run, input, id=f"double-start-{uuid.uuid4}"
        )
        return nexus.TemporalOperationResult.sync(None)

    @nexus.temporal_operation
    async def sync_result(
        self,
        _ctx: StartOperationContext,
        _client: nexus.TemporalNexusClient,
        input: Input,
    ) -> nexus.TemporalOperationResult[str]:
        return nexus.TemporalOperationResult.sync(input.value)


@workflow.defn
class EchoWorkflowCaller:
    @workflow.run
    async def run(self, input: Input) -> str:
        client = workflow.create_nexus_client(
            service=TestService, endpoint=make_nexus_endpoint_name(input.task_queue)
        )
        return await client.execute_operation(TestService.echo, input)


async def test_temporal_operation_start_workflow(
    client: Client, env: WorkflowEnvironment
):
    task_queue = str(uuid.uuid4())
    endpoint_name = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint_name, task_queue)
    async with Worker(
        env.client,
        task_queue=task_queue,
        nexus_service_handlers=[EchoServiceHandler()],
        workflows=[EchoWorkflow, EchoWorkflowCaller],
    ):
        wf_handle = await client.start_workflow(
            EchoWorkflowCaller.run,
            Input(value="test", task_queue=task_queue),
            task_queue=task_queue,
            id=str(uuid.uuid4()),
        )
        result = await wf_handle.result()
        assert result == "test"

        await assert_event_subsequence(
            wf_handle,
            [
                EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
                EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED,
                EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
            ],
        )


@workflow.defn
class BlockingWorkflow:
    done: bool = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.update
    async def unblock(self):
        self.done = True


@workflow.defn
class CancelBlockingWorkflowCaller:
    op_started = False

    @workflow.run
    async def run(self, input: Input) -> None:
        client = workflow.create_nexus_client(
            service=TestService, endpoint=make_nexus_endpoint_name(input.task_queue)
        )
        op_handle = await client.start_operation(TestService.blocking, None)
        self.op_started = True
        return await op_handle

    @workflow.update
    async def wait_operation_started(self):
        await workflow.wait_condition(lambda: self.op_started)


async def test_temporal_operation_cancel_workflow(
    client: Client, env: WorkflowEnvironment
):
    task_queue = str(uuid.uuid4())
    endpoint_name = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint_name, task_queue)
    async with Worker(
        env.client,
        task_queue=task_queue,
        nexus_service_handlers=[EchoServiceHandler()],
        workflows=[BlockingWorkflow, CancelBlockingWorkflowCaller],
    ):
        wf_handle = await client.start_workflow(
            CancelBlockingWorkflowCaller.run,
            Input(value="test", task_queue=task_queue),
            task_queue=task_queue,
            id=f"blocking-{uuid.uuid4()}",
        )

        await wf_handle.execute_update(
            CancelBlockingWorkflowCaller.wait_operation_started
        )

        await wf_handle.cancel()

        await assert_event_subsequence(
            wf_handle,
            [
                EventType.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED,
                EventType.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
                EventType.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
            ],
        )


@workflow.defn
class DoubleStartWorkflowCaller:
    @workflow.run
    async def run(self, input: Input) -> None:
        client = workflow.create_nexus_client(
            service=TestService, endpoint=make_nexus_endpoint_name(input.task_queue)
        )
        op_handle = await client.start_operation(TestService.double_start, input)
        return await op_handle


async def test_temporal_operation_double_start_raises_handler_err(
    client: Client, env: WorkflowEnvironment
):
    task_queue = str(uuid.uuid4())
    endpoint_name = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint_name, task_queue)
    async with Worker(
        env.client,
        task_queue=task_queue,
        nexus_service_handlers=[EchoServiceHandler()],
        workflows=[EchoWorkflow, DoubleStartWorkflowCaller],
    ):
        with pytest.raises(WorkflowFailureError) as err:
            await client.execute_workflow(
                DoubleStartWorkflowCaller.run,
                Input(value="test", task_queue=task_queue),
                task_queue=task_queue,
                id=f"double-start-{uuid.uuid4()}",
            )

        assert isinstance(err.value.cause, temporalio.exceptions.NexusOperationError)
        assert isinstance(err.value.cause.cause, nexusrpc.HandlerError)
        assert err.value.cause.cause.type == HandlerErrorType.BAD_REQUEST
        assert (
            "Only one async operation can be started per operation handler invocation"
            in err.value.cause.cause.message
        )


@workflow.defn
class SyncResultCaller:
    @workflow.run
    async def run(self, input: Input) -> str:
        client = workflow.create_nexus_client(
            service=TestService, endpoint=make_nexus_endpoint_name(input.task_queue)
        )
        return await client.execute_operation(TestService.sync_result, input)


async def test_temporal_operation_sync_result(client: Client, env: WorkflowEnvironment):
    task_queue = str(uuid.uuid4())
    endpoint_name = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint_name, task_queue)
    async with Worker(
        env.client,
        task_queue=task_queue,
        nexus_service_handlers=[EchoServiceHandler()],
        workflows=[SyncResultCaller],
    ):
        wf_handle = await client.start_workflow(
            SyncResultCaller.run,
            Input(value="test", task_queue=task_queue),
            task_queue=task_queue,
            id=str(uuid.uuid4()),
        )
        result = await wf_handle.result()
        assert result == "test"

        # Sync results do not produce a NEXUS_OPERATION_STARTED event,
        await assert_event_subsequence(
            wf_handle,
            [
                EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
                EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
            ],
        )
