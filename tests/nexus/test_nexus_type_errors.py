"""
This file exists to test for type-checker false positives and false negatives.
It doesn't contain any test functions.
"""

from dataclasses import dataclass

import nexusrpc

import temporalio.nexus
from temporalio import workflow


@dataclass
class MyInput:
    pass


@dataclass
class MyOutput:
    pass


@workflow.defn
class MyNoArgProcWorkflow:
    @workflow.run
    async def run(self) -> None:
        pass


@workflow.defn
class MyOneArgProcWorkflow:
    @workflow.run
    async def run(self, _input: MyInput) -> None:
        pass


@workflow.defn
class MyTwoArgProcWorkflow:
    @workflow.run
    async def run(self, _input: MyInput, _arg2: int) -> None:
        pass


@workflow.defn
class MyThreeArgProcWorkflow:
    @workflow.run
    async def run(self, _input: MyInput, _arg2: int, _arg3: int) -> None:
        pass


@workflow.defn
class MyFourArgProcWorkflow:
    @workflow.run
    async def run(self, _input: MyInput, _arg2: int, _arg3: int, _arg4: int) -> None:
        pass


@workflow.defn
class MyFiveArgProcWorkflow:
    @workflow.run
    async def run(
        self, _input: MyInput, _arg2: int, _arg3: int, _arg4: int, _arg5: int
    ) -> None:
        pass


@nexusrpc.service
class MyService:
    my_sync_operation: nexusrpc.Operation[MyInput, MyOutput]
    my_workflow_run_operation: nexusrpc.Operation[MyInput, MyOutput]
    my_temporal_operation: nexusrpc.Operation[int, None]


@nexusrpc.handler.service_handler(service=MyService)
class MyServiceHandler:
    @nexusrpc.handler.sync_operation
    async def my_sync_operation(
        self, _ctx: nexusrpc.handler.StartOperationContext, _input: MyInput
    ) -> MyOutput:
        raise NotImplementedError

    @temporalio.nexus.workflow_run_operation
    async def my_workflow_run_operation(
        self, _ctx: temporalio.nexus.WorkflowRunOperationContext, _input: MyInput
    ) -> temporalio.nexus.WorkflowHandle[MyOutput]:
        raise NotImplementedError

    @temporalio.nexus.temporal_operation
    async def my_temporal_operation(
        self,
        _ctx: nexusrpc.handler.StartOperationContext,
        client: temporalio.nexus.TemporalNexusClient,
        input: int,
    ) -> temporalio.nexus.TemporalOperationResult[None]:
        """
        Typed proc workflow starts from a generic Temporal Nexus operation handler
        infer TemporalOperationResult[None] for 0 to 5 workflow parameters.
        """
        if input == 0:
            result_0: temporalio.nexus.TemporalOperationResult[
                None
            ] = await client.start_workflow(MyNoArgProcWorkflow.run, id="proc-0")
            return result_0
        if input == 1:
            result_1: temporalio.nexus.TemporalOperationResult[
                None
            ] = await client.start_workflow(
                MyOneArgProcWorkflow.run, MyInput(), id="proc-1"
            )
            return result_1
        if input == 2:
            result_2: temporalio.nexus.TemporalOperationResult[
                None
            ] = await client.start_workflow(
                MyTwoArgProcWorkflow.run, args=[MyInput(), 2], id="proc-2"
            )
            return result_2
        if input == 3:
            result_3: temporalio.nexus.TemporalOperationResult[
                None
            ] = await client.start_workflow(
                MyThreeArgProcWorkflow.run,
                args=[MyInput(), 2, 3],
                id="proc-3",
            )
            return result_3
        if input == 4:
            result_4: temporalio.nexus.TemporalOperationResult[
                None
            ] = await client.start_workflow(
                MyFourArgProcWorkflow.run,
                args=[MyInput(), 2, 3, 4],
                id="proc-4",
            )
            return result_4
        if input == 5:
            result_5: temporalio.nexus.TemporalOperationResult[
                None
            ] = await client.start_workflow(
                MyFiveArgProcWorkflow.run,
                args=[MyInput(), 2, 3, 4, 5],
                id="proc-5",
            )
            return result_5
        # assert-type-error-pyright: 'No overloads for "start_workflow" match'
        return await client.start_workflow(  # type: ignore
            MyOneArgProcWorkflow.run,
            # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
            "wrong-input-type",  # type: ignore
            id="proc-wrong-input",
        )


@nexusrpc.handler.service_handler(service=MyService)
class MyServiceHandler2:
    @nexusrpc.handler.sync_operation
    async def my_sync_operation(
        self, _ctx: nexusrpc.handler.StartOperationContext, _input: MyInput
    ) -> MyOutput:
        raise NotImplementedError

    @temporalio.nexus.workflow_run_operation
    async def my_workflow_run_operation(
        self, _ctx: temporalio.nexus.WorkflowRunOperationContext, _input: MyInput
    ) -> temporalio.nexus.WorkflowHandle[MyOutput]:
        raise NotImplementedError

    @temporalio.nexus.temporal_operation
    async def my_temporal_operation(
        self,
        _ctx: nexusrpc.handler.StartOperationContext,
        _client: temporalio.nexus.TemporalNexusClient,
        _input: int,
    ) -> temporalio.nexus.TemporalOperationResult[None]:
        raise NotImplementedError


@nexusrpc.handler.service_handler
class MyServiceHandlerWithoutServiceDefinition:
    @nexusrpc.handler.sync_operation
    async def my_sync_operation(
        self, _ctx: nexusrpc.handler.StartOperationContext, _input: MyInput
    ) -> MyOutput:
        raise NotImplementedError

    @temporalio.nexus.workflow_run_operation
    async def my_workflow_run_operation(
        self, _ctx: temporalio.nexus.WorkflowRunOperationContext, _input: MyInput
    ) -> temporalio.nexus.WorkflowHandle[MyOutput]:
        raise NotImplementedError

    @temporalio.nexus.temporal_operation
    async def my_temporal_operation(
        self,
        _ctx: nexusrpc.handler.StartOperationContext,
        _client: temporalio.nexus.TemporalNexusClient,
        _input: int,
    ) -> temporalio.nexus.TemporalOperationResult[None]:
        raise NotImplementedError


@workflow.defn
class MyWorkflow1:
    @workflow.run
    async def test_invoke_by_operation_definition_happy_path(self) -> None:
        """
        When a nexus client  calls an operation by referencing an operation definition on
        a service definition, the output type is inferred correctly.
        """
        nexus_client = workflow.create_nexus_client(
            service=MyService,
            endpoint="fake-endpoint",
        )
        input = MyInput()

        # sync operation
        _output_1: MyOutput = await nexus_client.execute_operation(
            MyService.my_sync_operation, input
        )
        _handle_1: workflow.NexusOperationHandle[
            MyOutput
        ] = await nexus_client.start_operation(MyService.my_sync_operation, input)
        _output_1_1: MyOutput = await _handle_1

        # workflow run operation
        _output_2: MyOutput = await nexus_client.execute_operation(
            MyService.my_workflow_run_operation, input
        )
        _handle_2: workflow.NexusOperationHandle[
            MyOutput
        ] = await nexus_client.start_operation(
            MyService.my_workflow_run_operation, input
        )
        _output_2_1: MyOutput = await _handle_2

        # temporal operation
        _output_3: None = await nexus_client.execute_operation(  # type: ignore
            MyService.my_temporal_operation, 0
        )
        _handle_3: workflow.NexusOperationHandle[
            None
        ] = await nexus_client.start_operation(MyService.my_temporal_operation, 0)
        _output_3_1: None = await _handle_3  # type: ignore


@workflow.defn
class MyWorkflow2:
    @workflow.run
    async def test_invoke_by_operation_handler_happy_path(self) -> None:
        """
        When a nexus client calls an operation by referencing an operation handler on a
        service handler, the output type is inferred correctly.
        """
        nexus_client = workflow.create_nexus_client(
            service=MyServiceHandler,  # MyService would also work
            endpoint="fake-endpoint",
        )
        input = MyInput()

        # sync operation
        _output_1: MyOutput = await nexus_client.execute_operation(
            MyServiceHandler.my_sync_operation, input
        )
        _handle_1: workflow.NexusOperationHandle[
            MyOutput
        ] = await nexus_client.start_operation(
            MyServiceHandler.my_sync_operation, input
        )
        _output_1_1: MyOutput = await _handle_1

        # workflow run operation
        _output_2: MyOutput = await nexus_client.execute_operation(
            MyServiceHandler.my_workflow_run_operation, input
        )
        _handle_2: workflow.NexusOperationHandle[
            MyOutput
        ] = await nexus_client.start_operation(
            MyServiceHandler.my_workflow_run_operation, input
        )
        _output_2_1: MyOutput = await _handle_2

        # temporal operation
        _output_3: None = await nexus_client.execute_operation(  # type: ignore
            MyServiceHandler.my_temporal_operation, 0
        )
        _handle_3: workflow.NexusOperationHandle[
            None
        ] = await nexus_client.start_operation(
            MyServiceHandler.my_temporal_operation, 0
        )
        _output_3_1: None = await _handle_3  # type: ignore


@workflow.defn
class MyWorkflow3:
    @workflow.run
    async def test_invoke_by_operation_definition_wrong_input_type(self) -> None:
        """
        When a nexus client calls an operation by referencing an operation definition on
        a service definition, there is a type error if the input type is wrong.
        """
        nexus_client = workflow.create_nexus_client(
            service=MyService,
            endpoint="fake-endpoint",
        )
        # assert-type-error-pyright: 'No overloads for "execute_operation" match'
        await nexus_client.execute_operation(  # type: ignore
            MyService.my_sync_operation,
            # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter "input"'
            "wrong-input-type",  # type: ignore
        )
        # assert-type-error-pyright: 'No overloads for "execute_operation" match'
        await nexus_client.execute_operation(  # type: ignore
            MyService.my_temporal_operation,
            # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter "input"'
            "wrong-input-type",  # type: ignore
        )


@workflow.defn
class MyWorkflow4:
    @workflow.run
    async def test_invoke_by_operation_handler_wrong_input_type(self) -> None:
        """
        When a nexus client calls an operation by referencing an operation handler on a
        service handler, there is a type error if the input type is wrong.
        """
        nexus_client = workflow.create_nexus_client(
            service=MyServiceHandler,
            endpoint="fake-endpoint",
        )
        # assert-type-error-pyright: 'No overloads for "execute_operation" match'
        await nexus_client.execute_operation(  # type: ignore
            MyServiceHandler.my_sync_operation,  # type: ignore[arg-type]
            # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter "input"'
            "wrong-input-type",  # type: ignore
        )
        # assert-type-error-pyright: 'No overloads for "execute_operation" match'
        await nexus_client.execute_operation(  # type: ignore
            MyServiceHandler.my_temporal_operation,  # type: ignore[arg-type]
            # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter "input"'
            "wrong-input-type",  # type: ignore
        )


@workflow.defn
class MyWorkflow5:
    @workflow.run
    async def test_invoke_by_operation_handler_method_on_wrong_service(self) -> None:
        """
        When a nexus client calls an operation by referencing an operation handler method
        on a service handler, there is a type error if the method does not belong to the
        service for which the client was created.

        (This form of type safety is not available when referencing an operation definition)
        """
        nexus_client = workflow.create_nexus_client(
            service=MyServiceHandler,
            endpoint="fake-endpoint",
        )
        # assert-type-error-pyright: 'No overloads for "execute_operation" match'
        await nexus_client.execute_operation(  # type: ignore
            # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter "operation"'
            MyServiceHandler2.my_sync_operation,  # type: ignore
            MyInput(),
        )
