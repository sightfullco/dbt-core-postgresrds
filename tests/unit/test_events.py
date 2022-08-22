# flake8: noqa
from dbt.adapters.reference_keys import _ReferenceKey
from dbt.events.test_types import UnitTestInfo
from dbt.events import AdapterLogger
from dbt.events.functions import event_to_serializable_dict
from dbt.events.types import *
from dbt.events.test_types import *

# from dbt.events.stubs import _CachedRelation, BaseRelation, _ReferenceKey, ParsedModelNode
from dbt.events.base_types import Event, TestLevel, DebugLevel, WarnLevel, InfoLevel, ErrorLevel
from dbt.events.core_proto_messages import NodeInfo, RunResultMsg
from importlib import reload
import dbt.events.functions as event_funcs
import dbt.flags as flags
import inspect
import json
from dbt.contracts.graph.parsed import ParsedModelNode, NodeConfig, DependsOn
from dbt.contracts.files import FileHash
from mashumaro.types import SerializableType
from typing import Generic, TypeVar
import re

# takes in a class and finds any subclasses for it
def get_all_subclasses(cls):
    all_subclasses = []
    for subclass in cls.__subclasses__():
        # If the test breaks because of abcs this list might have to be updated.
        if subclass in [TestLevel, DebugLevel, WarnLevel, InfoLevel, ErrorLevel]:
            continue
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))
    return set(all_subclasses)


class TestAdapterLogger:
    # this interface is documented for adapter maintainers to plug into
    # so we should test that it at the very least doesn't explode.
    def test_basic_adapter_logging_interface(self):
        logger = AdapterLogger("dbt_tests")
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.exception("exception message")
        logger.critical("exception message")

    # python loggers allow deferring string formatting via this signature:
    def test_formatting(self):
        logger = AdapterLogger("dbt_tests")
        # tests that it doesn't throw
        logger.debug("hello {}", "world")

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="hello {}", args=("world",))
        assert "hello world" in event.message()

        # tests that it doesn't throw
        logger.debug("1 2 {}", 3)

        # enters lower in the call stack to test that it formats correctly
        event = AdapterEventDebug(name="dbt_tests", base_msg="1 2 {}", args=(3,))
        assert "1 2 3" in event.message()

        # tests that it doesn't throw
        logger.debug("boop{x}boop")

        # enters lower in the call stack to test that it formats correctly
        # in this case it's that we didn't attempt to replace anything since there
        # were no args passed after the initial message
        event = AdapterEventDebug(name="dbt_tests", base_msg="boop{x}boop", args=())
        assert "boop{x}boop" in event.message()


class TestEventCodes:

    # checks to see if event codes are duplicated to keep codes singluar and clear.
    # also checks that event codes follow correct namming convention ex. E001
    def test_event_codes(self):
        all_concrete = get_all_subclasses(Event)
        all_codes = set()

        for event in all_concrete:
            if not inspect.isabstract(event):
                # must be in the form 1 capital letter, 3 digits
                assert re.match("^[A-Z][0-9]{3}", event.code)
                # cannot have been used already
                assert (
                    event.code not in all_codes
                ), f"{event.code} is assigned more than once. Check types.py for duplicates."
                all_codes.add(event.code)


class TestEventBuffer:
    def setUp(self) -> None:
        flags.EVENT_BUFFER_SIZE = 10
        reload(event_funcs)

    # ensure events are populated to the buffer exactly once
    def test_buffer_populates(self):
        self.setUp()
        event_funcs.fire_event(UnitTestInfo(msg="Test Event 1"))
        event_funcs.fire_event(UnitTestInfo(msg="Test Event 2"))
        event1 = event_funcs.EVENT_HISTORY[-2]
        assert event_funcs.EVENT_HISTORY.count(event1) == 1

    # ensure events drop from the front of the buffer when buffer maxsize is reached
    def test_buffer_FIFOs(self):
        self.setUp()
        event_funcs.EVENT_HISTORY.clear()
        for n in range(1, (flags.EVENT_BUFFER_SIZE + 1)):
            event_funcs.fire_event(UnitTestInfo(msg=f"Test Event {n}"))

        event_full = event_funcs.EVENT_HISTORY[-1]
        assert event_full.code == "Z048"
        assert event_funcs.EVENT_HISTORY.count(event_full) == 1
        assert event_funcs.EVENT_HISTORY.count(UnitTestInfo(msg="Test Event 1", code="T006")) == 0


def MockNode():
    return ParsedModelNode(
        alias="model_one",
        name="model_one",
        database="dbt",
        schema="analytics",
        resource_type=NodeType.Model,
        unique_id="model.root.model_one",
        fqn=["root", "model_one"],
        package_name="root",
        original_file_path="model_one.sql",
        root_path="/usr/src/app",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "view",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
            }
        ),
        tags=[],
        path="model_one.sql",
        raw_code="",
        description="",
        columns={},
        checksum=FileHash.from_contents(""),
    )


sample_values = [
    MainReportVersion(version=""),
    MainKeyboardInterrupt(),
    MainEncounteredError(exc=""),
    MainStackTrace(stack_trace=""),
    MainTrackingUserState(user_state=""),
    ParsingStart(),
    ParsingCompiling(),
    ParsingWritingManifest(),
    ParsingDone(),
    ManifestDependenciesLoaded(),
    ManifestLoaderCreated(),
    ManifestLoaded(),
    ManifestChecked(),
    ManifestFlatGraphBuilt(),
    ReportPerformancePath(path=""),
    GitSparseCheckoutSubdirectory(subdir=""),
    GitProgressCheckoutRevision(revision=""),
    GitProgressUpdatingExistingDependency(dir=""),
    GitProgressPullingNewDependency(dir=""),
    GitNothingToDo(sha=""),
    GitProgressUpdatedCheckoutRange(start_sha="", end_sha=""),
    GitProgressCheckedOutAt(end_sha=""),
    SystemErrorRetrievingModTime(path=""),
    SystemCouldNotWrite(path="", reason="", exc=""),
    SystemExecutingCmd(cmd=[""]),
    SystemStdOutMsg(bmsg=b""),
    SystemStdErrMsg(bmsg=b""),
    SelectorReportInvalidSelector(valid_selectors="", spec_method="", raw_spec=""),
    MacroEventInfo(msg=""),
    MacroEventDebug(msg=""),
    NewConnection(conn_type="", conn_name=""),
    ConnectionReused(conn_name=""),
    ConnectionLeftOpen(conn_name=""),
    ConnectionClosed(conn_name=""),
    RollbackFailed(conn_name=""),
    ConnectionClosed2(conn_name=""),
    ConnectionLeftOpen2(conn_name=""),
    Rollback(conn_name=""),
    CacheMiss(conn_name="", database="", schema=""),
    ListRelations(database="", schema="", relations=[]),
    ConnectionUsed(conn_type="", conn_name=""),
    SQLQuery(conn_name="", sql=""),
    SQLQueryStatus(status="", elapsed=0.1),
    CodeExecution(conn_name="", code_content=""),
    CodeExecutionStatus(status="", elapsed=0.1),
    SQLCommit(conn_name=""),
    ColTypeChange(
        orig_type="",
        new_type="",
        table=_ReferenceKey(database="", schema="", identifier=""),
    ),
    SchemaCreation(relation=_ReferenceKey(database="", schema="", identifier="")),
    SchemaDrop(relation=_ReferenceKey(database="", schema="", identifier="")),
    UncachedRelation(
        dep_key=_ReferenceKey(database="", schema="", identifier=""),
        ref_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    AddLink(
        dep_key=_ReferenceKey(database="", schema="", identifier=""),
        ref_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    AddRelation(relation=_ReferenceKey(database="", schema="", identifier="")),
    DropMissingRelation(relation=_ReferenceKey(database="", schema="", identifier="")),
    DropCascade(
        dropped=_ReferenceKey(database="", schema="", identifier=""),
        consequences={_ReferenceKey(database="", schema="", identifier="")},
    ),
    UpdateReference(
        old_key=_ReferenceKey(database="", schema="", identifier=""),
        new_key=_ReferenceKey(database="", schema="", identifier=""),
        cached_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    TemporaryRelation(key=_ReferenceKey(database="", schema="", identifier="")),
    RenameSchema(
        old_key=_ReferenceKey(database="", schema="", identifier=""),
        new_key=_ReferenceKey(database="", schema="", identifier=""),
    ),
    DumpBeforeAddGraph(dump=dict()),
    DumpAfterAddGraph(dump=dict()),
    DumpBeforeRenameSchema(dump=dict()),
    DumpAfterRenameSchema(dump=dict()),
    AdapterImportError(exc=ModuleNotFoundError()),
    PluginLoadError(),
    SystemReportReturnCode(returncode=0),
    NewConnectionOpening(connection_state=""),
    TimingInfoCollected(),
    MergedFromState(nbr_merged=0, sample=[]),
    MissingProfileTarget(profile_name="", target_name=""),
    InvalidVarsYAML(),
    GenericTestFileParse(path=""),
    MacroFileParse(path=""),
    PartialParsingFullReparseBecauseOfError(),
    PartialParsingFile(file_dict={}),
    PartialParsingExceptionFile(file=""),
    PartialParsingException(exc_info={}),
    PartialParsingSkipParsing(),
    PartialParsingMacroChangeStartFullParse(),
    ManifestWrongMetadataVersion(version=""),
    PartialParsingVersionMismatch(saved_version="", current_version=""),
    PartialParsingFailedBecauseConfigChange(),
    PartialParsingFailedBecauseProfileChange(),
    PartialParsingFailedBecauseNewProjectDependency(),
    PartialParsingFailedBecauseHashChanged(),
    PartialParsingDeletedMetric(id=""),
    ParsedFileLoadFailed(path="", exc=""),
    PartialParseSaveFileNotFound(),
    StaticParserCausedJinjaRendering(path=""),
    UsingExperimentalParser(path=""),
    SampleFullJinjaRendering(path=""),
    StaticParserFallbackJinjaRendering(path=""),
    StaticParsingMacroOverrideDetected(path=""),
    StaticParserSuccess(path=""),
    StaticParserFailure(path=""),
    ExperimentalParserSuccess(path=""),
    ExperimentalParserFailure(path=""),
    PartialParsingEnabled(deleted=0, added=0, changed=0),
    PartialParsingAddedFile(file_id=""),
    PartialParsingDeletedFile(file_id=""),
    PartialParsingUpdatedFile(file_id=""),
    PartialParsingNodeMissingInSourceFile(source_file=""),
    PartialParsingMissingNodes(file_id=""),
    PartialParsingChildMapMissingUniqueID(unique_id=""),
    PartialParsingUpdateSchemaFile(file_id=""),
    PartialParsingDeletedSource(unique_id=""),
    PartialParsingDeletedExposure(unique_id=""),
    InvalidDisabledSourceInTestNode(msg=""),
    InvalidRefInTestNode(msg=""),
    RunningOperationCaughtError(exc=""),
    RunningOperationUncaughtError(exc=Exception("")),
    DbtProjectError(),
    DbtProjectErrorException(exc=Exception("")),
    DbtProfileError(),
    DbtProfileErrorException(exc=Exception("")),
    ProfileListTitle(),
    ListSingleProfile(profile=""),
    NoDefinedProfiles(),
    ProfileHelpMessage(),
    CatchableExceptionOnRun(exc=""),
    InternalExceptionOnRun(build_path="", exc=Exception("")),
    GenericExceptionOnRun(build_path="", unique_id="", exc=""),
    NodeConnectionReleaseError(node_name="", exc=""),
    CheckCleanPath(path=""),
    ConfirmCleanPath(path=""),
    ProtectedCleanPath(path=""),
    FinishedCleanPaths(),
    OpenCommand(open_cmd="", profiles_dir=""),
    DepsNoPackagesFound(),
    DepsStartPackageInstall(package_name=""),
    DepsInstallInfo(version_name=""),
    DepsUpdateAvailable(version_latest=""),
    DepsListSubdirectory(subdirectory=""),
    DepsNotifyUpdatesAvailable(packages=[]),
    DatabaseErrorRunning(hook_type=""),
    EmptyLine(),
    HooksRunning(num_hooks=0, hook_type=""),
    HookFinished(stat_line="", execution="", execution_time=0),
    WriteCatalogFailure(num_exceptions=0),
    CatalogWritten(path=""),
    CannotGenerateDocs(),
    BuildingCatalog(),
    CompileComplete(),
    FreshnessCheckComplete(),
    ServingDocsPort(address="", port=0),
    ServingDocsAccessInfo(port=""),
    ServingDocsExitInfo(),
    SeedHeader(header=""),
    SeedHeaderSeparator(len_header=0),
    RunResultWarning(resource_type="", node_name="", path=""),
    RunResultFailure(resource_type="", node_name="", path=""),
    StatsLine(stats={"pass": 0, "warn": 0, "error": 0, "skip": 0, "total": 0}),
    RunResultError(msg=""),
    RunResultErrorNoMessage(status=""),
    SQLCompiledPath(path=""),
    CheckNodeTestFailure(relation_name=""),
    FirstRunResultError(msg=""),
    AfterFirstRunResultError(msg=""),
    EndOfRunSummary(num_errors=0, num_warnings=0, keyboard_interrupt=False),
    PrintStartLine(description="", index=0, total=0, node_info=NodeInfo()),
    PrintHookStartLine(statement="", index=0, total=0, node_info=NodeInfo()),
    PrintHookEndLine(
        statement="", status="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    SkippingDetails(
        resource_type="", schema="", node_name="", index=0, total=0, node_info=NodeInfo()
    ),
    PrintErrorTestResult(name="", index=0, num_models=0, execution_time=0, node_info=NodeInfo()),
    PrintPassTestResult(name="", index=0, num_models=0, execution_time=0, node_info=NodeInfo()),
    PrintWarnTestResult(
        name="", index=0, num_models=0, execution_time=0, num_failures=0, node_info=NodeInfo()
    ),
    PrintFailureTestResult(
        name="", index=0, num_models=0, execution_time=0, num_failures=0, node_info=NodeInfo()
    ),
    PrintSkipBecauseError(schema="", relation="", index=0, total=0),
    PrintModelErrorResultLine(
        description="", status="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintModelResultLine(
        description="", status="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintSnapshotErrorResultLine(
        status="", description="", cfg={}, index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintSnapshotResultLine(
        status="", description="", cfg={}, index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintSeedErrorResultLine(
        status="", index=0, total=0, execution_time=0, schema="", relation="", node_info=NodeInfo()
    ),
    PrintSeedResultLine(
        status="", index=0, total=0, execution_time=0, schema="", relation="", node_info=NodeInfo()
    ),
    PrintHookEndErrorLine(
        source_name="", table_name="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintHookEndErrorStaleLine(
        source_name="", table_name="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintHookEndWarnLine(
        source_name="", table_name="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintHookEndPassLine(
        source_name="", table_name="", index=0, total=0, execution_time=0, node_info=NodeInfo()
    ),
    PrintCancelLine(conn_name=""),
    DefaultSelector(name=""),
    NodeStart(unique_id="", node_info=NodeInfo()),
    NodeCompiling(unique_id="", node_info=NodeInfo()),
    NodeExecuting(unique_id="", node_info=NodeInfo()),
    NodeFinished(unique_id="", node_info=NodeInfo(), run_result=RunResultMsg()),
    QueryCancelationUnsupported(type=""),
    ConcurrencyLine(num_threads=0, target_name=""),
    StarterProjectPath(dir=""),
    ConfigFolderDirectory(dir=""),
    NoSampleProfileFound(adapter=""),
    ProfileWrittenWithSample(name="", path=""),
    ProfileWrittenWithTargetTemplateYAML(name="", path=""),
    ProfileWrittenWithProjectTemplateYAML(name="", path=""),
    SettingUpProfile(),
    InvalidProfileTemplateYAML(),
    ProjectNameAlreadyExists(name=""),
    GetAddendum(msg=""),
    DepsSetDownloadDirectory(path=""),
    EnsureGitInstalled(),
    DepsCreatingLocalSymlink(),
    DepsSymlinkNotAvailable(),
    FoundStats(stat_line=""),
    CompilingNode(unique_id=""),
    WritingInjectedSQLForNode(unique_id=""),
    DisableTracking(),
    SendingEvent(kwargs=""),
    SendEventFailure(),
    FlushEvents(),
    FlushEventsFailure(),
    TrackingInitializeFailure(),
    RetryExternalCall(attempt=0, max=0),
    GeneralWarningMsg(msg="", log_fmt=""),
    GeneralWarningException(exc=Exception(""), log_fmt=""),
    PartialParsingProfileEnvVarsChanged(),
    AdapterEventDebug(name="", base_msg="", args=()),
    AdapterEventInfo(name="", base_msg="", args=()),
    AdapterEventWarning(name="", base_msg="", args=()),
    AdapterEventError(name="", base_msg="", args=()),
    PrintDebugStackTrace(),
    MainReportArgs(args={}),
    RegistryProgressMakingGETRequest(url=""),
    RegistryIndexProgressMakingGETRequest(url=""),
    RegistryIndexProgressGETResponse(url="", resp_code=1),
    RegistryResponseUnexpectedType(response=""),
    RegistryResponseMissingTopKeys(response=""),
    RegistryResponseMissingNestedKeys(response=""),
    RegistryResponseExtraNestedKeys(response=""),
    DepsUTD(),
    PartialParsingNotEnabled(),
    SQLRunnerException(exc=""),
    DropRelation(dropped=_ReferenceKey(database="", schema="", identifier="")),
    PartialParsingProjectEnvVarsChanged(),
    RegistryProgressGETResponse(url="", resp_code=1),
    IntegrationTestDebug(msg=""),
    IntegrationTestInfo(msg=""),
    IntegrationTestWarn(msg=""),
    IntegrationTestError(msg=""),
    IntegrationTestException(msg=""),
    EventBufferFull(),
    RecordRetryException(exc=Exception("")),
    UnitTestInfo(msg=""),
]


class TestEventJSONSerialization:

    # attempts to test that every event is serializable to json.
    # event types that take `Any` are not possible to test in this way since some will serialize
    # just fine and others won't.
    def test_all_serializable(self):
        no_test = [DummyCacheEvent]

        all_non_abstract_events = set(
            filter(
                lambda x: not inspect.isabstract(x) and x not in no_test, get_all_subclasses(Event)
            )
        )
        all_event_values_list = list(map(lambda x: x.__class__, sample_values))
        diff = all_non_abstract_events.difference(set(all_event_values_list))
        assert (
            not diff
        ), f"test is missing concrete values in `sample_values`. Please add the values for the aforementioned event classes"

        # make sure everything in the list is a value not a type
        for event in sample_values:
            assert type(event) != type

        # if we have everything we need to test, try to serialize everything
        for event in sample_values:
            event_dict = event.to_dict()
            try:
                event_json = event.to_json()
            except TypeError as e:
                raise Exception(f"{event} is not serializable to json. Originating exception: {e}")


T = TypeVar("T")


@dataclass
class Counter(Generic[T], SerializableType):
    dummy_val: T
    count: int = 0

    def next(self) -> T:
        self.count = self.count + 1
        return self.dummy_val

    # mashumaro serializer
    def _serialize() -> Dict[str, int]:
        return {"count": count}


@dataclass
class DummyCacheEvent(InfoLevel, Cache, SerializableType):
    code = "X999"
    counter: Counter

    def message(self) -> str:
        return f"state: {self.counter.next()}"

    # mashumaro serializer
    def _serialize() -> str:
        return "DummyCacheEvent"
