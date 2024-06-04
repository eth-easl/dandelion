pub mod records;

use records::RecordPoint;

pub type EngineTypeId = u8;
pub type ContextTypeId = u8;
pub type FunctionId = u64;

// TODO define error types, possibly better printing than debug
#[derive(Debug, Clone, PartialEq)]
pub enum DandelionError {
    /// trying to use a feature that is not yet implemented
    NotImplemented,
    // errors in configurations
    /// configuration vector was malformed
    MalformedConfig,
    /// parser did not find symbol that it was searching for
    UnknownSymbol,
    // domain and context errors
    /// error creating layout for read only context
    ContextReadOnlyLayout,
    /// context handed to context specific function was wrong type
    ContextMissmatch,
    /// domain could not be allocated because there is no space available
    OutOfMemory,
    /// memory size is not allowed, e.g. Wasm memory must be <= 4GiB
    InvalidMemorySize,
    /// the value specified for the context size does not match the WASM memory size
    WasmContextMemoryMismatch,
    /// error when trying to allocate memory
    MemoryAllocationError,
    /// context can't fit additional memory
    ContextFull,
    /// read buffer was misaligned for requested data type
    ReadMisaligned,
    /// tried to read from domain outside of domain bounds
    InvalidRead,
    /// offset handed to writing was not aligned with type to write
    WriteMisaligned,
    /// tried to write to domain ouside of domain bounds
    InvalidWrite,
    /// found a case with a data item that is a set but has no entries
    EmptyDataSet,
    /// tried to transfer a set index that is not in the content of the context
    TransferInputNoSetAvailable,
    /// tried to transfer to a data item that was already present
    TransferItemAlreadyPresent,
    /// error converting pointers or integers
    UsizeTypeConversionError,
    /// context synchronization failed
    ContextSyncError,
    // engine errors
    /// missmatch between the function config the engine expects and the one given
    ConfigMissmatch,
    /// missmatch between the resource an engine was given and what it expects to run on or
    /// the resource doesn't exist
    EngineResourceError,
    /// attempted abort when no function was running
    NoRunningFunction,
    /// attempted to run on already busy engine
    EngineAlreadyRunning,
    /// there was a non recoverable issue with the engine
    EngineError,
    /// asked driver for engine, but there are no more available
    NoEngineAvailable,
    /// there was a non recoverable issue when spawning or running the MMU worker
    MmuWorkerError,
    // system engine errors
    /// The arguments in the context handed to the system function are malformed or otherwise insufissient
    /// the string identifies the argument that was malformed or gives other information about the issue
    MalformedSystemFuncArg(String),
    /// Argument given to system function was not valid
    InvalidSystemFuncArg(String),
    /// System function did get unexpected response
    SystemFuncResponseError,
    /// Tried to call parser for system function
    CalledSystemFuncParser,
    // dispatcher errors
    /// dispatcher does not find a loader for this engine type
    DispatcherMissingLoader(EngineTypeId),
    /// error from resulting from assumptions based on config passed to dispatcher
    DispatcherConfigError,
    /// dispatcher was asked to queue function it can't find
    DispatcherUnavailableFunction,
    /// function to register did not have metadata available
    DispatcherMetaDataUnavailable,
    /// dispatcher encountered an issue when trasmitting data between tasks
    DispatcherChannelError,
    /// dispatcher found set to transfer that has no registered name
    DispatcherSetMissmatch,
    /// dispatcher failed to combine two composition sets
    DispatcherCompositionCombine,
    /// dispatcher found mistake when trying to find waiting functions
    DispatcherDependencyError,
    // metering errors
    /// Mutex for metering was poisoned
    RecordLockFailure,
    /// Call to record time spans were not called in order
    RecordSequencingFailure(RecordPoint, RecordPoint),
    // Gerneral util errors
    /// error while performing IO on a file
    FileError,
    // protection errors
    /// the function issued a system call outside the authorized list
    UnauthorizedSyscall,
    /// the function triggered a memory protection fault
    SegmentationFault,
    /// other protection errors caused by the function
    OtherProctionError,
    // errors from the functions
    /// Function indicated it failed
    FunctionError(i32),
}

pub type DandelionResult<T> = std::result::Result<T, DandelionError>;
