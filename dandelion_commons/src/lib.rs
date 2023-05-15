// TODO define error types, possibly better printing than debug
#[derive(Debug, Clone, PartialEq)]
pub enum DandelionError {
    NotImplemented, // trying to use a feature that is not yet implemented
    // errors in configurations
    MalformedConfig, // configuration vector was malformed
    UnknownSymbol,   // parser did not find symbol that it was searching for
    // domain and context errors
    ContextMissmatch, // context handed to context specific function was wrong type
    OutOfMemory,      // domain could not be allocated because there is no space available
    ContextFull,      // context can't fit additional memory
    InvalidRead,      // tried to read from domain outside of domain bounds
    InvalidWrite,     // tried to write to domain ouside of domain bounds
    EmptyDataItemSet, // found a case with a data item that is a set but has no entries
    // engine errors
    ConfigMissmatch, // missmatch between the function config the engine expects and the one given
    NoRunningFunction, // attempted abort when no function was running
    EngineAlreadyRunning, // attempted to run on already busy engine
    EngineError,     // there was a non recoverable issue with the engine
    NoEngineAvailable, // asked driver for engine, but there are no more available
    // dispatcher errors
    DispatcherConfigError, // error from resulting from assumptions based on config passed to dispatcher
    DispatcherUnavailableFunction, // dispatcher was asked to queue function it can't find
    // Gerneral util errors
    FileError, // error while performing IO on a file
    // protection errors
    UnauthorizedSyscall,
    SegmentationFault,
    OtherProctionError,
}

pub type DandelionResult<T> = std::result::Result<T, DandelionError>;
