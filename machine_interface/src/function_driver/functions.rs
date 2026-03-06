use crate::{
    function_driver::load_utils::load_static,
    machine_config::EngineType,
    memory_domain::{Context, MemoryDomain},
    DataRequirementList,
};
use dandelion_commons::{records::Recorder, DandelionResult};
use log::warn;
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, RwLock,
    },
};

#[derive(Clone)]
#[allow(dead_code)]
pub struct ElfConfig {
    // TODO change to positions
    pub(super) system_data_offset: usize,
    #[cfg(feature = "cheri")]
    pub(super) return_offset: (usize, usize),
    pub(super) entry_point: usize,
    #[cfg(feature = "mmu")]
    pub(super) protection_flags: Arc<Vec<(u32, crate::Position)>>,
}

#[derive(Clone, Copy)]
pub enum SystemFunction {
    HTTP,
    MEMCACHED,
}

impl core::fmt::Display for SystemFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        return match self {
            SystemFunction::HTTP => write!(f, "HTTP"),
            SystemFunction::MEMCACHED => write!(f, "MEMCACHED"),
        };
    }
}

#[derive(Clone)]
pub enum FunctionConfig {
    ElfConfig(ElfConfig),
    SysConfig(SystemFunction),
}

pub struct Function {
    pub requirements: DataRequirementList,
    pub context: Arc<Context>,
    pub config: FunctionConfig,
}

impl Function {
    pub fn load(
        &self,
        domain: &Box<dyn MemoryDomain>,
        ctx_size: usize,
    ) -> DandelionResult<Context> {
        return match &self.config {
            FunctionConfig::ElfConfig(_) => {
                load_static(domain, self.context.clone(), &self.requirements, ctx_size)
            }
            FunctionConfig::SysConfig(_) => domain.acquire_context(ctx_size),
        };
    }
}

/// Struct holding all information about an alternative engine to execute the function.
pub struct FunctionAlternative {
    /// The engine type of the alternative.
    pub engine: EngineType,
    /// The default context size of this alternative.
    pub context_size: usize,
    /// Path to the function binary.
    pub path: String,
    /// Domain to use for loading this function
    pub domain: Arc<Box<dyn MemoryDomain>>,
    /// state of the function cell
    /// 0: empty, 1: load in progress, 2: ready
    /// This is to make it easy to read the state from the queue later on
    // function_state: AtomicU8,
    /// Function object once the binary is loaded in memory.
    pub function: RwLock<Option<Arc<Function>>>,
}

unsafe impl Sync for FunctionAlternative {}

impl FunctionAlternative {
    pub fn new_loaded(
        engine: EngineType,
        context_size: usize,
        path: String,
        domain: Arc<Box<dyn MemoryDomain>>,
        function: Arc<Function>,
    ) -> Self {
        FunctionAlternative {
            engine,
            context_size,
            path,
            domain,
            // function_state: AtomicU8::new(2),
            function: RwLock::new(Some(function)),
        }
    }

    pub fn new_unloaded(
        engine: EngineType,
        context_size: usize,
        path: String,
        domain: Arc<Box<dyn MemoryDomain>>,
    ) -> Self {
        FunctionAlternative {
            engine,
            context_size,
            path,
            domain,
            // function_state: AtomicU8::new(0),
            function: RwLock::new(None),
        }
    }

    /// Load the given function info of given engine type.
    /// Assumes the caller has succesfully set the state to loading
    pub fn load_function(
        &self,
        caching: bool,
        recorder: &mut Recorder,
    ) -> DandelionResult<Arc<Function>> {
        // load the function
        // let mut write_lock = None;
        if caching {
            let read_guard = self.function.read().unwrap();
            if let Some(inner) = read_guard.as_ref() {
                return Ok(inner.clone());
                // } else {
                // drop(read_guard);
                // write_lock = Some(self.function.write().unwrap());
            }
            // let state = self.function_state.load(Ordering::Acquire);
            // debug_assert_ne!(0, state, "State should never be 0 if loading with caching");
            // if state == 2 {
            //     return Ok(self.function.read().unwrap().as_ref().unwrap().clone());
            // }
        }
        let driver = self.engine.get_driver();
        recorder.record(dandelion_commons::records::RecordPoint::ParsingStart);
        let function = Arc::new(driver.parse_function(self.path.clone(), &self.domain)?);
        recorder.record(dandelion_commons::records::RecordPoint::ParsingEnd);
        if caching {
            let mut function_lock = self.function.write().unwrap();
            *function_lock = Some(function.clone());
            // self.function_state.store(2, Ordering::Release);
            // self.function_state
            //     .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
            //     .expect("Finish loading should always find load reservation");
        }
        Ok(function)
    }
}

impl Debug for FunctionAlternative {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Function alternative")
            .field("engine", &self.engine)
            .field("context_size", &self.context_size)
            .field("path", &self.path)
            // .field(
            //     "function",
            //     match self.function_state.load(Ordering::Acquire) {
            //         0 => &"None",
            //         1 => &"Loading",
            //         2 => &"Ready",
            //         x => panic!("Should never have function state {:x}", x),
            //     },
            // )
            .finish()
    }
}
