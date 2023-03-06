
// separate file for memory domain enum definition and transfer functions
mod memory_domain;

// define all interfaces to hardware dependent part of dandelion
mod hardware_interface {
    use std::vec::Vec;
    use crate::memory_domain::MemoryDomain;
    // TODO define error types
    pub enum ControllerError {
        Default,
    }

    pub type Result<T> = std::result::Result<T, ControllerError>;

    pub trait MemoryDomainController {
        type ControllerMemoryDomain;
        // initialization and shutdown
        fn start_domain_controller(domain_config : Vec<u8>) -> Result<Box<Self>>;
        fn stop_domain_controller(self) -> Result<()>;
        // allocation and distruction
        fn alloc_memory_subdomain(size : usize) -> Result<MemoryDomain>;
        fn free_memory_subdomain(domain : Self::ControllerMemoryDomain) -> Result<()>;
    }

    struct MemoryDomainTuple<&'a T: MemoryDomainController> {
        controller : &'a T,
        domain : &'a T::ControllerMemoryDomain,
    }

    enum RequirementType {
        StaticData,
        Input,
    }
    
    enum OffsetOrAlignment {
        Offset(isize),
        Allignment(isize),
    }

    enum SizeRequirement {
        Range(isize, isize),
        ModResidual(isize, isize)
    }

    struct DataRequirement {
        id : u32,
        req_type : RequirementType,
        position : Option<OffsetOrAlignment>,
        size : Option<SizeRequirement>,
    }

    struct DataRequirementList {
        domain_type : MemoryDomain,
        requirements : Vec<DataRequirement>,
    }

    struct Position {
        offset : isize,
        size : isize,
    }

    enum DataItemType {
        Item(Position),
        Set(Vec<Position>),
    }

    struct DataItem {
        index : u32,
        item_type : DataItemType,
    }

    // todo find better name
    pub trait ComputeController {
        // required parts of the trait
        type functionConfig;
        // initialization and shutdown
        fn start_compute_controller(config : Vec<u8>) -> Result<Box<Self>>;
        fn stop_compute_controller(self) -> Result<()>;
        // compute unit setup
        fn take_computer(id : u32) -> Result<()>;
        fn yield_computer(id : u32) -> Result<()>;
        // code setup
        fn setUpFunctionCode(code : Vec<u8>) -> Result<functionConfig>;
        fn tearDownFunctionCode(config : functionConfig) -> Result<()>;
        TODO callback also needs to be able to communicate error codes.
        fn run(id : u32, code : functionConfig, domain : memory_domain, inputPositions : Vec<DataItem>,
            callback : impl FnOnce(memory_domain, Vec<DataItem>)->()) -> Result<()>;
        fn stop(id : u32, callback : impl FnOnce(memory_domain)->()) -> Result<()>;
    }

// Todo force all controllers to implement deallocation on dropping
// impl Drop for ExecutionUnit {
//     fn drop(& mut self){
//         self.removeEU();
//     }
// }

}

// TODO add compiler switches
// mod cheri;