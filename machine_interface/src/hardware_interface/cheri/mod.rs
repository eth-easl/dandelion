// use crate::hardwareInterface::*;

// use std::sync::mpsc::{Sender};
// use std::sync::mpsc::{Sender, Receiver};
// use std::sync::mpsc;

// use dandelion_function_interface::ioStruct;

use 

pub struct CheriMemoryDomain {
    memorySpace : std::vec::Vec<u8>
}

pub struct CheriMemoryDomainController {

}

impl MemoryDomainController for CheriMemoryDomainController {
    type ControllerMemoryDomain = CheriMemoryDomain;
    fn start_domain_controller(domain_config : Vec<u8>) -> Result<Box<Self>> {

    }
    fn stop_domain_controller(self) -> Result<()> {

    }
    // allocation and distruction
    fn alloc_memory_subdomain(size : usize) -> Result<MemoryDomain> {

    }
    fn free_memory_subdomain(domain : Self::ControllerMemoryDomain) -> Result<()> {
        return Ok();
    }
}

// pub struct CheriThread {
//     coreId : u32,
//     function : Option<CheriFunction>,
//     channel : Sender<()>
// }

// impl ExecutionUnit for CheriThread {
//     type Function = CheriFunction;
//     fn addEU(id : u32) -> Self{
//         // TODO start thread
//         let (sender, _) = std::sync::mpsc::channel();
//         CheriThread{
//             coreId : id,
//             function : None,
//             channel : sender 
//         }
//     }
//     fn removeEU(&mut self){
//         () // TODO kill thread
//     }
//     fn setup(&mut self, function : Self::Function){
//         self.function = Some(function);
//     }
//     fn tearDown(&mut self){
//         self.function = None;
//     }
//     fn run(&mut self, input : Vec<ioStruct>, callback : impl FnMut(Vec<ioStruct>)->()){
//         callback(input);
//     }
//     fn stop(&mut self) {
//         () // TODO kill thread and make new one.
//     }
// }

