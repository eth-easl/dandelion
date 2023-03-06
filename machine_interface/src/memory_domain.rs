
// always need to contain a option to a MemoryDomainTuple
#[derive(Debug)]
pub enum MemoryDomain {
    Blackhole,
    // CheriDomain(Option<MemoryDomainTuple<cheriController>>),
}

// add in specialized cases for transfer options
pub fn transefer_memory(destination : MemoryDomain, source : MemoryDomain, 
    source_offset : isize, destination_offset : isize,
    size : isize, sanitize : bool, callback : impl FnOnce(MemoryDomain, MemoryDomain)->()){
    match (destination, source) {
        // (MemoryDomain::CheriDomain, MemoryDomain::CheriDomain) => println!("Not yet implemented cheri mem transfer"),
        (MemoryDomain::Blackhole, MemoryDomain::Blackhole) => println!("From the hole it comes to the hole it goes"),
        (dst, src) => println!("Tried to transfer from {:?} at {} to {:?} at {}, for {} bytes, sanitation {}",
            src, source_offset, dst, destination_offset, size, sanitize),
    }
}