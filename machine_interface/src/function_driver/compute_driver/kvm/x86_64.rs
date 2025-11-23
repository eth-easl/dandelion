use dandelion_commons::{DandelionError, DandelionResult, UserError};
use kvm_bindings::{kvm_fpu, kvm_regs, kvm_segment, kvm_sregs};
use kvm_ioctls::{VcpuFd, VmFd};
use log::trace;
use std::{arch::global_asm, os::raw::c_void, slice};

// CR0 bits
/// Protected Mode Enable
const CR0_PE: u64 = 1 << 0;
/// Monitor Co-Processor
const CR0_MP: u64 = 1 << 1;
/// Extension Type
const CR0_ET: u64 = 1 << 4;
/// Numeric Error
const CR0_NE: u64 = 1 << 5;
/// Write Protect
const CR0_WP: u64 = 1 << 16;
/// Alignment Mask
const CR0_AM: u64 = 1 << 18;
/// Paging
const CR0_PG: u64 = 1 << 31;

// CR4 bits
/// Physical Address Extension
const CR4_PAE: u64 = 1 << 5;
/// OS support for fxsave and fxrstor instructions
const CR4_OSFXSR: u64 = 1 << 9;
/// OS Support for unmasked simd floating point exceptions
const CR4_OSXMMEXCPT: u64 = 1 << 10;
/// Enables the instructions RDFSBASE, RDGSBASE, WRFSBASE, and WRGSBASE
const CR4_FSGSBASE: u64 = 1 << 16;

// EFER bits
/// Long Mode Enable
const EFER_LME: u64 = 1 << 8;
/// Long Mode Active
const EFER_LMA: u64 = 1 << 10;

// 64-bit page directory entry bits
/// Present
const PDE64_PRESENT: u64 = 1 << 0;
/// Writable
const PDE64_RW: u64 = 1 << 1;
/// User accessible
const PDE64_USER: u64 = 1 << 2;
/// Default flags for user accessable pages
const PDE64_ALL_ALLOWED: u64 = PDE64_PRESENT | PDE64_RW | PDE64_USER;
/// Set by hardware if the entry has been used to translate a linear address
const PDE64_ACCESSED: u64 = 1 << 5;
/// Page size, for p3 and lower, this indicates the entry address is the mapping, not another table
/// In the docs this is called the PDE64_PS
const PDE64_IS_PAGE: u64 = 1 << 7;

// 4 level paging:
// each linear address consists of the following
// 47 .. 39  | 38 .. 30        | 29 .. 21  | 20 .. 12  | 11 .. 0
// PML4      | Directory PTR   | Directory | Table     | Offset
// with the PML4 being the offset into the page table pointed to by the root pointer in CR3
// and then the others being indexes into the tables recursively
// The direcotry pointer table entries are either 1GB pages or directory table pointers
// Directory table entries are either 2MB pages or tables
// Table entries are always 4KB pages
const PAGE_SHIFT: usize = 12;
pub const PAGE_SIZE: usize = 1 << PAGE_SHIFT;
const LARGE_PAGE_SHIFT: usize = 21;
pub const LARGE_PAGE: usize = 1 << LARGE_PAGE_SHIFT;
const HUGE_PAGE_SHIFT: usize = 30;
const HUGE_PAGE: usize = 1 << HUGE_PAGE_SHIFT;
const PML4_SHIFT: usize = 39;
const TABLE_SIZE: usize = 512;

fn u8_slice_to_u64_slice(input: &mut [u8]) -> &mut [u64] {
    assert!(
        input.len() % 8 == 0,
        "Input slice length must be a multiple of 8, but has length: {}",
        input.len()
    );
    assert!(
        input.as_ptr() as usize % 8 == 0,
        "Input slice must be 8-byte aligned, but got {:?}",
        input.as_ptr()
    );
    let u64_len = input.len() / 8;
    unsafe { std::slice::from_raw_parts_mut(input.as_mut_ptr() as *mut u64, u64_len) }
}

pub struct ResetState {
    sregs: kvm_sregs,
}

global_asm!(include_str!("x86_64.asm"));
extern "C" {
    // symbols for first and last
    fn asm_start();
    fn fault_handlers_end();
    // interrupt vector handlers
    fn default_handler();
    fn divide_error_exception_handler();
    fn debug_interrupt_handler();
    fn nmi_interrupt_handler();
    fn breakpoint_exception_handler();
    fn overflow_exception_handler();
    fn bound_range_excepion_handler();
    fn invalid_opcode_exception_handler();
    fn device_not_available_exception_handler();
    fn double_fault_exception_handler();
    fn coprocessor_segment_overrun_handler();
    fn invalid_tss_exception_handler();
    fn segment_not_present_handler();
    fn stack_fault_exception_handler();
    fn general_protection_exception_handler();
    fn page_fault_exception_handler();
    fn floating_point_error_handler();
    fn alignment_check_exception_handler();
    fn machine_check_exception_handler();
    fn simd_fp_exception_handler();
    fn virtualization_exception_handler();
    fn control_protection_exception();
    fn user_exit_handler();
}

impl ResetState {
    pub fn new(_vm: &VmFd, vcpu: &VcpuFd) -> Self {
        let mut sregs = vcpu.get_sregs().unwrap();
        setup_long_mode(&mut sregs);
        return Self { sregs };
    }

    /// initialized_pages: Vec with all pages that do not need to be zeroed on first access
    /// The two usize parameters are start and end (start + size, so 1 past end) addresses
    /// of the present pages, if the Option is None, it is already at the correct place,
    /// if the option is Some(address) that address points to the start address of a copy
    /// on write space that should be mapped between start and end of the same size
    pub fn init_vcpu(
        &self,
        vcpu: &VcpuFd,
        entry_point: u64,
        guest_mem: &mut [u8],
        initialized_pages: Vec<(usize, usize, Option<usize>)>,
        mut stack_pointer: usize,
        last_address: usize,
    ) -> PageFaultMetadata {
        let mut sregs = self.sregs.clone();
        let interrupt_end = stack_pointer;
        set_interrupt_table(&mut sregs, guest_mem, &mut stack_pointer);
        let interrupt_start = stack_pointer;
        let page_fault_metadata;
        (stack_pointer, page_fault_metadata) = set_page_table(
            &mut sregs,
            guest_mem,
            initialized_pages,
            stack_pointer,
            (interrupt_start, interrupt_end),
            last_address,
        );

        vcpu.set_sregs(&sregs).unwrap();
        vcpu.set_regs(&kvm_regs {
            rip: entry_point,
            rsp: stack_pointer as u64 - 32,
            rbp: stack_pointer as u64 - 32,
            rflags: 2,
            ..Default::default()
        })
        .unwrap();
        vcpu.set_fpu(&kvm_fpu::default()).unwrap();
        return page_fault_metadata;
    }
}

/// Function set a 8 byte memory location up to be a memory segment
/// The priviledge level 0 is the highest (root) and 3 the lowest
/// Segment type 10 is execute, 2 is read/write
fn setup_segement(mem_location: &mut [u8], privilege_level: u8, segment_type: u8) {
    // 0 for system, 1 for code or data
    let descriptor_type = 1;
    // 0 for code in segment is 32 bit, 1 for native 64 bit
    let code_64_bit = 1;
    // setup limit to max
    mem_location[0] = 0xFF;
    mem_location[1] = 0xFF;
    // set present, descriptor type and type
    mem_location[5] = (1 << 7) | (privilege_level << 5) | (descriptor_type << 4) | segment_type;
    // set granularity, if it is 64 bit code and upper 4 bits of segment limit
    mem_location[6] = (1 << 7) | ((code_64_bit as u8) << 5) | 0xF;
}

/// Function sets a 16 byte memory location to be a interrupt descriptor table entry
fn setup_interrupt_gate(mem_location: &mut [u8], selector: u16, address: u64) {
    // write the address as offset into the correct places in the memory location
    let address_array = address.to_le_bytes();
    mem_location[0] = address_array[0];
    mem_location[1] = address_array[1];
    mem_location[6..12].copy_from_slice(&address_array[2..8]);
    // set the segment selector
    mem_location[2..4].copy_from_slice(&selector.to_le_bytes());
    // set interrupt stack table entry to use to load the stack when transitioning to the handler
    mem_location[4] = 1;
    // set the gate the present bit, priviledge level and gate type
    let priviledge_level = 3; // 2 bit number from 0 to 3
    let gate_type = 0xF; // in 64 bit mode 0xE is a trap gate
    mem_location[5] = 1 << 7 | (priviledge_level << 5) | gate_type;
}

/// Intel Interrupt Descriptor Table (IDT) in 64-bit mode:
/// - Entries are 16-byte descriptors (in protected mode)
/// - Base address should be alligned to 8-byte boundry (for cache alignment)
/// - First 32 entries are reserved for intel interrupts rest (32-255) are user defined
///     (so we don't need to hanlde them if we don't plan to use them)
/// - Entry 13 is for handling general protection faults (the default fallback fault)
/// - Entry 14 is for handling page faults
/// - Not all entries need to be filled, for empty slots should have the present flag in the descriptor set to 0
/// - Address of the IDT is held in the IDTR which holds both a 32-bit base address as well as a 16-bit limit,
///     the limit should always be one less than an integral multiple of eight (adding it to the base address,
///     should give the address of the last valid byte in the IDT so, it should be 8N -1, with N entries)
fn set_interrupt_table(sregs: &mut kvm_sregs, guest_mem: &mut [u8], stack_start: &mut usize) {
    // constants given by the manuals or how we set it up
    const GDT_SIZE: u16 = 80;
    const TSS_SIZE: u16 = 104;

    // setup memory space
    *stack_start -= PAGE_SIZE;
    let interrupt_handler = *stack_start;
    guest_mem[interrupt_handler..interrupt_handler + PAGE_SIZE].fill(0);
    let handler_length = (fault_handlers_end as *const c_void).addr()
        - (asm_start as *const () as *const c_void).addr();

    let rounded_handler = handler_length.next_multiple_of(8);
    let gdt = if rounded_handler + ((GDT_SIZE + TSS_SIZE) as usize) < PAGE_SIZE {
        interrupt_handler + rounded_handler
    } else {
        *stack_start -= PAGE_SIZE;
        guest_mem[*stack_start..*stack_start + PAGE_SIZE].fill(0);
        *stack_start
    };
    let gdt_end: usize = gdt + (GDT_SIZE as usize);
    let tss_start: usize = gdt_end;
    let tss_end: usize = tss_start + (TSS_SIZE as usize);

    // setup global descriptor table
    // kernel code segment
    setup_segement(&mut guest_mem[gdt + 16..gdt + 32], 0, 10);
    // kernel data segment
    setup_segement(&mut guest_mem[gdt + 32..gdt + 48], 0, 2);
    // user code segment
    setup_segement(&mut guest_mem[gdt + 48..gdt + 64], 3, 10);
    // user data segment
    setup_segement(&mut guest_mem[gdt + 64..gdt + 80], 3, 2);

    sregs.gdt = kvm_bindings::kvm_dtable {
        base: gdt as u64,
        limit: GDT_SIZE - 1,
        ..Default::default()
    };

    // set up task state segement
    sregs.tr = kvm_segment {
        base: tss_start as u64,
        limit: (TSS_SIZE - 1) as u32,
        selector: 10 << 3,
        type_: 11,
        present: 1,
        dpl: 0,
        db: 0,
        s: 0,
        l: 0,
        g: 0,
        avl: 0,
        ..Default::default()
    };

    // set the revevant parts of the TSS, i.e. the stack address
    // since it is a stack, neeeds to be set to top end of page
    // set the interrupt stack address
    let tss = &mut guest_mem[tss_start..tss_end];
    tss[36..44].copy_from_slice(&(*stack_start).to_le_bytes());
    *stack_start -= PAGE_SIZE;
    guest_mem[*stack_start..*stack_start + PAGE_SIZE].fill(0);

    // setup interupt handler table
    let destination = unsafe {
        slice::from_raw_parts_mut(
            guest_mem.as_mut_ptr().add(interrupt_handler),
            handler_length,
        )
    };
    let source = unsafe { slice::from_raw_parts(asm_start as *const u8, handler_length) };
    destination.copy_from_slice(source);

    // set the general protection handler (also fall back for when others are not initialized)
    // two consequitive u64 are the descriptor for one entry
    // need to set the segment selector to 1 for the segment we have set up in the GDT
    // the segmenet selector uses bits 0 and 1 for the requested priviledge level (0 for us)
    // then bit 2 to the table indicator (0 for GDT, 1 for LDT)
    // let idt_general_protection = &mut guest_mem[GDT + 13 * 16..GDT + 14 * 16];

    // use selector 2, as in 64 bit mode, all the entries in the GDT use two entries (because entry size is determined by 32 bit mode)
    let segment_selector: u16 = 2 << 3 | 0;

    let idt_base = tss_end;
    for i in 0..33 {
        let handler_address = (match i {
            0 => divide_error_exception_handler,
            1 => debug_interrupt_handler,
            2 => nmi_interrupt_handler,
            3 => breakpoint_exception_handler,
            4 => overflow_exception_handler,
            5 => bound_range_excepion_handler,
            6 => invalid_opcode_exception_handler,
            7 => device_not_available_exception_handler,
            8 => double_fault_exception_handler,
            9 => coprocessor_segment_overrun_handler,
            10 => invalid_tss_exception_handler,
            11 => segment_not_present_handler,
            12 => stack_fault_exception_handler,
            13 => general_protection_exception_handler,
            14 => page_fault_exception_handler,
            16 => floating_point_error_handler,
            17 => alignment_check_exception_handler,
            18 => machine_check_exception_handler,
            19 => simd_fp_exception_handler,
            20 => virtualization_exception_handler,
            21 => control_protection_exception,
            32 => user_exit_handler,
            _ => default_handler,
        } as *const u8)
            .addr();
        let handler_offset = (interrupt_handler + handler_address
            - (asm_start as *const () as *const c_void).addr()) as u64;

        setup_interrupt_gate(
            &mut guest_mem[idt_base + i * 16..idt_base + (i + 1) * 16],
            segment_selector,
            handler_offset,
        );
    }
    // limit is the number to add to the base to address the last byte in the table
    // 16 bytes per entry, need to cover up to and including entry 14
    sregs.idt = kvm_bindings::kvm_dtable {
        base: idt_base as u64,
        limit: 33 * 16 - 1,
        ..Default::default()
    };
}

pub struct PageFaultMetadata {
    /// Address of the singular p4 table, only first entry set
    p4_address: usize,
    /// Address of the singular p3 table, only first entry set
    p3_address: usize,
    /// Base address for the p2 table array
    p2_base: usize,
    /// Base address for the p1 table array
    p1_base: usize,
    /// The highest address for which page faults should be resolved.
    /// If the fault lies above, the function tried to access memory that was not mapped on purpose
    max_address: usize,
}

fn set_range(
    p2_full_array: &mut [u64],
    p1_full_array: &mut [u64],
    p1_base: usize,
    virtual_start: usize,
    virtual_end: usize,
    protection_flags: u64,
    mut base_physical: usize,
    mut previous_past_last_page: usize,
) -> usize {
    // in check which page tables entries need to be set
    let first_page = virtual_start >> PAGE_SHIFT;
    let mut current_page_entry = first_page;
    let past_last_page = virtual_end >> PAGE_SHIFT;
    // Ensure assumption that there is never overlap between the previous and the current range
    debug_assert!(previous_past_last_page <= first_page);

    // can always set the first and the last p2 entries to point to the p1 tables,
    // if the pages are 2MB alligned it will be overwritten, but that is fine,
    // and if they are the same, the second will also overwrite the first, but that is also fine
    let first_p2_index = virtual_start >> LARGE_PAGE_SHIFT;
    // want to set up the p1 table, if it has not been set up by last one anyway alreay
    // or if we won't set it up in the loop anyway, <= since the previous last page is set to 1 past_last_page
    if previous_past_last_page <= (first_p2_index * TABLE_SIZE)
        && current_page_entry % TABLE_SIZE != 0
    {
        p2_full_array[first_p2_index] = PDE64_ALL_ALLOWED
            | (p1_base + (current_page_entry & !(TABLE_SIZE - 1)) * size_of::<u64>()) as u64;
        p1_full_array[first_p2_index * TABLE_SIZE..current_page_entry].fill(0);
    }
    while current_page_entry < past_last_page {
        // check if we are entering a region under a new p2 entry
        let p1_entry_address = p1_base + current_page_entry * size_of::<u64>();
        if current_page_entry % TABLE_SIZE == 0 {
            if current_page_entry + 512 <= past_last_page {
                p2_full_array[current_page_entry / TABLE_SIZE] =
                    protection_flags | PDE64_IS_PAGE | base_physical as u64;
                base_physical += LARGE_PAGE;
                current_page_entry += 512;
                continue;
            } else {
                p2_full_array[current_page_entry / TABLE_SIZE] =
                    PDE64_ALL_ALLOWED | p1_entry_address as u64;
            }
        }
        // p1_full_array[current_page_entry] =
        //     PDE64_ALL_ALLOWED | (current_page_entry << PAGE_SHIFT) as u64;
        p1_full_array[current_page_entry] = protection_flags | base_physical as u64;

        // update variables used across loops
        previous_past_last_page = past_last_page;
        base_physical += PAGE_SIZE;
        current_page_entry += 1;
    }
    p1_full_array[current_page_entry..current_page_entry.next_multiple_of(TABLE_SIZE)].fill(0);
    previous_past_last_page
}

/// present_pages: a vec with start and end (address of last byte still written, so start + size -1) of all memory that does not need to be zeroed on first access.
/// The option points to the start of a copy on write segment, if there is any, if None the mapping can be installed directly
/// last_address: last address in the context available, not related to if it could be used
pub fn set_page_table(
    sregs: &mut kvm_sregs,
    guest_mem: &mut [u8],
    present_pages: Vec<(usize, usize, Option<usize>)>,
    mut stack_start: usize,
    interrupt_range: (usize, usize),
    last_address: usize,
) -> (usize, PageFaultMetadata) {
    // allocate top level table containing 512 entries for 512 GB ranges, total of 256 TB
    // naming:
    // - p4 is the top level page table overseeing 256TB, each entry being a p3 table which each oversees 512GB
    // - p3 table oversees 512GB, each p3 entry being a HUGE_PAGE (1GB) or a p2 table
    // - p2 table oversees 1GB, each entry being a LARGE PAGE (2MB) or a p1 table
    // - p1 table oversees 2MB, each entry being a PAGE (4KB).

    // for the VM to be able to run the first instruction without crashing,
    // at least the page with the first instruction and the page with the stack start
    // need to be accessible

    // always only need 1 p4 table, since it is top level
    stack_start -= PAGE_SIZE;
    let p4_address = stack_start;
    // always only allocate a single p3 table, since we do not expect functions to have more than 512GB of memory
    stack_start -= PAGE_SIZE;
    let p3_address = stack_start;
    assert!(last_address < (1 << 39));

    // check how many p2 tables we need and allocate them
    let p2_table_number = last_address.next_multiple_of(HUGE_PAGE) >> HUGE_PAGE_SHIFT;
    stack_start -= p2_table_number << PAGE_SHIFT;
    let p2_base = stack_start;
    // check how many p1 tables we need and allocate them
    let p1_table_number = p2_table_number * TABLE_SIZE;
    stack_start -= p1_table_number << PAGE_SHIFT;
    let p1_base = stack_start;

    // set up all the tables for easy access
    let (guest_mem, p4_raw) = guest_mem.split_at_mut(p4_address);
    let p4_table = u8_slice_to_u64_slice(&mut p4_raw[0..PAGE_SIZE]);
    p4_table.fill(0);
    let (guest_mem, p3_raw) = guest_mem.split_at_mut(p3_address);
    let p3_table = u8_slice_to_u64_slice(&mut p3_raw[0..PAGE_SIZE]);
    let (guest_mem, p2_raw) = guest_mem.split_at_mut(p2_base);
    let p2_full_array = u8_slice_to_u64_slice(&mut p2_raw[0..p2_table_number << PAGE_SHIFT]);
    let (guest_mem, p1_raw) = guest_mem.split_at_mut(p1_base);
    let p1_full_array = u8_slice_to_u64_slice(&mut p1_raw[0..p1_table_number << PAGE_SHIFT]);

    // set up the p4 table, which only has 1 entry at the start, the single p3 table
    p4_table[0] = PDE64_ALL_ALLOWED | p3_address as u64;

    // point the p3 table to all p2 tables
    {
        let mut p2_address = p2_base;
        for p3_entry in 0..p2_table_number {
            p3_table[p3_entry] = PDE64_ALL_ALLOWED | (p2_address) as u64;
            p2_full_array[p3_entry * TABLE_SIZE..(p3_entry + 1) * TABLE_SIZE].fill(0);
            p2_address += PAGE_SIZE;
        }
        p3_table[p2_table_number..].fill(0);
    };

    // TODO: remove this and introduce actual changes
    // for (index, p1_entry) in p1_full_array.iter_mut().enumerate() {
    //     *p1_entry = PDE64_PRESENT | PDE64_RW | PDE64_USER | (index * PAGE_SIZE) as u64;
    // }
    // for (index, p2_entry) in p2_full_array.iter_mut().enumerate() {
    //     *p2_entry =
    //         PDE64_PRESENT | PDE64_RW | PDE64_USER | (index * TABLE_SIZE * 8 + p1_base) as u64;
    // }

    // start installing entries for all already present pages
    let mut previous_past_last_page = 0;
    // the page starting at address 0 is expected to never be written
    if present_pages.len() > 0 {
        debug_assert_ne!(0, present_pages[0].0 >> PAGE_SHIFT);
    }
    for (guest_virtual_start, guest_virtual_end, origin_option) in present_pages {
        let (protection_flags, base_physical) = if let Some(guest_physical) = origin_option {
            // map to page at differnt space in guest, that is there and needs to be copy on write
            (PDE64_PRESENT | PDE64_USER, guest_physical)
        } else {
            // map directly, since content is already there
            (PDE64_ALL_ALLOWED, guest_virtual_start)
        };
        previous_past_last_page = set_range(
            p2_full_array,
            p1_full_array,
            p1_base,
            guest_virtual_start,
            guest_virtual_end + 1,
            protection_flags,
            base_physical,
            previous_past_last_page,
        );
    }

    // TODO MAKE ONLY SINGLE PAGE AND ZERO IT OUT
    // need to set page for stack
    p2_full_array[stack_start >> LARGE_PAGE_SHIFT] =
        PDE64_ALL_ALLOWED | PDE64_IS_PAGE | (stack_start & !(LARGE_PAGE - 1)) as u64;
    guest_mem[stack_start & !(LARGE_PAGE - 1)..stack_start].fill(0);

    set_range(
        p2_full_array,
        p1_full_array,
        p1_base,
        interrupt_range.0,
        interrupt_range.1,
        PDE64_PRESENT | PDE64_RW,
        interrupt_range.0,
        previous_past_last_page,
    );

    // go through tree and see if it is set up the way we expect
    #[cfg(debug_assertions)]
    {
        // try to resolve every page up to the stack pointer
        let mut page_address = 0;
        while page_address < last_address {
            // get all table entries
            let mut debug_info = format!(
                "p2_base: {}, p1_base: {}, for page address: {}",
                p2_base, p1_base, page_address
            );
            let p4_entry = get_entry_from_address(page_address, PML4_SHIFT);
            assert_eq!(0, p4_entry, "{}", debug_info);
            let p3_entry = get_entry_from_address(page_address, HUGE_PAGE_SHIFT);
            assert_eq!(0, p3_entry, "{}", debug_info);
            let p2_entry = get_entry_from_address(page_address, LARGE_PAGE_SHIFT);
            let p1_entry = get_entry_from_address(page_address, PAGE_SHIFT);

            let p3_address_local = p4_table[p4_entry] as usize & !(PAGE_SIZE - 1);
            let p3_flags = p4_table[p4_entry] & (PAGE_SIZE as u64 - 1);

            assert_eq!(p3_address, p3_address_local, "{}", debug_info);
            assert_eq!(
                PDE64_PRESENT | PDE64_RW | PDE64_USER,
                p3_flags,
                "{}",
                debug_info
            );

            let p2_address_local = p3_table[p3_entry] as usize & !(PAGE_SIZE - 1);
            let p2_flags = p3_table[p3_entry] & (PAGE_SIZE as u64 - 1);

            let expected_address = p2_base + (page_address / HUGE_PAGE) * size_of::<u64>();
            assert_eq!(expected_address, p2_address_local, "{}", debug_info);
            assert_eq!(
                PDE64_PRESENT | PDE64_RW | PDE64_USER,
                p2_flags,
                "{}",
                debug_info
            );

            let p2_index = p3_entry * TABLE_SIZE + p2_entry;
            let p1_address_local = p2_full_array[p2_index] as usize & !(PAGE_SIZE - 1);
            let p1_flags = p2_full_array[p2_index] & (PAGE_SIZE as u64 - 1);
            debug_info.push_str(
                format!(
                    " p1_laddress_local: {}, with p1_flags: {}",
                    p1_address_local, p1_flags
                )
                .as_str(),
            );
            if p1_flags & PDE64_PRESENT != 0 {
                if p1_flags & PDE64_IS_PAGE != 0 {
                    if p1_flags & PDE64_RW == 0 || p1_flags & PDE64_USER == 0 {
                        // copy on write large page or system page
                        assert!(page_address > stack_start, "{}", debug_info);
                    } else {
                        // directly mapped large page
                        let large_page_address = page_address & !(LARGE_PAGE - 1);
                        assert_eq!(large_page_address, p1_address_local, "{}", debug_info);
                    }
                } else {
                    // if it is not a page, go lower
                    assert!(p1_flags & PDE64_RW != 0, "{}", debug_info);
                    let p1_index = p2_index * TABLE_SIZE + p1_entry;
                    let page_address_local = p1_full_array[p1_index] as usize & !(PAGE_SIZE - 1);
                    let page_flags_local = p1_full_array[p1_index] & (PAGE_SIZE as u64 - 1);

                    debug_info.push_str(
                        format!(
                            " paged_address_local: {}, page_flags_local: {}",
                            page_address_local, page_flags_local
                        )
                        .as_str(),
                    );
                    if page_flags_local & PDE64_PRESENT != 0 {
                        if page_flags_local & PDE64_RW == 0 || page_flags_local & PDE64_USER == 0 {
                            assert!(page_address_local > stack_start, "{}", debug_info);
                        } else {
                            assert_eq!(page_address, page_address_local, "{}", debug_info);
                        }
                    }
                }
            } else {
                assert_eq!(0, p1_address_local, "{}", debug_info);
                assert_eq!(0, p1_flags, "{}", debug_info);
            }
            page_address += PAGE_SIZE;
        }
    }

    // // go through all p3 entries and call the correct setup
    // for p2_index in first_p2..past_last_p2 {
    //     // determine range of pages in this table that need to be touched on
    //     let p2_lowest_address = p3_entry * HUGE_PAGE;
    //     let local_start = std::cmp::max(guest_virtual_start, p2_lowest_address);
    //     let local_end = std::cmp::min(guest_virtual_end, p2_lowest_address + HUGE_PAGE);
    //     let first_p1 = (local_start % HUGE_PAGE) / LARGE_PAGE;
    //     let last_p1 = (local_end % HUGE_PAGE) / LARGE_PAGE;
    //     debug_assert!(last_p1 <= 512);
    //     debug_assert!(first_p1 < last_p1);

    //     let local_physical = guest_physical + (local_virtual - guest_virtual);

    //     let p2_offset = (p3_table[p3_entry] & !0xFFF) as usize;
    //     set_p2_table(
    //         local_virtual,
    //         virtual_end,
    //         local_physical,
    //         PDE64_PRESENT | PDE64_USER,
    //         p2_offset,
    //         guest_mem,
    //         &mut stack_start,
    //     );
    // }
    // }

    // // protect high address space from user, by removing the mappings
    // let first_p3 = stack_start / HUGE_PAGE;
    // // all later p3 entries can be protected directly
    // for p3_entry in first_p3 + 1..p2_table_number {
    //     let p3_table = u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
    //     p3_table[p3_entry] = 0;
    // }
    // let p3_table = u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
    // if (p3_table[first_p3] & PDE64_IS_PAGE) != 0 {
    //     let p2_address = (p3_table[first_p3] & !0xFFF) as usize;
    //     let p2_table = u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE]);
    //     let first_p2 = (stack_start / LARGE_PAGE) % 512;
    //     for p2_entry in first_p2 + 1..512 {
    //         p2_table[p2_entry] = 0;
    //     }
    //     if (p2_table[first_p2] & PDE64_IS_PAGE) != 0 {
    //         let p1_address = (p2_table[first_p2] & !0xFFF) as usize;
    //         let p1_table =
    //             u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE]);
    //         let first_p1 = (stack_start / PAGE_SIZE) % 512;
    //         for p1_entry in first_p1..512 {
    //             p1_table[p1_entry] = 0;
    //         }
    //     }
    // }

    sregs.cr3 = p4_address as u64;
    (
        stack_start,
        PageFaultMetadata {
            p4_address,
            p3_address,
            p2_base,
            p1_base,
            max_address: stack_start,
        },
    )
}

/// The mask to get the entry into a table with 512 entries of 8 bytes each
/// Need to zero out everything above 4096 (512 * 8) and the 3 bits at the end indexing into the 8 bytes
const ENTRY_MASK: usize = 512 - 1;
fn get_entry_from_address(address: usize, shift: usize) -> usize {
    (address >> shift) & ENTRY_MASK
}

fn get_offset_and_flags(memory: &mut [u8], offset: usize, entry: usize) -> (usize, u64) {
    let table = u8_slice_to_u64_slice(&mut memory[offset..offset + PAGE_SIZE]);
    let value = table[entry];
    (
        (value as usize & !(PAGE_SIZE - 1)),
        (value & (PAGE_SIZE as u64 - 1)),
    )
}

pub fn handle_page_fault(
    vcpu: &VcpuFd,
    metadata: &PageFaultMetadata,
    guest_mem: &mut [u8],
) -> DandelionResult<usize> {
    let regs = vcpu.get_regs().unwrap();
    let sregs = vcpu.get_sregs().unwrap();
    // the faulting address is in cr2, cr3 holds the root table address, rax holds the error code
    let faulting_address = sregs.cr2 as usize;
    let p4_address = sregs.cr3 as usize;
    let error_code = regs.rax;
    let not_present = error_code & 0b1 != 0;
    let write_error = error_code & 0b10 != 0;
    // base address of the page that was accessed
    let page_base_address = faulting_address & !(PAGE_SIZE - 1);

    if faulting_address >= metadata.max_address {
        return Err(DandelionError::UserError(UserError::SegmentationFault));
    }

    if p4_address != metadata.p4_address {
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    // get all table entries
    let p4_entry = get_entry_from_address(faulting_address, PML4_SHIFT);
    let p3_entry = get_entry_from_address(faulting_address, HUGE_PAGE_SHIFT);
    let p2_entry = get_entry_from_address(faulting_address, LARGE_PAGE_SHIFT);
    let p1_entry = get_entry_from_address(faulting_address, PAGE_SHIFT);

    let (p3_address, p3_flags) = get_offset_and_flags(guest_mem, p4_address, p4_entry);
    // check the directory table has the correct flags
    // should have all the default flags, as they are applied to all memory in scope
    if p3_flags & !PDE64_ACCESSED != PDE64_ALL_ALLOWED || p3_address != metadata.p3_address {
        trace!(
            "p3 flags not as expected: {}, for offset: {} (expected: {})",
            p3_flags,
            p3_address,
            metadata.p3_address,
        );
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    let (p2_address, p2_flags) = get_offset_and_flags(guest_mem, p3_address, p3_entry);

    // check the p2 is at the expected place
    if p2_address != (metadata.p2_base + faulting_address / HUGE_PAGE) {
        trace!(
            "p2 address not as expected: {}, found {}",
            p2_address,
            metadata.p2_base + faulting_address / HUGE_PAGE,
        );
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    let (p1_address, p1_flags) = get_offset_and_flags(guest_mem, p2_address, p2_entry);
    let canonical_p1_start = metadata.p1_base + faulting_address / LARGE_PAGE;

    // check if it currently is a page or pointing to a p1 table
    if p1_flags & PDE64_IS_PAGE != 0 {
        // if it currently is page, need to set up the p1 table
        // correct the p2 entry:
        u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE])[p2_entry] =
            PDE64_ALL_ALLOWED | canonical_p1_start as u64;
        let p1_table = u8_slice_to_u64_slice(
            &mut guest_mem[canonical_p1_start..canonical_p1_start + PAGE_SIZE],
        );

        if p2_flags & PDE64_PRESENT == 0 {
            // page was not present so we need to demand page a zero page
            debug_assert!(not_present);
            // rest of the p1 was not used so can simply zero fill it
            p1_table.fill(0);
            // need to set the p1 entry to the physical page it is supposed to point to
            // need to zero fill that page
            u8_slice_to_u64_slice(&mut guest_mem[page_base_address..page_base_address + PAGE_SIZE])
                .fill(0);
        } else {
            // page was present so it is a write error
            debug_assert!(write_error);
            debug_assert_eq!(
                PDE64_PRESENT | PDE64_IS_PAGE | PDE64_USER,
                p2_flags & !PDE64_ACCESSED
            );
            debug_assert!(
                p1_address >= metadata.max_address,
                "Copy on write page original was lower than max address"
            );
            // need to set the p1 entry to the physical page it is supposed to point to
            // and set the protected mapping for the remainder of the LARGE_PAGE
            let mut old_p2_page = p1_address;
            for entry in 0..TABLE_SIZE {
                p1_table[entry] = PDE64_PRESENT | PDE64_USER | old_p2_page as u64;
                old_p2_page += PAGE_SIZE;
            }
            // fill the writable page with the old content
            let (new_page, old_page) = guest_mem.split_at_mut(p1_address);
            new_page[page_base_address..page_base_address + PAGE_SIZE]
                .copy_from_slice(&old_page[p1_entry * PAGE_SIZE..(p1_entry + 1) * PAGE_SIZE]);
        };
    } else {
        // p2 entry should contain valid p1 table, so validate that
        if p1_address != canonical_p1_start {
            trace!(
                "p2 address not as expected: {}, found {}",
                p2_address,
                metadata.p2_base + faulting_address / HUGE_PAGE,
            );
            return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
        }
        // load page address from p1 table
        let (old_address, old_flags) =
            get_offset_and_flags(guest_mem, canonical_p1_start, p1_entry);
        if old_flags & PDE64_PRESENT != 0 {
            // page was not present so demand page it to zero and set entry
            debug_assert!(not_present);
            u8_slice_to_u64_slice(&mut guest_mem[page_base_address..page_base_address + PAGE_SIZE])
                .fill(0);
        } else {
            // page was present so it is a write error
            debug_assert!(write_error);
            debug_assert_eq!(
                PDE64_PRESENT | PDE64_IS_PAGE | PDE64_USER,
                p1_flags & !PDE64_ACCESSED
            );
            debug_assert!(
                old_address >= metadata.max_address,
                "Copy on write page original was lower than max address"
            );
            // fill the writable page with the old content
            let (new_page, old_page) = guest_mem.split_at_mut(old_address);
            new_page[page_base_address..page_base_address + PAGE_SIZE]
                .copy_from_slice(&old_page[..PAGE_SIZE]);
        }
    };

    // set the p1 entry to contain the correct address
    let p1_table =
        u8_slice_to_u64_slice(&mut guest_mem[canonical_p1_start..canonical_p1_start + PAGE_SIZE]);
    p1_table[p1_entry] = page_base_address as u64 | PDE64_ALL_ALLOWED;
    Ok(page_base_address)
}

fn setup_long_mode(sregs: &mut kvm_sregs) {
    sregs.cr4 = CR4_PAE | CR4_OSFXSR | CR4_OSXMMEXCPT | CR4_FSGSBASE;
    sregs.cr0 = CR0_PE | CR0_MP | CR0_ET | CR0_NE | CR0_WP | CR0_AM | CR0_PG;
    sregs.efer = EFER_LME | EFER_LMA;
    // set dpl on both code and stack segment to make sure it is user level
    // let priviledge_level = 0u8;
    let priviledge_level = 3u8;
    let code_selector = if priviledge_level == 0 { 2 } else { 6 };
    sregs.cs = kvm_segment {
        base: 0,
        limit: 0xFFFF_FFFF,
        selector: (code_selector) << 3 | (priviledge_level as u16),
        type_: 11,
        present: 1,
        dpl: priviledge_level,
        db: 0,
        s: 1,
        l: 1,
        g: 1,
        ..Default::default()
    };
    sregs.ss = kvm_segment {
        base: 0,
        limit: 0xFFFF_FFFF,
        selector: (code_selector + 2) << 3 | (priviledge_level as u16),
        type_: 3,
        present: 1,
        dpl: priviledge_level,
        db: 0,
        s: 1,
        l: 1,
        g: 1,
        ..Default::default()
    };
}

pub fn dump_regs(_vcpu: &VcpuFd) {
    #[cfg(feature = "backend_debug")]
    {
        let regs = _vcpu.get_regs().unwrap();
        trace!("Register state: ");
        trace!(
            "rax:\t{:>#10x}, rbx:\t{:>#10x}, rcx:\t{:>#10x}, rdx:\t{:>#10x}",
            regs.rax,
            regs.rbx,
            regs.rcx,
            regs.rdx
        );
        trace!(
            "rsi:\t{:>#10x}, rdi:\t{:>#10x}, rsp:\t{:>#10x}, rbp:\t{:>#10x}",
            regs.rsi,
            regs.rdi,
            regs.rsp,
            regs.rbp
        );
        trace!(
            "r8: \t{:>#10x}, r9: \t{:>#10x}, r10:\t{:>#10x}, r11:\t{:>#10x}",
            regs.r8,
            regs.r9,
            regs.r10,
            regs.r11
        );
        trace!(
            "r12:\t{:>#10x}, r13:\t{:>#10x}, r14:\t{:>#10x}, r15:\t{:>#10x}",
            regs.r12,
            regs.r13,
            regs.r14,
            regs.r15
        );
        trace!("rip:\t{:>#10x}, rflags:\t{:>#10x}", regs.rip, regs.rflags,);

        let sregs = _vcpu.get_sregs().unwrap();
        trace!("System registers");
        trace!("cs:\t{:?}", sregs.cs);
        trace!("ss:\t{:?}", sregs.ss);
        trace!("ds:\t{:?}", sregs.ds);
        trace!("es:\t{:?}", sregs.es);
        trace!("fs:\t{:?}", sregs.fs);
        trace!("gs:\t{:?}", sregs.gs);
        trace!("tr:\t{:?}", sregs.tr);
        trace!("ldt:\t{:?}", sregs.ldt);
        trace!("gdt:\t{:?}", sregs.gdt);
        trace!("idt:\t{:?}", sregs.idt);
        trace!(
            "cr0: \t{:>#10x}, cr2: \t{:>#10x}, cr3:\t{:>#10x}, cr4:\t{:>#10x}",
            sregs.cr0,
            sregs.cr2,
            sregs.cr3,
            sregs.cr4
        );
        trace!(
            "cr8:\t{:>#10x}, efer:\t{:>#10x}, apci_base:\t{:>#10x}",
            sregs.cr8,
            sregs.efer,
            sregs.apic_base
        );
        trace!("interrupt_bitmap: {:?}", sregs.interrupt_bitmap);

        let events = _vcpu.get_vcpu_events().unwrap();
        trace!("events: {:?}\n", events);
    }
}
