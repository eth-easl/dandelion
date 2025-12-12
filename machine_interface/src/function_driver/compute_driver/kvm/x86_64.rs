use dandelion_commons::{DandelionError, DandelionResult, UserError};
use kvm_bindings::{kvm_fpu, kvm_regs, kvm_segment, kvm_sregs};
use kvm_ioctls::{VcpuFd, VmFd};
use log::{debug, trace};
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
/// Set by hardware if entry has been use to write to linaer address
const PDE64_DIRTY: u64 = 1 << 6;
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
const PAGE_MASK: u64 = (PAGE_SIZE - 1) as u64;
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

global_asm!(include_str!("x86_64.asm"),
    PAGE_SIZE = const PAGE_SIZE,
    LARGE_PAGE = const LARGE_PAGE,
    PDE64_PRESENT = const PDE64_PRESENT,
    PDE64_USER = const PDE64_USER,
    PDE64_ALL_ALLOWED = const PDE64_ALL_ALLOWED,
    PDE64_IS_PAGE = const PDE64_IS_PAGE
);
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
    ) -> DandelionResult<PageFaultMetadata> {
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
        )?;

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
        Ok(page_fault_metadata)
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
    /// Base address for the tables with p2 and p1 tables
    table_base: usize,
    /// The highest address for which page faults should be resolved.
    /// If the fault lies above, the function tried to access memory that was not mapped on purpose
    stack_start: usize,
}

impl PageFaultMetadata {
    pub fn get_stack_start(&self) -> usize {
        self.stack_start
    }
}

fn get_p2(current_page_entry: usize) -> (usize, usize) {
    let p2_base = (current_page_entry / (TABLE_SIZE * TABLE_SIZE)) * (TABLE_SIZE + 1) * TABLE_SIZE;
    let p2_entry = (current_page_entry / TABLE_SIZE) % TABLE_SIZE;
    (p2_base, p2_entry)
}

fn get_address_and_flags(entry: u64) -> (usize, u64) {
    ((entry & !PAGE_MASK) as usize, entry & PAGE_MASK)
}

fn set_range(
    table_array: &mut [u64],
    table_base: usize,
    virtual_start: usize,
    virtual_end: usize,
    protection_flags: u64,
    mut base_physical: usize,
    previous_past_last_page: usize,
) -> usize {
    // in check which page tables entries need to be set
    let mut current_page_entry = virtual_start >> PAGE_SHIFT;
    let past_last_page = virtual_end >> PAGE_SHIFT;
    // Ensure assumption that there is never overlap between the previous and the current range
    debug_assert!(previous_past_last_page <= current_page_entry);

    // if the p2 table for the first page has not been set up, need to do it now
    // it has been set up, if the previous last page was in the same large page,
    // which is the case if the previous last page rounded up to the next large page end is not smaller
    // or if we won't set it up in the loop anyway, <= since the previous last page is set to 1 past_last_page
    if previous_past_last_page.next_multiple_of(TABLE_SIZE) <= current_page_entry
        && current_page_entry % TABLE_SIZE != 0
    {
        let (p2_base, p2_entry) = get_p2(current_page_entry);
        let p1_offset = p2_base + (1 + p2_entry) * TABLE_SIZE;
        table_array[p2_base + p2_entry] =
            PDE64_ALL_ALLOWED | (table_base + p1_offset * size_of::<u64>()) as u64;
        table_array[p1_offset..p1_offset + (current_page_entry % TABLE_SIZE)].fill(0);
    }
    while current_page_entry < past_last_page {
        // check if we are entering a region under a new p2 entry
        let (p2_base, p2_entry) = get_p2(current_page_entry);
        let p1_offset = current_page_entry % TABLE_SIZE;
        if p1_offset == 0 {
            if current_page_entry + TABLE_SIZE <= past_last_page {
                table_array[p2_base + p2_entry] =
                    protection_flags | PDE64_IS_PAGE | base_physical as u64;
                base_physical += LARGE_PAGE;
                current_page_entry += TABLE_SIZE;
                continue;
            } else {
                table_array[p2_base + p2_entry] = PDE64_ALL_ALLOWED
                    | (table_base + (p2_base + (1 + p2_entry) * TABLE_SIZE) * size_of::<u64>())
                        as u64;
            }
        }
        let p1_base = p2_base + (1 + p2_entry) * TABLE_SIZE;
        table_array[p1_base + p1_offset] = protection_flags | base_physical as u64;
        // update variables used across loops
        base_physical += PAGE_SIZE;
        current_page_entry += 1;
    }
    // need to make sure that any p1 table that was started is filled with 0 to the end
    // current page entry is now at past_last_page, need to check before accessing, in case it is at the end of the
    // table array
    if current_page_entry % TABLE_SIZE != 0 {
        let last_index =
            current_page_entry + TABLE_SIZE * (1 + current_page_entry / (TABLE_SIZE * TABLE_SIZE));
        table_array[last_index..last_index.next_multiple_of(TABLE_SIZE)].fill(0);
    }
    past_last_page
}

/// present_pages: a vec with start and end (address of last byte still written, so start + size -1) of all memory that does not need to be zeroed on first access.
/// The option points to the start of a copy on write segment, if there is any, if None the mapping can be installed directly
/// last_address: last address in the context available, not related to if it could be used
fn set_page_table(
    sregs: &mut kvm_sregs,
    guest_mem: &mut [u8],
    present_pages: Vec<(usize, usize, Option<usize>)>,
    mut stack_start: usize,
    interrupt_range: (usize, usize),
    last_address: usize,
) -> DandelionResult<(usize, PageFaultMetadata)> {
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

    // check how many p2 and p1 tables we need and allocate them
    let p2_table_number = last_address.next_multiple_of(HUGE_PAGE) >> HUGE_PAGE_SHIFT;
    let p1_table_number = p2_table_number * TABLE_SIZE;
    stack_start -= (p2_table_number + p1_table_number) << PAGE_SHIFT;
    let table_base = stack_start;
    // have a joint table for p2 and p1 tables with 1 p2 table followed by the 512 p1 tables it could point to
    // makes it easier to find the p1 table in the asm page fault handler if we need to install it,
    // since we can just offset from the p2, which is always present

    // set up all the tables for easy access
    let (guest_mem, p4_raw) = guest_mem.split_at_mut(p4_address);
    let p4_table = u8_slice_to_u64_slice(&mut p4_raw[0..PAGE_SIZE]);
    p4_table.fill(0);
    let (guest_mem, p3_raw) = guest_mem.split_at_mut(p3_address);
    let p3_table = u8_slice_to_u64_slice(&mut p3_raw[0..PAGE_SIZE]);
    let (guest_mem, table_raw) = guest_mem.split_at_mut(table_base);
    let table_array =
        u8_slice_to_u64_slice(&mut table_raw[0..(p2_table_number + p1_table_number) << PAGE_SHIFT]);

    // set up the p4 table, which only has 1 entry at the start, the single p3 table
    p4_table[0] = PDE64_ALL_ALLOWED | p3_address as u64;

    // point the p3 table to all p2 tables
    {
        let mut p2_address = table_base;
        for p3_entry in 0..p2_table_number {
            p3_table[p3_entry] = PDE64_ALL_ALLOWED | (p2_address) as u64;
            let start_index = p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE;
            table_array[start_index..start_index + TABLE_SIZE].fill(0);
            p2_address += (TABLE_SIZE + 1) * PAGE_SIZE;
        }
        p3_table[p2_table_number..].fill(0);
    };
    // can assume that all p2 tables are zero initialized after this loop

    // TODO: remove this and introduce actual changes, or hide it as debug option
    // for p2_index in 0..p1_table_number {
    //     let p2_entry =
    //         (p2_index % TABLE_SIZE) + (p2_index / TABLE_SIZE) * (TABLE_SIZE * (TABLE_SIZE + 1));
    //     let p1_address = table_base
    //         + ((p2_index / TABLE_SIZE) * TABLE_SIZE * (TABLE_SIZE + 1)
    //             + ((p2_index % TABLE_SIZE) + 1))
    //             * PAGE_SIZE;
    //     // println!("p2_entry: {}, p1_address: {}", p2_entry, p1_address);
    //     table_array[p2_entry] = PDE64_PRESENT | PDE64_RW | PDE64_USER | p1_address as u64;
    //     for p1_index in 0..TABLE_SIZE {
    //         let p1_entry = (p2_index / TABLE_SIZE) * (TABLE_SIZE * (TABLE_SIZE + 1))
    //             + (p2_index + 1) * TABLE_SIZE
    //             + p1_index;
    //         table_array[p1_entry] = PDE64_PRESENT
    //             | PDE64_RW
    //             | PDE64_USER
    //             | (p2_index * LARGE_PAGE + p1_index * PAGE_SIZE) as u64;
    //     }
    // }

    // start installing entries for all already present pages
    let mut previous_past_last_page = 0;
    // the page starting at address 0 is expected to never be written
    if present_pages.len() > 0 {
        debug_assert_ne!(0, present_pages[0].0 >> PAGE_SHIFT);
        if let Some((_, virtual_end, _)) = present_pages.last() {
            if stack_start <= *virtual_end {
                return Err(DandelionError::ContextFull);
            }
        }
    }
    for (guest_virtual_start, guest_virtual_end, origin_option) in present_pages {
        let (protection_flags, base_physical) = if let Some(guest_physical) = origin_option {
            // map to page at differnt space in guest, that is there and needs to be copy on write
            (PDE64_PRESENT | PDE64_USER, guest_physical)
        } else {
            // map directly, since content is already there
            (PDE64_ALL_ALLOWED, guest_virtual_start)
        };
        debug!(
            "mapping new address range mapped {} to {}",
            guest_virtual_start,
            guest_virtual_end + 1
        );
        previous_past_last_page = set_range(
            table_array,
            table_base,
            guest_virtual_start,
            guest_virtual_end + 1,
            protection_flags,
            base_physical,
            previous_past_last_page,
        );
    }

    // need to set page for stack
    let stack_page_start = stack_start & !(LARGE_PAGE - 1);
    guest_mem[stack_page_start..stack_start].fill(0);
    previous_past_last_page = set_range(
        table_array,
        table_base,
        stack_page_start,
        stack_start,
        PDE64_ALL_ALLOWED,
        stack_page_start,
        previous_past_last_page,
    );

    // the page tables also need to be accessable to root mode
    previous_past_last_page = set_range(
        table_array,
        table_base,
        table_base,
        p4_address + PAGE_SIZE,
        PDE64_PRESENT | PDE64_RW,
        table_base,
        previous_past_last_page,
    );

    // need to make the page table handler accessable to root mode
    previous_past_last_page = set_range(
        table_array,
        table_base,
        interrupt_range.0,
        interrupt_range.1,
        PDE64_PRESENT | PDE64_RW,
        interrupt_range.0,
        previous_past_last_page,
    );

    // make the read only mappings also avaliable at their physical locations for root mode
    set_range(
        table_array,
        table_base,
        interrupt_range.1,
        last_address,
        PDE64_PRESENT,
        interrupt_range.1,
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
                "table_base: {}\npage address: {}\n",
                table_base, page_address,
            );
            let p4_entry = get_entry_from_address(page_address, PML4_SHIFT);
            assert_eq!(0, p4_entry, "{}", debug_info);
            let p3_entry = get_entry_from_address(page_address, HUGE_PAGE_SHIFT);
            let p2_entry = get_entry_from_address(page_address, LARGE_PAGE_SHIFT);
            let p1_entry = get_entry_from_address(page_address, PAGE_SHIFT);

            debug_info.push_str(
                format!(
                    "p3 entry: {}, p2_entry: {} p1_entry: {}\n",
                    p3_entry, p2_entry, p1_entry
                )
                .as_str(),
            );

            let (p3_address_local, p3_flags) = get_address_and_flags(p4_table[p4_entry]);
            assert_eq!(p3_address, p3_address_local, "{}", debug_info);
            assert_eq!(
                PDE64_PRESENT | PDE64_RW | PDE64_USER,
                p3_flags,
                "{}",
                debug_info
            );

            let (p2_address_local, p2_flags) = get_address_and_flags(p3_table[p3_entry]);
            let expected_address = table_base + (p3_entry * (TABLE_SIZE + 1)) * PAGE_SIZE;
            assert_eq!(expected_address, p2_address_local, "{}", debug_info);
            assert_eq!(
                PDE64_PRESENT | PDE64_RW | PDE64_USER,
                p2_flags,
                "{}",
                debug_info
            );
            debug_info.push_str(
                format!(
                    "p2_address: {}, with p2_flags: {} for large page: {}\n",
                    p2_address_local,
                    p2_flags,
                    p3_entry * HUGE_PAGE + p2_entry * LARGE_PAGE,
                )
                .as_str(),
            );

            let p2_index = p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE + p2_entry;
            let (p1_address_local, p1_flags) = get_address_and_flags(table_array[p2_index]);
            debug_info.push_str(
                format!(
                    "p1_address_local: {}, with p1_flags: {}\n",
                    p1_address_local, p1_flags
                )
                .as_str(),
            );
            if p1_flags & PDE64_PRESENT != 0 {
                if p1_flags & PDE64_IS_PAGE != 0 {
                    if p1_flags & PDE64_RW == 0 || p1_flags & PDE64_USER == 0 {
                        // copy on write large page or system page
                        assert!(p1_address_local > stack_start, "{}", debug_info);
                    } else {
                        // directly mapped large page
                        let large_page_address = page_address & !(LARGE_PAGE - 1);
                        assert_eq!(large_page_address, p1_address_local, "{}", debug_info);
                    }
                } else {
                    // if it is not a page, go lower
                    assert!(p1_flags & PDE64_RW != 0, "{}", debug_info);
                    let p1_index = TABLE_SIZE
                        + p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE
                        + p2_entry * TABLE_SIZE
                        + p1_entry;
                    let (page_address_local, page_flags_local) =
                        get_address_and_flags(table_array[p1_index]);

                    debug_info.push_str(
                        format!(
                            "paged_address_local: {}, page_flags_local: {}\n",
                            page_address_local, page_flags_local
                        )
                        .as_str(),
                    );
                    if page_flags_local & PDE64_PRESENT != 0 {
                        if page_flags_local & PDE64_RW == 0 || page_flags_local & PDE64_USER == 0 {
                            assert!(page_address_local >= stack_start, "{}", debug_info);
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

    sregs.cr3 = p4_address as u64;
    Ok((
        stack_start,
        PageFaultMetadata {
            p4_address,
            p3_address,
            table_base,
            stack_start,
        },
    ))
}

/// The mask to get the entry into a table with 512 entries of 8 bytes each
/// Need to zero out everything above 4096 (512 * 8) and the 3 bits at the end indexing into the 8 bytes
const ENTRY_MASK: usize = 512 - 1;
fn get_entry_from_address(address: usize, shift: usize) -> usize {
    (address >> shift) & ENTRY_MASK
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
    // let error_code = regs.rax;
    // let _not_present = error_code & 0b1 != 0;
    // let write_error = error_code & 0b10 != 0;
    // base address of the page that was accessed
    let page_base_address = faulting_address & !(PAGE_SIZE - 1);

    debug!("Starting to handle page fault at {}", faulting_address);
    if faulting_address < PAGE_SIZE || metadata.stack_start <= faulting_address {
        return Err(DandelionError::UserError(UserError::SegmentationFault));
    }

    if p4_address != metadata.p4_address {
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    // get all table entries
    let p4_entry = get_entry_from_address(faulting_address, PML4_SHIFT);
    assert_eq!(0, p4_entry);
    let p3_entry = get_entry_from_address(faulting_address, HUGE_PAGE_SHIFT);
    let p2_entry = get_entry_from_address(faulting_address, LARGE_PAGE_SHIFT);
    let p1_entry = get_entry_from_address(faulting_address, PAGE_SHIFT);

    // Asserts for the exit of the debug assembly
    // r10 and r11 should contain addresses for the entries of the p4 and p3 entries
    assert_eq!(regs.r10, (metadata.p4_address) as u64);
    assert_eq!(
        regs.r11,
        (metadata.p3_address + p3_entry * size_of::<u64>()) as u64
    );
    // r12 and r13 hold the indices into the p2 and p1 tables respectively
    assert_eq!(regs.r12, (p2_entry * size_of::<u64>()) as u64);
    assert_eq!(regs.r13, (p1_entry * size_of::<u64>()) as u64);
    // r14 and p15 hold the p2 and p1 base address respecively
    let p2_offset = (p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE) * size_of::<u64>();
    assert_eq!(
        regs.r14,
        (metadata.table_base + p2_offset * size_of::<u64>()) as u64
    );
    let p1_offset = ((p3_entry * (TABLE_SIZE + 1) + 1 + p2_entry) * TABLE_SIZE) * size_of::<u64>();
    debug!(
        "table_base: {}, p2 address: {}",
        metadata.table_base, regs.r14
    );
    assert_eq!(regs.r15, (metadata.table_base + p1_offset) as u64);

    // go through the page table and check it is correct
    let (p3_address, p3_flags) = get_address_and_flags(
        u8_slice_to_u64_slice(&mut guest_mem[p4_address..p4_address + PAGE_SIZE])[p4_entry],
    );
    assert_eq!(p3_address, metadata.p3_address);
    assert_eq!(p3_flags & !PDE64_ACCESSED, PDE64_ALL_ALLOWED);

    let (p2_address, p2_flags) = get_address_and_flags(
        u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE])[p3_entry],
    );
    let canonical_p2_start =
        metadata.table_base + (p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE) * size_of::<u64>();
    assert_eq!(p2_address, canonical_p2_start);
    assert_eq!(p2_flags & !PDE64_ACCESSED, PDE64_ALL_ALLOWED);

    let (p1_address, p1_flags) = get_address_and_flags(
        u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE])[p2_entry],
    );
    let canonical_p1_start = canonical_p2_start + ((p2_entry + 1) * TABLE_SIZE) * size_of::<u64>();
    assert_eq!(p1_address, canonical_p1_start);
    assert_eq!(
        p1_flags & !PDE64_ACCESSED,
        PDE64_ALL_ALLOWED,
        "r15 is holding: {}",
        regs.r15
    );

    let (page_address, page_flags) = get_address_and_flags(
        u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])[p1_entry],
    );
    assert_eq!(page_address, page_base_address);
    assert_eq!(
        page_flags & !(PDE64_ACCESSED | PDE64_DIRTY),
        PDE64_ALL_ALLOWED
    );

    // r8 should beholding the previous p2 entry, so check that also
    let handler_p1_address = regs.r8 & !(PAGE_SIZE as u64 - 1);
    let handler_p1_flags = regs.r8 & (PAGE_SIZE as u64 - 1);
    let handler_page_address = regs.r9 & !(PAGE_SIZE as u64 - 1);
    let handler_page_flags = regs.r9 & (PAGE_SIZE as u64 - 1);
    debug!(
        "handler p2 entry, address: {}, flags: {}",
        handler_p1_address, handler_p1_flags
    );
    debug!(
        "handler p1 entry, address: {}, flags: {}",
        handler_page_address, handler_page_flags
    );

    if handler_p1_flags & PDE64_PRESENT == 0 || handler_p1_flags & PDE64_IS_PAGE != 0 {
        // if it currently not present or is a LARGE_PAGE page, need to set up the p1 table
        trace!("Checking p2 fault handling");
        // check if actual page is reasonably set
        if handler_p1_flags & PDE64_PRESENT == 0 {
            // check that the remainder of the entries have been set to 0
            for p1_index in 0..p1_entry {
                let set_value =
                    u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
                        [p1_index];
                assert_eq!(set_value, 0, "at index {}", p1_index);
            }
            let new_entry =
                u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])[p1_entry];
            assert_eq!(
                new_entry & !(PDE64_DIRTY | PDE64_ACCESSED),
                PDE64_ALL_ALLOWED | page_base_address as u64
            );
            for p1_index in p1_entry + 1..TABLE_SIZE {
                let set_value =
                    u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
                        [p1_index];
                assert_eq!(set_value, 0, "at index {}", p1_index);
            }
            for page_index in 0..TABLE_SIZE {
                let set_value = u8_slice_to_u64_slice(
                    &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
                )[page_index];
                assert_eq!(set_value, 0, "at index {}", page_index);
            }
            // let large_page_base = faulting_address & !(LARGE_PAGE - 1);
            // correct the p2 entry:
            // u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE])[p2_entry] =
            //     PDE64_ALL_ALLOWED | PDE64_IS_PAGE | large_page_base as u64;
            // let p1_table = u8_slice_to_u64_slice(
            //     &mut guest_mem[canonical_p1_start..canonical_p1_start + PAGE_SIZE],
            // );
            // page was not present so we need to demand page a zero page
            // TODO: this an later check return error code 6 often, which is a user and write error instead of the expected present error
            // there could be something about the order in which things are checked that need to be understood to make this check correctly
            // debug_assert!(
            //     not_present,
            //     "page not present, but error code is {}, for address {}",
            //     error_code, faulting_address
            // );
            // trace!("Converting not present p1 table to 0s with 1 entry");
            // rest of the p1 was not used so can simply zero fill it
            // p1_table.fill(0);
            // need to set the p1 entry to the physical page it is supposed to point to
            // need to zero fill that page
            // u8_slice_to_u64_slice(&mut guest_mem[large_page_base..large_page_base + LARGE_PAGE])
            //     .fill(0);
        } else {
            let old_address = handler_p1_address as usize;
            assert_eq!(old_address % LARGE_PAGE, 0);
            // check the p1 table
            for p1_index in 0..p1_entry {
                let set_val =
                    u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
                        [p1_index];
                assert_eq!(
                    set_val,
                    PDE64_USER
                        | PDE64_PRESENT
                        | (handler_p1_address + (p1_index * PAGE_SIZE) as u64),
                    "at index {}",
                    p1_index
                );
            }
            let set_val =
                u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])[p1_entry];
            assert_eq!(set_val, PDE64_ALL_ALLOWED | page_base_address as u64);
            for p1_index in p1_entry..TABLE_SIZE {
                let set_val =
                    u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
                        [p1_index];
                assert_eq!(
                    set_val,
                    PDE64_USER
                        | PDE64_PRESENT
                        | (handler_p1_address + (p1_index * PAGE_SIZE) as u64),
                    "at index {}",
                    p1_index
                );
            }
            for page_index in 0..TABLE_SIZE {
                let set_value = u8_slice_to_u64_slice(
                    &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
                )[page_index];
                let expected = u8_slice_to_u64_slice(
                    &mut guest_mem[old_address + p2_entry * PAGE_SIZE
                        ..old_address + p2_entry * PAGE_SIZE + PAGE_SIZE],
                )[page_index];
                assert_eq!(
                    set_value, expected,
                    "at index {} for base {}",
                    page_index, page_base_address
                );
            }
            // check the page
            // correct the p2 entry:
            // u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE])[p2_entry] =
            //     PDE64_ALL_ALLOWED | canonical_p1_start as u64;
            // let p1_table = u8_slice_to_u64_slice(
            //     &mut guest_mem[canonical_p1_start..canonical_p1_start + PAGE_SIZE],
            // );
            // page was present so it is a write error
            // debug_assert!(write_error);
            // debug_assert_eq!(
            //     PDE64_PRESENT | PDE64_IS_PAGE | PDE64_USER,
            //     p1_flags & !PDE64_ACCESSED,
            //     "Flag check failed, for p2 present, but write protected at {}",
            //     faulting_address
            // );
            // debug_assert!(
            //     p1_address >= metadata.stack_start,
            //     "Copy on write page original was lower than max address"
            // );
            // need to set the p1 entry to the physical page it is supposed to point to
            // and set the protected mapping for the remainder of the LARGE_PAGE
            // let mut old_p2_page = p1_address;
            // for entry in 0..TABLE_SIZE {
            //     p1_table[entry] = PDE64_PRESENT | PDE64_USER | old_p2_page as u64;
            //     old_p2_page += PAGE_SIZE;
            // }
            // fill the writable page with the old content
            // let (new_page, old_page) = guest_mem.split_at_mut(p1_address);
            // new_page[page_base_address..page_base_address + PAGE_SIZE]
            //     .copy_from_slice(&old_page[p1_entry * PAGE_SIZE..(p1_entry + 1) * PAGE_SIZE]);
        };
    } else {
        // p2 entry should contain valid p1 table, so validate that
        // if p1_address != canonical_p1_start {
        //     trace!(
        //         "p1 address not as expected: {}, found {}, for faulting address: {}",
        //         p1_address,
        //         canonical_p1_start,
        //         faulting_address
        //     );
        //     return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
        // }
        // load page address from p1 table
        // let (old_address, old_flags) =
        // get_offset_and_flags(guest_mem, canonical_p1_start, p1_entry);
        if handler_page_flags & PDE64_PRESENT == 0 {
            // trace!("demand page 0 page at {}", page_base_address);
            // page was not present so demand page it to zero and set entry
            // debug_assert!(
            //     not_present,
            //     "page not present, but error code is {}, for address {}",
            //     error_code, faulting_address,
            // );
            for page_index in 0..TABLE_SIZE {
                let set_value = u8_slice_to_u64_slice(
                    &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
                )[page_index];
                assert_eq!(
                    set_value, 0,
                    "at index {} for address: {}",
                    page_index, page_base_address
                );
            }
        } else {
            // page was present so it is a write error
            // debug_assert!(write_error);
            // debug_assert_eq!(
            //     PDE64_PRESENT | PDE64_USER,
            //     old_flags & !(PDE64_ACCESSED | PDE64_DIRTY)
            // );
            // debug_assert!(
            //     old_address >= metadata.stack_start,
            //     "Copy on write page original was lower than max address"
            // );
            // trace!(
            //     "copy page over at address {} from {}",
            //     page_base_address,
            //     old_address
            // );
            // fill the writable page with the old content
            // let (new_page, old_page) = guest_mem.split_at_mut(old_address);
            // new_page[page_base_address..page_base_address + PAGE_SIZE]
            //     .copy_from_slice(&old_page[..PAGE_SIZE]);
            let old_address = handler_page_address as usize;
            for page_index in 0..TABLE_SIZE {
                let set_value = u8_slice_to_u64_slice(
                    &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
                )[page_index];
                let expected =
                    u8_slice_to_u64_slice(&mut guest_mem[old_address..old_address + PAGE_SIZE])
                        [page_index];
                assert_eq!(set_value, expected, "at index {}", page_index);
            }
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
