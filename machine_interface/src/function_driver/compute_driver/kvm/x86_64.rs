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
const PDE64_DEFAULT_FLAGS: u64 = PDE64_PRESENT | PDE64_RW | PDE64_USER;
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

    /// copy_on_write_pages: mappings that need to be copy on write,
    /// each list entry contains the guest virtual, guest physical and size
    /// last_address: the total size of the address space attached to the vm, including additional read only segments after stack
    pub fn init_vcpu(
        &self,
        vcpu: &VcpuFd,
        entry_point: u64,
        guest_mem: &mut [u8],
        copy_on_write_pages: Vec<(usize, usize, usize)>,
        mut stack_pointer: usize,
        last_address: usize,
    ) {
        let mut sregs = self.sregs.clone();
        set_interrupt_table(&mut sregs, guest_mem, &mut stack_pointer);
        stack_pointer = set_page_table(
            &mut sregs,
            guest_mem,
            copy_on_write_pages,
            stack_pointer,
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
///
/// Each descriptor is built as follows
fn set_interrupt_table(sregs: &mut kvm_sregs, guest_mem: &mut [u8], stack_start: &mut usize) {
    // constants given by the manuals or how we set it up
    const GDT_SIZE: u16 = 80;
    const TSS_SIZE: u16 = 104;

    // setup memory space
    *stack_start -= PAGE_SIZE;
    let interrupt_handler = *stack_start;
    let handler_length = (fault_handlers_end as *const c_void).addr()
        - (asm_start as *const () as *const c_void).addr();

    let rounded_handler = handler_length.next_multiple_of(8);
    let gdt = if rounded_handler + ((GDT_SIZE + TSS_SIZE) as usize) < PAGE_SIZE {
        rounded_handler
    } else {
        *stack_start -= PAGE_SIZE;
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

    // set the revevant parts of the TSS
    let tss = &mut guest_mem[tss_start..tss_end];
    // set the interrupt stack address
    *stack_start -= PAGE_SIZE;
    tss[36..44].copy_from_slice(&(*stack_start).to_le_bytes());

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

/// Assumes virtual and physical are page alligned, size is multiple of page size.
/// Want to set page mapping granularity for everything for now, to make the copys cheap by default.
fn set_p2_table(
    virtual_start: usize,
    virtual_end: usize,
    physical: usize,
    flags: u64,
    p2_offset: usize,
    memory: &mut [u8],
    stack_start: &mut usize,
) {
    // assuming p2 table is the correct one for the virtual start
    let p2_base_address = virtual_start & !(HUGE_PAGE - 1);
    let first_entry = (virtual_start >> LARGE_PAGE_SHIFT) & (512 - 1);
    let last_entry = (virtual_end >> LARGE_PAGE_SHIFT) & (512 - 1);
    let mut p1_base_address = p2_base_address + first_entry * LARGE_PAGE;

    for entry in first_entry..=last_entry {
        let p2_table = u8_slice_to_u64_slice(&mut memory[p2_offset..p2_offset + PAGE_SIZE]);
        // check if there is alreay a valid p1 table
        let (p1_offset, new_table) = if p2_table[entry] & PDE64_IS_PAGE != 0 {
            *stack_start -= PAGE_SIZE;
            p2_table[entry] = PDE64_DEFAULT_FLAGS | (*stack_start as u64);
            (*stack_start, true)
        } else {
            ((p2_table[entry] & !0xFFF) as usize, false)
        };
        let p1_table = u8_slice_to_u64_slice(&mut memory[p1_offset..p1_offset + PAGE_SIZE]);
        let (p1_first_entry, p1_physical) = if virtual_start > p1_base_address {
            ((virtual_start >> PAGE_SHIFT) & (512 - 1), physical)
        } else {
            (0, physical + p1_base_address - virtual_start)
        };
        let p1_last_entry = if virtual_end < p1_base_address + LARGE_PAGE {
            virtual_end >> PAGE_SHIFT & (512 - 1)
        } else {
            512
        };
        set_p1_table(
            p1_base_address,
            p1_first_entry,
            p1_last_entry,
            p1_physical,
            flags,
            p1_table,
            new_table,
        );
        p1_base_address += LARGE_PAGE;
    }
}

/// Assumes virtual and physical are page alligned, size is a mutliple of page size.
fn set_p1_table(
    base_address: usize,
    first_entry: usize,
    last_entry: usize,
    physical_start: usize,
    flags: u64,
    table: &mut [u64],
    new_table: bool,
) {
    // not setting the IS_PAGE flag, since that has a different meaning for 4K pages,
    // since they cannot point to another table
    if new_table {
        for entry in 0..first_entry {
            table[entry] = PDE64_DEFAULT_FLAGS | (base_address + entry * PAGE_SIZE) as u64;
        }
    }
    for entry in first_entry..last_entry {
        table[entry] = flags | (physical_start + (entry - first_entry) * PAGE_SIZE) as u64;
    }
    if new_table {
        for entry in last_entry..512 {
            table[entry] = PDE64_DEFAULT_FLAGS | (base_address + entry * PAGE_SIZE) as u64;
        }
    }
}

/// Vec contains a list of all copy on write mappings, with their guest virtual, guest physical and size
pub fn set_page_table(
    sregs: &mut kvm_sregs,
    guest_mem: &mut [u8],
    mappings: Vec<(usize, usize, usize)>,
    mut stack_start: usize,
    last_address: usize,
) -> usize {
    // have multiple special regions we care about, everything else should be user accessable and  writeable
    // (everything should is per default marked as executable)
    // for all of the bellow want to mark not usser accessable, some could also be marked not writable
    // but are left as is for simplicty
    // - the page tables
    // - the space with the gdt, idt, handler code
    // - the read only segments
    // all these regions are at the top of the address space, so we can install all mappings, marking everything as default user access and writable
    // remembering the lowest non user addess and then just add no user access from there on out (that basically being the stack start)

    // allocate top level table containing 512 entries for 512 GB ranges
    stack_start -= PAGE_SIZE;
    let p4_address = stack_start;
    // allocate table with 512 entries for 1 GB ranges
    stack_start -= PAGE_SIZE;
    let p3_address = stack_start;

    let p4 = u8_slice_to_u64_slice(&mut guest_mem[p4_address..p4_address + PAGE_SIZE]);
    // everything should be zero, so we only need to set first entry, for a page table with 512 entries with 1GB pages eachp4[0] = PDE64_PRESENT | PDE64_RW | PDE64_USER | p3_address as u64;
    assert!(last_address < (1 << 39));
    p4[0] = PDE64_DEFAULT_FLAGS | p3_address as u64;

    // install default entries for 1 GB pages
    let entries = last_address.next_multiple_of(HUGE_PAGE) / HUGE_PAGE;
    for p3_entry in 0..entries {
        let p3_table = u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
        stack_start -= PAGE_SIZE;
        p3_table[p3_entry] = PDE64_DEFAULT_FLAGS | stack_start as u64;
        let p2_table = u8_slice_to_u64_slice(&mut guest_mem[stack_start..stack_start + PAGE_SIZE]);
        let p3_virtual_start = p3_entry * HUGE_PAGE;
        for p2_entry in 0..512 {
            p2_table[p2_entry] = PDE64_DEFAULT_FLAGS
                | PDE64_IS_PAGE
                | (p3_virtual_start + p2_entry * LARGE_PAGE) as u64;
        }
    }

    // start installing cow entries
    for (guest_virtual, guest_physical, size) in mappings {
        // go through all p2 entries and call the correct setup
        let first_p3 = guest_virtual / HUGE_PAGE;
        let past_last_p3 = ((guest_virtual + size) / HUGE_PAGE) + 1;
        for p3_entry in first_p3..past_last_p3 {
            let p3_base = p3_entry * HUGE_PAGE;
            // adjust guest virtual and physical to the subrange of the p3 entry
            let local_virtual = std::cmp::max(guest_virtual, p3_base);
            let virtual_end = std::cmp::min(guest_virtual + size, p3_base + HUGE_PAGE);
            let local_physical = guest_physical + (local_virtual - guest_virtual);
            let p3_table =
                u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);

            let p2_offset = (p3_table[p3_entry] & !0xFFF) as usize;
            set_p2_table(
                local_virtual,
                virtual_end,
                local_physical,
                PDE64_PRESENT | PDE64_USER,
                p2_offset,
                guest_mem,
                &mut stack_start,
            );
        }
    }

    // protect high address space from user, by removing the mappings
    let first_p3 = stack_start / HUGE_PAGE;
    // all later p3 entries can be protected directly
    for p3_entry in first_p3 + 1..entries {
        let p3_table = u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
        p3_table[p3_entry] = 0;
    }
    let p3_table = u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
    if (p3_table[first_p3] & PDE64_IS_PAGE) != 0 {
        let p2_address = (p3_table[first_p3] & !0xFFF) as usize;
        let p2_table = u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE]);
        let first_p2 = (stack_start / LARGE_PAGE) % 512;
        for p2_entry in first_p2 + 1..512 {
            p2_table[p2_entry] = 0;
        }
        if (p2_table[first_p2] & PDE64_IS_PAGE) != 0 {
            let p1_address = (p2_table[first_p2] & !0xFFF) as usize;
            let p1_table =
                u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE]);
            let first_p1 = (stack_start / PAGE_SIZE) % 512;
            for p1_entry in first_p1..512 {
                p1_table[p1_entry] = 0;
            }
        }
    }

    sregs.cr3 = p4_address as u64;
    return stack_start;
}

/// The mask to get the entry into a table with 512 entries of 8 bytes each
/// Need to zero out everything above 4096 (512 * 8) and the 3 bits at the end indexing into the 8 bytes
const ENTRY_MASK: u64 = 512 - 1;
fn get_entry_from_address(address: u64, shift: usize) -> usize {
    usize::try_from((address >> shift) & ENTRY_MASK).unwrap()
}

pub fn handle_page_fault(vcpu: &VcpuFd, guest_mem: &mut [u8]) -> DandelionResult<usize> {
    let regs = vcpu.get_regs().unwrap();
    let sregs = vcpu.get_sregs().unwrap();
    // the faulting address is in cr2, cr3 holds the root table address, rax holds the error code
    let faulting_address = sregs.cr2;
    let p4_offset = sregs.cr3 as usize;
    let _error_code = regs.rax;

    // get all table entries
    let p4_entry = get_entry_from_address(faulting_address, PML4_SHIFT);
    let p3_entry = get_entry_from_address(faulting_address, HUGE_PAGE_SHIFT);
    let p2_entry = get_entry_from_address(faulting_address, LARGE_PAGE_SHIFT);
    let p1_entry = get_entry_from_address(faulting_address, PAGE_SHIFT);

    let mut get_offset_flags = |offset, entry| {
        let table = u8_slice_to_u64_slice(&mut guest_mem[offset..offset + PAGE_SIZE]);
        let value = table[entry];
        (
            (value as usize & !(PAGE_SIZE - 1)),
            (value & (PAGE_SIZE as u64 - 1)),
        )
    };

    // let p4_table = &guest_mem_u64[p4_offset..p4_offset + TABLE_SIZE];
    let (p3_offset, p3_flags) = get_offset_flags(p4_offset, p4_entry);
    // check the directory table has the correct flags
    // should have all the default flags, as they are applied to all memory in scope
    if p3_flags & !PDE64_ACCESSED != PDE64_DEFAULT_FLAGS {
        trace!(
            "p3 flags not as expected: {}, for offset: {}",
            p3_flags,
            p3_offset
        );
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    // let p3_table = &guest_mem_u64[p3_offset..p3_offset + TABLE_SIZE];
    let (p2_offset, p2_flags) = get_offset_flags(p3_offset, p3_entry);

    if p2_flags & !PDE64_ACCESSED != PDE64_DEFAULT_FLAGS {
        trace!(
            "p2 flags not as expected: {}, for p2 offset: {}",
            p2_flags,
            p2_offset
        );
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    // let p2_table = &guest_mem_u64[p2_offset..p2_offset + TABLE_SIZE];
    let (p1_offset, p1_flags) = get_offset_flags(p2_offset, p2_entry);
    if p1_flags & !PDE64_ACCESSED != PDE64_DEFAULT_FLAGS {
        trace!(
            "p1 flags not as expected: {}, for p1 offset: {}",
            p1_flags,
            p1_offset
        );
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }

    let (old_address, old_flags) = get_offset_flags(p1_offset, p1_entry);
    if old_flags & !PDE64_ACCESSED != PDE64_PRESENT | PDE64_USER {
        trace!(
            "page flags not as expected: {}, for old address {}",
            old_flags,
            old_address
        );
        return Err(DandelionError::UserError(UserError::ManupulatedPageTables));
    }
    let new_address = faulting_address as usize & !(PAGE_SIZE - 1);
    assert!(
        new_address < old_address,
        "The copy on write pages should always be at higher physical addresses"
    );
    let (new_page, old_page) = guest_mem.split_at_mut(old_address);
    new_page[new_address..new_address + PAGE_SIZE].copy_from_slice(&old_page[0..PAGE_SIZE]);

    // let p1_table = &mut guest_mem_u64[p1_offset..p1_offset + TABLE_SIZE];
    let p1_table = u8_slice_to_u64_slice(&mut guest_mem[p1_offset..p1_offset + PAGE_SIZE]);
    p1_table[p1_entry] = new_address as u64 | PDE64_DEFAULT_FLAGS;

    Ok(new_address)
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

        let sregs = vcpu.get_sregs().unwrap();
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

        let events = vcpu.get_vcpu_events().unwrap();
        trace!("events: {:?}\n", events);
    }
}
