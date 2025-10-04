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
/// Page size, for p3 and lower, this indicates the entry address is the mapping, not another table
const PDE64_PS: u64 = 1 << 7;

/// PML4 address (guest virtual)
// const P4_ADDR: usize = 0x1000;
/// First PDP address (guest virtual)
// const P3_ADDR: usize = 0x2000;
// const P2_ADDR: usize = 0x3000;

const PAGE_SIZE: usize = 1 << 12;
pub const LARGE_PAGE: usize = 1 << 21;
const HUGE_PAGE: usize = 1 << 30;

const INTERRUPT_HANDLER: usize = 0x4000;
const GDT: usize = 0x5000;
// const TEST_CS: usize = 0x5800;

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
        stack_pointer = set_page_table(
            &mut sregs,
            guest_mem,
            copy_on_write_pages,
            stack_pointer,
            last_address,
        );
        println!("stack_pointer: {}", stack_pointer);
        set_interrupt_table(&mut sregs, guest_mem);
        vcpu.set_sregs(&sregs).unwrap();
        vcpu.set_regs(&kvm_regs {
            // rip: entry_point,
            // rip: 0x31b90,
            rip: INTERRUPT_HANDLER as u64,
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
pub fn set_interrupt_table(sregs: &mut kvm_sregs, guest_mem: &mut [u8]) {
    // constants given by the manuals or how we set it up
    const GDT_SIZE: u16 = 80;
    const GDT_END: usize = GDT + (GDT_SIZE as usize);
    const TSS_START: usize = GDT_END;
    const TSS_SIZE: u16 = 104;
    const TSS_END: usize = TSS_START + (TSS_SIZE as usize);

    // setup global descriptor table
    // kernel code segment
    setup_segement(&mut guest_mem[GDT + 16..GDT + 32], 0, 10);
    // setup_segement(&mut guest_mem[GDT + 8..GDT + 16], 0, 10);
    // kernel data segment
    setup_segement(&mut guest_mem[GDT + 32..GDT + 48], 0, 2);
    // setup_segement(&mut guest_mem[GDT + 16..GDT + 24], 0, 2);
    // user code segment
    setup_segement(&mut guest_mem[GDT + 48..GDT + 64], 3, 10);
    // setup_segement(&mut guest_mem[GDT + 24..GDT + 32], 3, 10);
    // user data segment
    setup_segement(&mut guest_mem[GDT + 64..GDT + 80], 3, 2);
    // setup_segement(&mut guest_mem[GDT + 32..GDT + 40], 3, 2);
    // // task state segment
    // setup_segement(&mut guest_mem[GDT + 80..GDT + 96], 3, 2);
    // let tss_segment_descriptor = &mut guest_mem[GDT + 80..GDT + 96];
    // //  (&mut guest_mem[GDT + 96..GDT + 112]);
    // // setup limit to TSS size
    // let tss_bytes = (TSS_SIZE - 1).to_le_bytes();
    // tss_segment_descriptor[0..2].copy_from_slice(&tss_bytes);
    // // set address
    // let tss_address = (TSS_START as u64).to_le_bytes();
    // tss_segment_descriptor[2..5].copy_from_slice(&tss_address[0..3]);
    // tss_segment_descriptor[7..12].copy_from_slice(&tss_address[3..8]);
    // set present, descriptor type and type
    // 0 for system, 1 for code or data
    // let descriptor_type = 0;
    // // 0 for read only segement
    // let segment_type = 11;
    // tss_segment_descriptor[5] = (1 << 7) | (descriptor_type << 4) | segment_type;
    // tss_segment_descriptor[5] = (1 << 7);
    // tss_segment_descriptor[5] = (1 << 7) | (0 << 5) | (0 << 4) | 9;
    // set granularity, 4 bits of segment limit
    // tss_segment_descriptor[6] = 0x0;

    sregs.gdt = kvm_bindings::kvm_dtable {
        base: GDT as u64,
        limit: GDT_SIZE - 1,
        ..Default::default()
    };

    // set up task state segement
    sregs.tr = kvm_segment {
        base: TSS_START as u64,
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
    let tss = &mut guest_mem[TSS_START..TSS_END];
    // set the interrupt stack address
    tss[36..44].copy_from_slice(&(0x6000u64).to_le_bytes());

    // let end_address = ((fault_handlers_end as *const c_void).addr()
    //     - (asm_start as *const () as *const c_void).addr()) as u64;
    // guest_mem[0x6008..0x6010].copy_from_slice(&end_address.to_le_bytes());

    // setup interup handler table

    // let handler_length = (page_fault_handler_end as *const c_void).addr()
    // - (page_fault_handler as *const () as *const c_void).addr();
    let handler_length = (fault_handlers_end as *const c_void).addr()
        - (asm_start as *const () as *const c_void).addr();
    // println!(
    //     "start:\t{:#x?}\nend:\t{:#x?}\nlength: {}",
    //     page_fault_exception_handler as *const (), fault_handlers_end as *const (), handler_length
    // );
    let destination = unsafe {
        slice::from_raw_parts_mut(
            guest_mem.as_mut_ptr().add(INTERRUPT_HANDLER),
            handler_length,
        )
    };
    // let source = unsafe { slice::from_raw_parts(page_fault_handler as *const u8, handler_length) };
    let source = unsafe { slice::from_raw_parts(asm_start as *const u8, handler_length) };
    destination.copy_from_slice(source);

    // let page_handler_address = (INTERRUPT_HANDLER
    // + (page_fault_exception_handler as *const c_void).addr()
    // - (asm_start as *const () as *const c_void).addr()) as u64;

    // set the general protection handler (also fall back for when others are not initialized)
    // two consequitive u64 are the descriptor for one entry
    // need to set the segment selector to 1 for the segment we have set up in the GDT
    // the segmenet selector uses bits 0 and 1 for the requested priviledge level (0 for us)
    // then bit 2 to the table indicator (0 for GDT, 1 for LDT)
    // let idt_general_protection = &mut guest_mem[GDT + 13 * 16..GDT + 14 * 16];

    // use selector 2, as in 64 bit mode, all the entries in the GDT use two entries (because entry size is determined by 32 bit mode)
    let segment_selector: u16 = 2 << 3 | 0;

    // let segment_selector: u16 = 1;
    // idt[26] = segment_selector << 16;
    // // set the lowest 16 bit of the interrupt handler offset in the segment in the lowest 16 bit of the gate
    // idt[26] = handler_address & 0xFFFF;
    // // set the bits 16 to 31 in the bits 48 to 63
    // idt[26] = (handler_address & 0xFFFF_0000) << 32;
    // // set the type to 64-bit interrupt gate
    // idt[26] = 14 << 40;
    // // set the upper 32 bits of the interrupt handler offset in the segment to the bits 64 to 96 in the entry
    // idt[27] = handler_address >> 32;

    // let idt_page_fault = &mut guest_mem[GDT + 14 * 16..GDT + 15 * 16];
    // // set the page fault handler
    // idt_page_fault[2] = segment_selector.to_le_bytes()[0];
    // idt_page_fault[3] = segment_selector.to_le_bytes()[1];
    // // set the lowest 16 bit of the interrupt handler offset in the segment in the lowest 16 bit of the gate
    // idt_page_fault[0] = handler_address.to_le_bytes()[0];
    // idt_page_fault[1] = handler_address.to_le_bytes()[1];
    // // set the bits 16 to 31 in the bits 48 to 63
    // idt_page_fault[6] = handler_address.to_le_bytes()[2];
    // idt_page_fault[7] = handler_address.to_le_bytes()[3];
    // // set the upper 32 bits of the interrupt handler offset in the segment to the bits 64 to 96 in the entry
    // idt_page_fault[8..12].copy_from_slice(&handler_address.to_le_bytes()[4..8]);
    // // set the type to 64-bit interrupt gate
    // idt_page_fault[5] = 14;

    // let test_location = &mut guest_mem[TEST_CS..];
    // test_location[0..8].copy_from_slice(&handler_address.to_le_bytes());
    // test_location[0..8].copy_from_slice(&(INTERRUPT_HANDLER as u64).to_le_bytes());
    // test_location[8] = segment_selector.to_be_bytes()[0];
    // test_location[9] = segment_selector.to_le_bytes()[1];

    // let general_handler_address =
    //     (INTERRUPT_HANDLER + (general_fault_handler as *const c_void).addr()
    //         - (asm_entry as *const () as *const c_void).addr()) as u64;

    let idt_base = TSS_END;
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
        let handler_offset = (INTERRUPT_HANDLER + handler_address
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

/// Assumes virtual and physical are page alligned, size is multiple of page size
fn set_p2_table(
    mut virtual_start: usize,
    mut physical: usize,
    mut size: usize,
    flags: u64,
    p2_offset: usize,
    memory: &mut [u8],
    stack_start: &mut usize,
) {
    // check if there is any header before the first large page
    let first_large_page = virtual_start.next_multiple_of(LARGE_PAGE);
    if virtual_start < first_large_page {
        let p2_table = u8_slice_to_u64_slice(&mut memory[p2_offset..p2_offset + PAGE_SIZE]);
        let sub_size = std::cmp::min(first_large_page - virtual_start, size);
        let p2_entry = (virtual_start >> 21) & 0x1FF;
        // check if there is already a table for that entry or if it was directly mapped so far
        let (p1_offset, new_table) = if p2_table[p2_entry] & PDE64_PS != 0 {
            *stack_start -= PAGE_SIZE;
            p2_table[p2_entry] = (*stack_start as u64) | PDE64_PRESENT | PDE64_RW | PDE64_USER;
            (*stack_start, true)
        } else {
            ((p2_table[p2_entry] & !0xFFF) as usize, false)
        };
        let p1_table = u8_slice_to_u64_slice(&mut memory[p1_offset..p1_offset + PAGE_SIZE]);
        set_p1_table(
            virtual_start,
            physical,
            sub_size,
            flags,
            p1_table,
            new_table,
        );
        size -= sub_size;
        if size == 0 {
            return;
        }
        physical += sub_size;
        virtual_start += sub_size;
    }

    let p2_table = u8_slice_to_u64_slice(&mut memory[p2_offset..p2_offset + PAGE_SIZE]);

    // virtual start is now set to large page boundry, iterate over and set all pages until last full 2MB page
    // need to add 1, in case size is exactly large page size
    let first_large_entry = first_large_page / LARGE_PAGE;
    // want to set it to the large page past the last one that is fully covered by size
    let large_pages = ((size + 1).next_multiple_of(LARGE_PAGE) / LARGE_PAGE).saturating_sub(1);
    let last_large_entry = first_large_entry + large_pages;
    for entry in first_large_entry..last_large_entry {
        println!("mapping {} to {}", physical, entry * LARGE_PAGE);
        p2_table[entry] = flags | physical as u64;
        physical += LARGE_PAGE;
    }
    size -= LARGE_PAGE * large_pages;
    virtual_start += LARGE_PAGE * large_pages;
    // set all pages after the large pages
    if size > 0 {
        let (p1_offset, new_table) = if p2_table[last_large_entry] & PDE64_PS != 0 {
            *stack_start -= PAGE_SIZE;
            p2_table[last_large_entry] =
                (*stack_start as u64) | PDE64_PRESENT | PDE64_RW | PDE64_USER;
            (*stack_start, true)
        } else {
            ((p2_table[last_large_entry] & !0xFFF) as usize, false)
        };
        let p1_table = u8_slice_to_u64_slice(&mut memory[p1_offset..p1_offset + PAGE_SIZE]);
        set_p1_table(virtual_start, physical, size, flags, p1_table, new_table);
    }
}

/// Assumes virtual and physical are page alligned, size is a mutliple of page size.
fn set_p1_table(
    virtual_start: usize,
    physical: usize,
    size: usize,
    flags: u64,
    table: &mut [u64],
    new_table: bool,
) {
    println!(
        "set p1 table ({:?}) for virtual start: {}, physical start: {}",
        table.as_ptr(),
        virtual_start,
        physical
    );
    let first_entry = (virtual_start >> 12) & 0x1FF;
    // base address of the 2 MB this page table is responsible for
    let virtual_table_base = virtual_start & !0xF_FFFF;
    let entry_number = size / PAGE_SIZE;
    if new_table {
        for entry in 0..first_entry {
            println!(
                "installing default mapping at {}, for entry {}",
                virtual_table_base + entry * PAGE_SIZE,
                entry
            );
            table[entry] = PDE64_PRESENT
                | PDE64_PS
                | PDE64_RW
                | PDE64_USER
                | (virtual_table_base + entry * PAGE_SIZE) as u64;
        }
    }
    for entry in first_entry..first_entry + entry_number {
        println!(
            "mapping {} to {}, for entry {}",
            virtual_table_base + entry * PAGE_SIZE,
            physical + (entry - first_entry) * PAGE_SIZE,
            entry
        );
        table[entry] = flags | (physical + (entry - first_entry) * PAGE_SIZE) as u64;
    }
    if new_table {
        for entry in first_entry + entry_number..512 {
            println!(
                "installing default mapping at {}, for entry {}",
                virtual_table_base + entry * PAGE_SIZE,
                entry
            );
            table[entry] = PDE64_PRESENT
                | PDE64_PS
                | PDE64_RW
                | PDE64_USER
                | (virtual_table_base + entry * PAGE_SIZE) as u64;
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
    println!(
        "guest_mem: {:?}, stack start: {}",
        guest_mem.as_mut_ptr(),
        stack_start
    );
    println!("value at offset {}: {}", 0x31b90, guest_mem[0x7C31b90]);
    guest_mem[0x31b90] = 12;
    let p4_address = stack_start;
    // allocate table with 512 entries for 1 GB ranges
    let p3_address = stack_start;
    stack_start -= PAGE_SIZE;

    let p4 = u8_slice_to_u64_slice(&mut guest_mem[p4_address..p4_address + PAGE_SIZE]);
    // everything should be zero, so we only need to set first entry, for a page table with 512 entries with 1GB pages eachp4[0] = PDE64_PRESENT | PDE64_RW | PDE64_USER | p3_address as u64;
    assert!(last_address < (1 << 39));
    p4[0] = PDE64_PRESENT | PDE64_RW | PDE64_USER | p3_address as u64;

    // install default entries for 1 GB pages
    let entries = last_address.next_multiple_of(HUGE_PAGE) / HUGE_PAGE;
    for p3_entry in 0..entries {
        let p3_table = u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
        stack_start -= PAGE_SIZE;
        p3_table[p3_entry] = PDE64_PRESENT | PDE64_RW | PDE64_USER | stack_start as u64;
        let p2_table = u8_slice_to_u64_slice(&mut guest_mem[stack_start..stack_start + PAGE_SIZE]);
        let p3_virtual_start = p3_entry * HUGE_PAGE;
        for p2_entry in 0..512 {
            p2_table[p2_entry] = PDE64_PRESENT
                | PDE64_RW
                | PDE64_USER
                | PDE64_PS
                | (p3_virtual_start + p2_entry * LARGE_PAGE) as u64;
        }
    }

    // start installing cow entries
    for (guest_virtual, guest_physical, size) in mappings {
        // go through all p2 entries and call the correct setup
        let first_p3 = ((guest_virtual + 1).next_multiple_of(HUGE_PAGE) / HUGE_PAGE) - 1;
        let past_last_p3 = (guest_virtual + size).next_multiple_of(HUGE_PAGE) / HUGE_PAGE;
        for p3_entry in first_p3..past_last_p3 {
            let p3_base = p3_entry * HUGE_PAGE;
            // adjust guest virtual and physical to the subrange of the p3 entry
            let local_virtual = std::cmp::max(guest_virtual, p3_base);
            let virtual_end = std::cmp::min(guest_virtual + size, p3_base + HUGE_PAGE);
            let local_size = virtual_end - local_virtual;
            let local_physical = guest_physical + (local_virtual - guest_virtual);
            let p3_table =
                u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE]);
            println!(
                "mapping virtual: {}, to physical: {} with size: {}, at p3 entry: {:x}",
                guest_virtual, guest_physical, size, p3_table[p3_entry]
            );
            let p2_offset = (p3_table[p3_entry] & !0xFFF) as usize;
            set_p2_table(
                local_virtual,
                local_physical,
                local_size,
                PDE64_PRESENT | PDE64_RW | PDE64_PS,
                p2_offset,
                guest_mem,
                &mut stack_start,
            );
        }
    }
    // protect high address space from user

    // // setup default mappings for guest mem
    // // TODO: store the page table in another guest memory slot (outside the context)
    // for i in 0..mem_size_gb {
    //     p3[i] = PDE64_PRESENT | PDE64_RW | PDE64_USER | PDE64_PS | (i as u64) << 30;
    // }
    // p3[0] = PDE64_PRESENT | PDE64_RW | PDE64_USER | P2_ADDR as u64;
    // let p2 = u8_slice_to_u64_slice(&mut guest_mem[P2_ADDR..P2_ADDR + 0x1000]);
    // for i in 0..512 {
    //     if i < 8 {
    //         p2[i] = PDE64_PRESENT | PDE64_RW | PDE64_USER | PDE64_PS | (i as u64) << 21;
    //     } else {
    //         p2[i] = PDE64_PRESENT | PDE64_USER | PDE64_PS | (i as u64) << 21;
    //     }
    // }

    println!("stack_start at end of table setup: {}", stack_start);
    sregs.cr3 = p4_address as u64;
    return stack_start;
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

pub fn dump_regs(vcpu: &VcpuFd) {
    let regs = vcpu.get_regs().unwrap();
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
