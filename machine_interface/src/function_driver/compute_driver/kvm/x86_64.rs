use crate::{
    function_driver::compute_driver::kvm::{
        GLOBAL_SYSTEM_MEMORY_SIZE, PER_VM_SYSTEM_MEMORY, SYSTEM_MEMORY_SIZE, SYSTEM_VIRTUAL_BASE,
    },
    memory_domain::{
        kvm::{KvmContext, Page, PageType},
        ContextTrait,
    },
};
use dandelion_commons::{err_dandelion, DandelionError, DandelionResult};
use kvm_bindings::{kvm_fpu, kvm_msr_entry, kvm_regs, kvm_segment, kvm_sregs, kvm_xcrs, Msrs};
use kvm_ioctls::{VcpuFd, VmFd};
use log::{debug, info, trace};
use std::{collections::BTreeMap, num::NonZeroUsize, os::raw::c_void, slice};

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
/// Enable process context identifiers
const CR4_PCIDE: u64 = 1 << 17;
/// Enable global page mappings (valid for all process contexts)
const CR4_PGE: u64 = 1 << 7;
/// OS support for fxsave and fxrstor instructions
const CR4_OSFXSR: u64 = 1 << 9;
/// OS Support for unmasked simd floating point exceptions
const CR4_OSXMMEXCPT: u64 = 1 << 10;
/// Enables the instructions RDFSBASE, RDGSBASE, WRFSBASE, and WRGSBASE
const CR4_FSGSBASE: u64 = 1 << 16;
/// Enables xsafe, required for vector instructions
const CR4_OSXSAVE: u64 = 1 << 18;

// XCR0 bits
/// X87 must be 1
const XCR0_X87: u64 = 1 << 0;
/// SSE, set to 1 to enable XMM registers and sse instructions
const XCR0_SSE: u64 = 1 << 1;
/// AVX, set to 1 to enable ymm registers and avx instructions
const XCR0_AVX: u64 = 1 << 2;
/// AVX-512, collection of bits to set to use avx-512 registers and instructions
const XCR0_AVX512: u64 = 1 << 5 | 1 << 6 | 1 << 7;

// EFER bits
/// Long Mode Enable
const EFER_LME: u64 = 1 << 8;
/// Long Mode Active
const EFER_LMA: u64 = 1 << 10;

// FPU control word bits
const FPU_EXCEPTION_MASK_PRECISION: u16 = 1 << 5;
const FPU_PRECISION_CONTROL_FILED: u16 = 3 << 8;
// MMX control bits
const MMX_EXCEPTION_MASK_PRECISION: u32 = 1 << 12;

// 64-bit page directory entry bits
/// Present
pub(super) const PDE64_PRESENT: u64 = 1 << 0;
/// Writable
const PDE64_RW: u64 = 1 << 1;
/// User accessible
pub(super) const PDE64_USER: u64 = 1 << 2;
/// Default flags for user accessable pages
pub(super) const PDE64_ALL_ALLOWED: u64 = PDE64_PRESENT | PDE64_RW | PDE64_USER;
/// Set by hardware if the entry has been used to translate a linear address
#[cfg(feature = "backend_debug")]
const PDE64_ACCESSED: u64 = 1 << 5;
/// Set by hardware if entry has been use to write to linaer address
#[cfg(feature = "backend_debug")]
const PDE64_DIRTY: u64 = 1 << 6;
/// Page size, for p3 and lower, this indicates the entry address is the mapping, not another table
/// In the docs this is called the PDE64_PS
pub(super) const PDE64_IS_PAGE: u64 = 1 << 7;
/// Page is global, meaning all process ids are allowed to use the mapping for this page
const PDE64_GLOBAL: u64 = 1 << 8;

// 4 level paging:
// each linear address consists of the following
// 47 .. 39  | 38 .. 30        | 29 .. 21  | 20 .. 12  | 11 .. 0
// PML4      | Directory PTR   | Directory | Table     | Offset
// with the PML4 being the offset into the page table pointed to by the root pointer in CR3
// and then the others being indexes into the tables recursively
// The direcotry pointer table entries are either 1GB pages or directory table pointers
// Directory table entries are either 2MB pages or tables
// Table entries are always 4KB pages
pub(super) const PAGE_SHIFT: usize = 12;
pub const PAGE_SIZE: usize = 1 << PAGE_SHIFT;
pub(super) const PAGE_MASK: u64 = (PAGE_SIZE - 1) as u64;
const LARGE_PAGE_SHIFT: usize = 21;
pub const LARGE_PAGE: usize = 1 << LARGE_PAGE_SHIFT;
pub(super) const HUGE_PAGE_SHIFT: usize = 30;
pub(super) const HUGE_PAGE: usize = 1 << HUGE_PAGE_SHIFT;
pub(super) const PML4_SHIFT: usize = 39;
const TABLE_SIZE: usize = 512;
/// When process context IDs are enabled, the bits 11:0 in CR3 are used to identify the PCID.
/// We use the max ID for the user inside the VM and max ID - 1 for the root user.
pub(super) const USER_PCID: u64 = (1 << 12) - 1;
pub(super) const ROOT_PCID: u64 = (1 << 12) - 2;

fn u8_slice_to_u64_slice(input: &mut [u8]) -> &mut [u64] {
    assert!(
        input.len().is_multiple_of(8),
        "Input slice length must be a multiple of 8, but has length: {}",
        input.len()
    );
    assert!(
        (input.as_ptr() as usize).is_multiple_of(8),
        "Input slice must be 8-byte aligned, but got {:?}",
        input.as_ptr()
    );
    let u64_len = input.len() / 8;
    unsafe { std::slice::from_raw_parts_mut(input.as_mut_ptr() as *mut u64, u64_len) }
}

pub struct ResetState {
    sregs: kvm_sregs,
    xregs: kvm_xcrs,
    pub task_stack: &'static mut [u8],
}

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
        use nix::sys::mman::{mmap_anonymous, MapFlags, ProtFlags};

        let mut sregs = vcpu.get_sregs().unwrap();
        let mut xregs = vcpu.get_xcrs().unwrap();
        setup_long_mode(&mut sregs, &mut xregs);
        let stack_pointer = unsafe {
            std::ptr::from_mut(
                mmap_anonymous(
                    None,
                    NonZeroUsize::new(PER_VM_SYSTEM_MEMORY).unwrap(),
                    ProtFlags::all(),
                    MapFlags::MAP_SHARED,
                )
                .unwrap()
                .as_mut(),
            )
        } as *mut u8;
        let task_stack = unsafe { slice::from_raw_parts_mut(stack_pointer, PER_VM_SYSTEM_MEMORY) };

        Self {
            sregs,
            xregs,
            task_stack,
        }
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
        kvm_context: &mut Box<KvmContext>,
        mut stack_pointer: usize,
    ) -> DandelionResult<PageFaultMetadata> {
        let mut sregs = self.sregs;
        let xregs = self.xregs;
        let interrupt_end = stack_pointer;
        let code_start = (entry_point as usize).next_multiple_of(PAGE_SIZE) - PAGE_SIZE;
        info!("Entry point: {}, code start {}", entry_point, code_start);
        set_interrupt_table(&mut sregs);
        let interrupt_start = stack_pointer;
        let page_fault_metadata;
        set_debug_asm(kvm_context, code_start);
        (stack_pointer, page_fault_metadata) = set_page_table(
            &mut sregs,
            kvm_context,
            stack_pointer,
            (interrupt_start, interrupt_end),
        )?;

        vcpu.set_sregs(&sregs).unwrap();
        vcpu.set_xcrs(&xregs).unwrap();
        vcpu.set_regs(&kvm_regs {
            // rip: entry_point,
            rip: code_start as u64,
            rsp: stack_pointer as u64 - 32,
            rbp: stack_pointer as u64 - 32,
            rflags: 2,
            ..Default::default()
        })
        .unwrap();
        vcpu.set_fpu(&kvm_fpu {
            fcw: FPU_PRECISION_CONTROL_FILED | FPU_EXCEPTION_MASK_PRECISION,
            mxcsr: MMX_EXCEPTION_MASK_PRECISION,
            ..Default::default()
        })
        .unwrap();

        // reset the cpu time step counter register
        let msrs = Msrs::from_entries(&[
            // set the MSR_IA32_TSC
            kvm_msr_entry {
                index: 0x10,
                data: 0,
                ..Default::default()
            },
        ])
        .unwrap();
        assert_eq!(1, vcpu.set_msrs(&msrs).unwrap());
        let msrs = Msrs::from_entries(&[kvm_msr_entry {
            index: 0x3b,
            data: 0,
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(1, vcpu.set_msrs(&msrs).unwrap());

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

// constants given by the manuals or how we set it up
const GDT_START: usize = 0;
const GDT_SIZE: u16 = 80;
const IDT_START: usize = GDT_START + (GDT_SIZE as usize);
const IDT_SIZE: usize = 33 * 16;
const IDT_END: usize = IDT_START + IDT_SIZE;
const TSS_START: usize = IDT_START + IDT_SIZE;
const TSS_SIZE: usize = 104;
const HANDLER_CODE: usize = (TSS_START + TSS_SIZE as usize).next_multiple_of(PAGE_SIZE);
fn handler_code_size() -> usize {
    unsafe {
        (fault_handlers_end as *const c_void)
            .byte_offset_from_unsigned(default_handler as *const () as *const c_void)
    }
}

pub(super) fn prepare_static_memory(memory: &mut [u8]) {
    write_gdt_and_idt(memory);
    write_global_page_table_entries(memory);
    write_direct_mappig(memory);
}

/// The GDT and IDT as well as the handler code can be used by all VMs, set this up once for global use.
fn write_gdt_and_idt(memory: &mut [u8]) {
    // The GDT starts at the beginning of the memory slice, which is assumed to start at SYSTEM_VIRTUAL_BASE
    // After the GDT we write the IDT
    // Write handlers at the start of the next page
    let handler_length = handler_code_size();

    // setup global descriptor table
    // kernel code segment
    let gdt = &mut memory[GDT_START..IDT_START];
    setup_segement(&mut gdt[16..32], 0, 10);
    // kernel data segment
    setup_segement(&mut gdt[32..48], 0, 2);
    // user code segment
    setup_segement(&mut gdt[48..64], 3, 10);
    // user data segment
    setup_segement(&mut gdt[64..80], 3, 2);

    // Set up IDT
    // use selector 2, as in 64 bit mode, all the entries in the GDT use two entries (because entry size is determined by 32 bit mode)
    // Selectors consist of 2 LSB for priviledge level, 1 bit for using GDT (0) or LDT (1) and then the segement index shifted by 3.
    let segment_selector: u16 = 2 << 3;

    let idt = &mut memory[IDT_START..IDT_END];
    for i in 0..33 {
        let handler_offset = unsafe {
            (match i {
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
                // There is no interrupt 15
                16 => floating_point_error_handler,
                17 => alignment_check_exception_handler,
                18 => machine_check_exception_handler,
                19 => simd_fp_exception_handler,
                20 => virtualization_exception_handler,
                21 => control_protection_exception,
                32 => user_exit_handler,
                _ => default_handler,
            } as *const u8)
                .offset_from_unsigned(default_handler as *const u8)
        };
        let handler_address = (SYSTEM_VIRTUAL_BASE + HANDLER_CODE + handler_offset) as u64;

        setup_interrupt_gate(
            &mut idt[i * 16..(i + 1) * 16],
            segment_selector,
            handler_address,
        );
    }

    // Set the revevant parts of the TSS, i.e. the stack address.
    // Since it is a stack, neeeds to be set to top end of page.
    let tss = &mut memory[TSS_START..TSS_START + TSS_SIZE];
    tss[36..44].copy_from_slice(
        &(SYSTEM_VIRTUAL_BASE + SYSTEM_MEMORY_SIZE + PER_VM_SYSTEM_MEMORY).to_le_bytes(),
    );

    // setup interupt handler code
    let source = unsafe { slice::from_raw_parts(default_handler as *const u8, handler_length) };
    memory[HANDLER_CODE..HANDLER_CODE + handler_length].copy_from_slice(source);
}

/// Pages with global state need to be added to all guest page tables.
/// This means we can set up a part of a page table that can be reused.
/// Set up a direct mapping of the static memory.
fn write_global_page_table_entries(memory: &mut [u8]) {
    // start on page after handler code has finished
    let table_start = (HANDLER_CODE + handler_code_size()).next_multiple_of(PAGE_SIZE);
    assert!(table_start + 2 * PAGE_SIZE < memory.len());
    // need to set up a 1 GiB Table with a single 2 MB table at the start
    let table_entry = u8_slice_to_u64_slice(&mut memory[table_start..table_start + 8]);
    // Needs to be writable, since the segments are marked accessed by the CPU when they are loaded.
    table_entry[0] = PDE64_RW
        | PDE64_GLOBAL
        | PDE64_PRESENT
        | (SYSTEM_VIRTUAL_BASE + table_start + PAGE_SIZE) as u64;
    let page_table =
        u8_slice_to_u64_slice(&mut memory[table_start + PAGE_SIZE..table_start + 2 * PAGE_SIZE]);
    // install mappings for all pages bellow this page table
    for (page_index, entry) in page_table.iter_mut().enumerate() {
        let address = page_index * PAGE_SIZE;
        if address < table_start
            || (SYSTEM_MEMORY_SIZE <= address
                && address < SYSTEM_MEMORY_SIZE + PER_VM_SYSTEM_MEMORY)
        {
            *entry =
                PDE64_RW | PDE64_GLOBAL | PDE64_PRESENT | (SYSTEM_VIRTUAL_BASE + address) as u64;
        }
    }
}

/// Construct a page table in global accessable memory containing a direct mapping for
/// the full available address space. This is for use in interrupt handlers,
/// to have a consistent view of the address space.
fn write_direct_mappig(mem_location: &mut [u8]) {
    // Have the P4 page table at the start of the range, with only the first entry non null, pointing to the P3 table.
    // The P3 Table, follows the P4 table, overseeing the first 512 GiB of memory.
    // Set all direct mappings in that table, each representing a 1GiB hugepage.

    // P4 page table, covering the entire 256 TiB range.
    let root_p4_table = u8_slice_to_u64_slice(
        &mut mem_location[GLOBAL_SYSTEM_MEMORY_SIZE..GLOBAL_SYSTEM_MEMORY_SIZE + PAGE_SIZE],
    );
    root_p4_table[0] = PDE64_PRESENT
        | PDE64_RW
        | (SYSTEM_VIRTUAL_BASE + GLOBAL_SYSTEM_MEMORY_SIZE + PAGE_SIZE) as u64;

    // P3 page table covering the first 512 GiB range
    let root_p3_table = u8_slice_to_u64_slice(
        &mut mem_location
            [GLOBAL_SYSTEM_MEMORY_SIZE + PAGE_SIZE..GLOBAL_SYSTEM_MEMORY_SIZE + PAGE_SIZE * 2],
    );
    for (page_index, entry) in root_p3_table.iter_mut().enumerate() {
        *entry = PDE64_PRESENT
            | PDE64_RW
            | PDE64_IS_PAGE
            | (page_index * TABLE_SIZE * TABLE_SIZE * PAGE_SIZE) as u64;
    }
}

/// Intel Interrupt Descriptor Table (IDT) in 64-bit mode:
/// - Entries are 16-byte descriptors (in protected mode)
/// - Base address should be alligned to 8-byte boundry (for cache alignment)
/// - First 32 entries are reserved for intel interrupts rest (32-255) are user defined
///   (so we don't need to hanlde them if we don't plan to use them)
/// - Entry 13 is for handling general protection faults (the default fallback fault)
/// - Entry 14 is for handling page faults
/// - Not all entries need to be filled, for empty slots should have the present flag in the descriptor set to 0
/// - Address of the IDT is held in the IDTR which holds both a 32-bit base address as well as a 16-bit limit,
///   the limit should always be one less than an integral multiple of eight (adding it to the base address,
///   should give the address of the last valid byte in the IDT so, it should be 8N -1, with N entries)
fn set_interrupt_table(sregs: &mut kvm_sregs) {
    // Use GDT that is mapped into all VMs
    sregs.gdt = kvm_bindings::kvm_dtable {
        // base: gdt as u64,
        base: (SYSTEM_VIRTUAL_BASE + GDT_START) as u64,
        limit: GDT_SIZE - 1,
        ..Default::default()
    };

    // Use IDT and interrup handler routines mapped into all VMs
    sregs.idt = kvm_bindings::kvm_dtable {
        base: (SYSTEM_VIRTUAL_BASE + IDT_START) as u64,
        limit: IDT_SIZE as u16 - 1,
        ..Default::default()
    };

    // set up task state segement
    sregs.tr = kvm_segment {
        base: (SYSTEM_VIRTUAL_BASE + TSS_START) as u64,
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
}

pub fn set_debug_asm(kvm_context: &mut Box<KvmContext>, code_start: usize) {
    let code_size = (default_handler as *const ()).addr() - (asm_start as *const ()).addr();
    let source = unsafe { slice::from_raw_parts(asm_start as *const u8, code_size) };
    debug!("ASM SOURCE: {:?}", source.to_vec());
    kvm_context.write(code_start, source);
}

pub struct PageFaultMetadata {
    /// Address of the singular p4 table, only first entry set
    // #[cfg(feature = "backend_debug")]
    // p4_address: usize,
    /// Address of the singular p3 table, only first entry set
    // #[cfg(feature = "backend_debug")]
    // p3_address: usize,
    /// Base address for the tables with p2 and p1 tables
    // #[cfg(feature = "backend_debug")]
    // table_base: usize,
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
        && !current_page_entry.is_multiple_of(TABLE_SIZE)
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
    if !current_page_entry.is_multiple_of(TABLE_SIZE) {
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
    kvm_context: &mut Box<KvmContext>,
    stack_start: usize,
    interrupt_range: (usize, usize),
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

    // Ensure the context size does not violate basic size assumption.
    assert!(kvm_context.context_size < SYSTEM_VIRTUAL_BASE);
    assert_eq!(0, stack_start % PAGE_SIZE);

    let p2_table_number = kvm_context
        .context_size
        .next_multiple_of(HUGE_PAGE)
        .div_ceil(HUGE_PAGE);
    trace!(
        "p2_table_number: {}, context_size: {}",
        p2_table_number,
        kvm_context.context_size
    );
    trace!(
        "value at 65536: {}",
        u8_slice_to_u64_slice(kvm_context.domain_slice)[65536 / 8]
    );

    // always only need 1 p4 table, since it is top level
    // get physical page
    let p4_physical = kvm_context.get_page()?;
    let p3_physical = kvm_context.get_page()?;
    // set up the p4 table, which only has 1 entry at the start, the single p3 table
    trace!("P4 address: {}, P3 address: {}", *p4_physical, *p3_physical);
    trace!(
        "value at 65536: {}",
        u8_slice_to_u64_slice(kvm_context.domain_slice)[65536 / 8]
    );
    let p4_table = p4_physical.get_slice(kvm_context.domain_slice);
    p4_table[0] = PDE64_ALL_ALLOWED | *p3_physical;
    p4_table[1..].fill(0);

    // always only allocate a single p3 table, since we do not expect functions to have more than 512GB of memory
    let p3_table = p3_physical.get_slice(kvm_context.domain_slice);
    p3_table[p2_table_number..].fill(0);

    trace!(
        "after zeroing p3 table base physical: {}",
        u8_slice_to_u64_slice(kvm_context.domain_slice)[0],
    );
    trace!(
        "value at 65536: {}",
        u8_slice_to_u64_slice(kvm_context.domain_slice)[65536 / 8]
    );
    // set up the p3 table, which points at all the p2 tables
    for p3_entry in 0..p2_table_number {
        // allocate a new p2 table
        let p2_physical = kvm_context.get_page()?;
        println!("new p2 physical: {}", *p2_physical);
        let p2_table = p2_physical.get_slice(kvm_context.domain_slice);
        p2_table.fill(0);
        p3_physical.get_slice(kvm_context.domain_slice)[p3_entry] =
            PDE64_ALL_ALLOWED | *p2_physical;
    }
    // can assume that all p2 tables are zero initialized after this loop

    trace!(
        "after setting p3 entries base physical: {}",
        u8_slice_to_u64_slice(kvm_context.domain_slice)[0],
    );

    // add the table with the global data in the last entry
    // find index based on the GiB at which SYSTEM_VIRTUAL_BASE is located at
    let table_entry = SYSTEM_VIRTUAL_BASE / (1 << 30);
    p3_physical.get_slice(kvm_context.domain_slice)[table_entry] = PDE64_ALL_ALLOWED
        | PDE64_GLOBAL
        | (SYSTEM_VIRTUAL_BASE + HANDLER_CODE + handler_code_size().next_multiple_of(PAGE_SIZE))
            as u64;

    // add pages for the stack, round down to start of next large page
    let stack_page_start = (stack_start - 1) & !(LARGE_PAGE - 1);
    trace!(
        "start filling in stack from {} to {}",
        stack_page_start,
        stack_start
    );
    assert!(stack_page_start > 0);
    for page in (stack_page_start..stack_start).step_by(PAGE_SIZE) {
        let (_, page_slice) = kvm_context.add_page(page)?;
        page_slice.fill(0);
    }
    trace!("filled in stack");

    // go through overlay and add all pages in the p2 tables
    for (virtual_address, (page_type, physical_address)) in kvm_context.overlay.iter() {
        // flags to set for the page
        let flags = match page_type {
            PageType::Page => PDE64_ALL_ALLOWED,
            PageType::CoW => PDE64_PRESENT | PDE64_USER,
        };

        // To get the table indices we need to shift by the page size,
        let p3_index = virtual_address >> HUGE_PAGE_SHIFT & (TABLE_SIZE - 1);
        let p2_index = virtual_address >> LARGE_PAGE_SHIFT & (TABLE_SIZE - 1);
        let p1_index = virtual_address >> PAGE_SHIFT & (TABLE_SIZE - 1);

        let p2_base = (p3_physical.get_slice(kvm_context.domain_slice)[p3_index] as usize
            & !(PAGE_SIZE - 1))
            / size_of::<u64>();
        // read p2 entry
        let p2_entry = u8_slice_to_u64_slice(kvm_context.domain_slice)[p2_base + p2_index];
        // check if p2 entry points to valid p1 table, if not insert table
        if p2_entry & PDE64_PRESENT == 0 {
            trace!(
                "adding p2 entry for address: {}, p3 index: {}, p2 index: {}, p1 index: {}",
                virtual_address,
                p3_index,
                p2_index,
                p1_index
            );
            let new_p1 = kvm_context.get_page()?;
            u8_slice_to_u64_slice(kvm_context.domain_slice)[p2_base + p2_index] =
                PDE64_ALL_ALLOWED | *new_p1;
            new_p1.get_slice(kvm_context.domain_slice).fill(0);
            new_p1.get_slice(kvm_context.domain_slice)[p1_index] = flags | **physical_address;
        } else {
            let p1_base = p2_entry as usize & !(PAGE_SIZE - 1) / size_of::<u64>();
            debug_assert_eq!(
                0,
                u8_slice_to_u64_slice(kvm_context.domain_slice)[p1_base + p1_index]
            );
            u8_slice_to_u64_slice(kvm_context.domain_slice)[p1_base + p1_index] =
                flags | **physical_address;
        }
    }

    // check how many p2 and p1 tables we need and allocate them
    // let p1_table_number = p2_table_number * TABLE_SIZE;
    // stack_start -= (p2_table_number + p1_table_number) << PAGE_SHIFT;
    // let table_base = stack_start;
    // have a joint table for p2 and p1 tables with 1 p2 table followed by the 512 p1 tables it could point to
    // makes it easier to find the p1 table in the asm page fault handler if we need to install it,
    // since we can just offset from the p2, which is always present

    // set up all the tables for easy access
    // let (guest_mem, p4_raw) = guest_mem.split_at_mut(mapping_offset + p4_address);
    // let p4_table = u8_slice_to_u64_slice(&mut p4_raw[0..PAGE_SIZE]);
    // p4_table.fill(0);
    // let (guest_mem, p3_raw) = guest_mem.split_at_mut(mapping_offset + p3_address);
    // let p3_table = u8_slice_to_u64_slice(&mut p3_raw[0..PAGE_SIZE]);
    // let (guest_mem, table_raw) = guest_mem.split_at_mut(mapping_offset + table_base);
    // let table_array =
    // u8_slice_to_u64_slice(&mut table_raw[0..(p2_table_number + p1_table_number) << PAGE_SHIFT]);

    // point the p3 table to all p2 tables
    // {
    //     let mut p2_address = mapping_offset + table_base;
    //     for (p3_index, p3_entry) in p3_table[0..p2_table_number].iter_mut().enumerate() {
    //         *p3_entry = PDE64_ALL_ALLOWED | (p2_address) as u64;
    //         let start_index = p3_index * (TABLE_SIZE + 1) * TABLE_SIZE;
    //         table_array[start_index..start_index + TABLE_SIZE].fill(0);
    //         p2_address += (TABLE_SIZE + 1) * PAGE_SIZE;
    //     }
    //     p3_table[p2_table_number..].fill(0);

    //     // add the table with the global data in the last entry
    //     // find index based on the GiB at which SYSTEM_VIRTUAL_BASE is located at
    //     let table_entry = SYSTEM_VIRTUAL_BASE / (1 << 30);
    //     p3_table[table_entry] = PDE64_ALL_ALLOWED
    //         | PDE64_GLOBAL
    //         | (SYSTEM_VIRTUAL_BASE + HANDLER_CODE + handler_code_size().next_multiple_of(PAGE_SIZE))
    //             as u64;
    // };
    // can assume that all p2 tables are zero initialized after this loop

    // start installing entries for all already present pages
    // let mut previous_past_last_page = 0;
    // the page starting at address 0 is expected to never be written
    // if !present_pages.is_empty() {
    //     debug_assert_ne!(0, present_pages[0].0 >> PAGE_SHIFT);
    //     if let Some((_, virtual_end, _)) = present_pages.last() {
    //         if stack_start <= *virtual_end {
    //             return err_dandelion!(DandelionError::ContextFull);
    //         }
    //     }
    // }
    // for (guest_virtual_start, guest_virtual_end, origin_option) in present_pages {
    //     let (protection_flags, base_physical) = if let Some(guest_physical) = origin_option {
    //         // map to page at differnt space in guest, that is there and needs to be copy on write
    //         panic!("Should not have things that need remapping");
    //         (PDE64_PRESENT | PDE64_USER, guest_physical)
    //     } else {
    //         // map directly, since content is already there
    //         (PDE64_ALL_ALLOWED, guest_virtual_start)
    //     };
    //     debug!(
    //         "mapping new address range mapped {} to {}",
    //         guest_virtual_start,
    //         guest_virtual_end + 1
    //     );
    //     previous_past_last_page = set_range(
    //         table_array,
    //         mapping_offset + table_base,
    //         guest_virtual_start,
    //         guest_virtual_end + 1,
    //         protection_flags,
    //         mapping_offset + base_physical,
    //         previous_past_last_page,
    //     );
    // }

    // need to set page for stack
    // let stack_page_start = stack_start & !(LARGE_PAGE - 1);
    // guest_mem[stack_page_start..stack_start].fill(0);
    // previous_past_last_page = set_range(
    //     table_array,
    //     mapping_offset + table_base,
    //     stack_page_start,
    //     stack_start,
    //     PDE64_ALL_ALLOWED,
    //     mapping_offset + stack_page_start,
    //     previous_past_last_page,
    // );

    // the page tables also need to be accessable to root mode
    // previous_past_last_page = set_range(
    //     table_array,
    //     mapping_offset + table_base,
    //     table_base,
    //     p4_address + PAGE_SIZE,
    //     PDE64_PRESENT | PDE64_RW,
    //     mapping_offset + table_base,
    //     previous_past_last_page,
    // );

    // need to make the page table handler accessable to root mode
    // previous_past_last_page = set_range(
    //     table_array,
    //     mapping_offset + table_base,
    //     interrupt_range.0,
    //     interrupt_range.1,
    //     PDE64_PRESENT | PDE64_RW,
    //     mapping_offset + interrupt_range.0,
    //     previous_past_last_page,
    // );

    // make the read only mappings also avaliable at their physical locations for root mode
    // set_range(
    //     table_array,
    //     mapping_offset + table_base,
    //     interrupt_range.1,
    //     last_address,
    //     PDE64_PRESENT,
    //     mapping_offset + interrupt_range.1,
    //     previous_past_last_page,
    // );

    // go through tree and see if it is set up the way we expect
    // #[cfg(debug_assertions)]
    // {
    //     // try to resolve every page up to the stack pointer
    //     let mut page_address = 0;
    //     while page_address < last_address {
    //         // get all table entries
    //         let mut debug_info = format!(
    //             "table_base: {}\npage address: {}\n",
    //             table_base, page_address,
    //         );
    //         let p4_entry = get_entry_from_address(page_address, PML4_SHIFT);
    //         assert_eq!(0, p4_entry, "{}", debug_info);
    //         let p3_entry = get_entry_from_address(page_address, HUGE_PAGE_SHIFT);
    //         let p2_entry = get_entry_from_address(page_address, LARGE_PAGE_SHIFT);
    //         let p1_entry = get_entry_from_address(page_address, PAGE_SHIFT);

    //         debug_info.push_str(
    //             format!(
    //                 "p3 entry: {}, p2_entry: {} p1_entry: {}\n",
    //                 p3_entry, p2_entry, p1_entry
    //             )
    //             .as_str(),
    //         );

    //         let (p3_address_local, p3_flags) = get_address_and_flags(p4_table[p4_entry]);
    //         assert_eq!(
    //             mapping_offset + p3_address,
    //             p3_address_local,
    //             "{}",
    //             debug_info
    //         );
    //         assert_eq!(
    //             PDE64_PRESENT | PDE64_RW | PDE64_USER,
    //             p3_flags,
    //             "{}",
    //             debug_info
    //         );

    //         let (p2_address_local, p2_flags) = get_address_and_flags(p3_table[p3_entry]);
    //         let expected_address =
    //             mapping_offset + table_base + (p3_entry * (TABLE_SIZE + 1)) * PAGE_SIZE;
    //         assert_eq!(expected_address, p2_address_local, "{}", debug_info);
    //         assert_eq!(
    //             PDE64_PRESENT | PDE64_RW | PDE64_USER,
    //             p2_flags,
    //             "{}",
    //             debug_info
    //         );
    //         debug_info.push_str(
    //             format!(
    //                 "p2_address: {}, with p2_flags: {} for large page: {}\n",
    //                 p2_address_local,
    //                 p2_flags,
    //                 p3_entry * HUGE_PAGE + p2_entry * LARGE_PAGE,
    //             )
    //             .as_str(),
    //         );

    //         let p2_index = p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE + p2_entry;
    //         let (p1_address_local, p1_flags) = get_address_and_flags(table_array[p2_index]);
    //         debug_info.push_str(
    //             format!(
    //                 "p1_address_local: {}, with p1_flags: {}\n",
    //                 p1_address_local, p1_flags
    //             )
    //             .as_str(),
    //         );
    //         if p1_flags & PDE64_PRESENT != 0 {
    //             if p1_flags & PDE64_IS_PAGE != 0 {
    //                 if p1_flags & PDE64_RW == 0 || p1_flags & PDE64_USER == 0 {
    //                     // copy on write large page or system page
    //                     assert!(p1_address_local > stack_start, "{}", debug_info);
    //                 } else {
    //                     // directly mapped large page
    //                     let large_page_address =
    //                         (mapping_offset + page_address) & !(LARGE_PAGE - 1);
    //                     assert_eq!(large_page_address, p1_address_local, "{}", debug_info);
    //                 }
    //             } else {
    //                 // if it is not a page, go lower
    //                 assert!(p1_flags & PDE64_RW != 0, "{}", debug_info);
    //                 let p1_index = TABLE_SIZE
    //                     + p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE
    //                     + p2_entry * TABLE_SIZE
    //                     + p1_entry;
    //                 let (page_address_local, page_flags_local) =
    //                     get_address_and_flags(table_array[p1_index]);

    //                 debug_info.push_str(
    //                     format!(
    //                         "paged_address_local: {}, page_flags_local: {}\n",
    //                         page_address_local, page_flags_local
    //                     )
    //                     .as_str(),
    //                 );
    //                 if page_flags_local & PDE64_PRESENT != 0 {
    //                     if page_flags_local & PDE64_RW == 0 || page_flags_local & PDE64_USER == 0 {
    //                         assert!(page_address_local >= stack_start, "{}", debug_info);
    //                     } else {
    //                         assert_eq!(
    //                             mapping_offset + page_address,
    //                             page_address_local,
    //                             "{}",
    //                             debug_info
    //                         );
    //                     }
    //                 }
    //             }
    //         } else {
    //             assert_eq!(0, p1_address_local, "{}", debug_info);
    //             assert_eq!(0, p1_flags, "{}", debug_info);
    //         }
    //         page_address += PAGE_SIZE;
    //     }
    // }
    let cr3 = *p4_physical | USER_PCID;
    info!("CR3: {}, (0x{:x})", cr3, cr3);
    sregs.cr3 = cr3;
    Ok((
        stack_start,
        PageFaultMetadata {
            #[cfg(feature = "backend_debug")]
            // p4_address: p4_physical,
            #[cfg(feature = "backend_debug")]
            // p3_address: p3_physical,
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

// #[cfg(feature = "backend_debug")]
// pub fn check_page_fault_handling(
//     vcpu: &VcpuFd,
//     metadata: &PageFaultMetadata,
//     guest_mem: &mut [u8],
// ) {
//     dump_regs(vcpu);
//     let regs = vcpu.get_regs().unwrap();
//     let sregs = vcpu.get_sregs().unwrap();
//     // the faulting address is in cr2, cr3 holds the root table address
//     let faulting_address = sregs.cr2 as usize;
//     let p4_address = sregs.cr3 as usize;
//     let page_base_address = faulting_address & !(PAGE_SIZE - 1);

//     debug!("Starting to handle page fault at {}", faulting_address);
//     assert!(faulting_address < PAGE_SIZE || metadata.stack_start <= faulting_address);
//     assert_ne!(p4_address, metadata.p4_address);

//     // get all table entries
//     let p4_entry = get_entry_from_address(faulting_address, PML4_SHIFT);
//     assert_eq!(0, p4_entry);
//     let p3_entry = get_entry_from_address(faulting_address, HUGE_PAGE_SHIFT);
//     let p2_entry = get_entry_from_address(faulting_address, LARGE_PAGE_SHIFT);
//     let p1_entry = get_entry_from_address(faulting_address, PAGE_SHIFT);

//     // Asserts for the exit of the debug assembly
//     // r10 and r11 should contain addresses for the entries of the p4 and p3 entries
//     assert_eq!(regs.r10, (metadata.p4_address) as u64);
//     assert_eq!(
//         regs.r11,
//         (metadata.p3_address + p3_entry * size_of::<u64>()) as u64
//     );
//     // r12 and r13 hold the indices into the p2 and p1 tables respectively
//     assert_eq!(regs.r12, (p2_entry * size_of::<u64>()) as u64);
//     assert_eq!(regs.r13, (p1_entry * size_of::<u64>()) as u64);
//     // r14 and p15 hold the p2 and p1 base address respecively
//     let p2_offset = (p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE) * size_of::<u64>();
//     assert_eq!(
//         regs.r14,
//         (metadata.table_base + p2_offset * size_of::<u64>()) as u64
//     );
//     let p1_offset = ((p3_entry * (TABLE_SIZE + 1) + 1 + p2_entry) * TABLE_SIZE) * size_of::<u64>();
//     debug!(
//         "table_base: {}, p2 address: {}",
//         metadata.table_base, regs.r14
//     );
//     assert_eq!(regs.r15, (metadata.table_base + p1_offset) as u64);

//     // go through the page table and check it is correct
//     let (p3_address, p3_flags) = get_address_and_flags(
//         u8_slice_to_u64_slice(&mut guest_mem[p4_address..p4_address + PAGE_SIZE])[p4_entry],
//     );
//     assert_eq!(p3_address, metadata.p3_address);
//     assert_eq!(p3_flags & !PDE64_ACCESSED, PDE64_ALL_ALLOWED);

//     let (p2_address, p2_flags) = get_address_and_flags(
//         u8_slice_to_u64_slice(&mut guest_mem[p3_address..p3_address + PAGE_SIZE])[p3_entry],
//     );
//     let canonical_p2_start =
//         metadata.table_base + (p3_entry * (TABLE_SIZE + 1) * TABLE_SIZE) * size_of::<u64>();
//     assert_eq!(p2_address, canonical_p2_start);
//     assert_eq!(p2_flags & !PDE64_ACCESSED, PDE64_ALL_ALLOWED);

//     let (p1_address, p1_flags) = get_address_and_flags(
//         u8_slice_to_u64_slice(&mut guest_mem[p2_address..p2_address + PAGE_SIZE])[p2_entry],
//     );
//     let canonical_p1_start = canonical_p2_start + ((p2_entry + 1) * TABLE_SIZE) * size_of::<u64>();
//     assert_eq!(p1_address, canonical_p1_start);
//     assert_eq!(
//         p1_flags & !PDE64_ACCESSED,
//         PDE64_ALL_ALLOWED,
//         "r15 is holding: {}",
//         regs.r15
//     );

//     let (page_address, page_flags) = get_address_and_flags(
//         u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])[p1_entry],
//     );
//     assert_eq!(page_address, page_base_address);
//     assert_eq!(
//         page_flags & !(PDE64_ACCESSED | PDE64_DIRTY),
//         PDE64_ALL_ALLOWED
//     );

//     // r8 should beholding the previous p2 entry, so check that also
//     let handler_p1_address = regs.r8 & !(PAGE_SIZE as u64 - 1);
//     let handler_p1_flags = regs.r8 & (PAGE_SIZE as u64 - 1);
//     let handler_page_address = regs.r9 & !(PAGE_SIZE as u64 - 1);
//     let handler_page_flags = regs.r9 & (PAGE_SIZE as u64 - 1);
//     debug!(
//         "handler p2 entry, address: {}, flags: {}",
//         handler_p1_address, handler_p1_flags
//     );
//     debug!(
//         "handler p1 entry, address: {}, flags: {}",
//         handler_page_address, handler_page_flags
//     );

//     if handler_p1_flags & PDE64_PRESENT == 0 || handler_p1_flags & PDE64_IS_PAGE != 0 {
//         // if it currently not present or is a LARGE_PAGE page, need to set up the p1 table
//         trace!("Checking p2 fault handling");
//         // check if actual page is reasonably set
//         if handler_p1_flags & PDE64_PRESENT == 0 {
//             // check that the remainder of the entries have been set to 0
//             for p1_index in 0..p1_entry {
//                 let set_value =
//                     u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
//                         [p1_index];
//                 assert_eq!(set_value, 0, "at index {}", p1_index);
//             }
//             let new_entry =
//                 u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])[p1_entry];
//             assert_eq!(
//                 new_entry & !(PDE64_DIRTY | PDE64_ACCESSED),
//                 PDE64_ALL_ALLOWED | page_base_address as u64
//             );
//             for p1_index in p1_entry + 1..TABLE_SIZE {
//                 let set_value =
//                     u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
//                         [p1_index];
//                 assert_eq!(set_value, 0, "at index {}", p1_index);
//             }
//             for page_index in 0..TABLE_SIZE {
//                 let set_value = u8_slice_to_u64_slice(
//                     &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
//                 )[page_index];
//                 assert_eq!(set_value, 0, "at index {}", page_index);
//             }
//         } else {
//             let old_address = handler_p1_address as usize;
//             assert_eq!(old_address % LARGE_PAGE, 0);
//             // check the p1 table
//             for p1_index in 0..p1_entry {
//                 let set_val =
//                     u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
//                         [p1_index];
//                 assert_eq!(
//                     set_val,
//                     PDE64_USER
//                         | PDE64_PRESENT
//                         | (handler_p1_address + (p1_index * PAGE_SIZE) as u64),
//                     "at index {}",
//                     p1_index
//                 );
//             }
//             let set_val =
//                 u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])[p1_entry];
//             assert_eq!(set_val, PDE64_ALL_ALLOWED | page_base_address as u64);
//             for p1_index in p1_entry..TABLE_SIZE {
//                 let set_val =
//                     u8_slice_to_u64_slice(&mut guest_mem[p1_address..p1_address + PAGE_SIZE])
//                         [p1_index];
//                 assert_eq!(
//                     set_val,
//                     PDE64_USER
//                         | PDE64_PRESENT
//                         | (handler_p1_address + (p1_index * PAGE_SIZE) as u64),
//                     "at index {}",
//                     p1_index
//                 );
//             }
//             for page_index in 0..TABLE_SIZE {
//                 let set_value = u8_slice_to_u64_slice(
//                     &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
//                 )[page_index];
//                 let expected = u8_slice_to_u64_slice(
//                     &mut guest_mem[old_address + p2_entry * PAGE_SIZE
//                         ..old_address + p2_entry * PAGE_SIZE + PAGE_SIZE],
//                 )[page_index];
//                 assert_eq!(
//                     set_value, expected,
//                     "at index {} for base {}",
//                     page_index, page_base_address
//                 );
//             }
//         };
//     } else {
//         if handler_page_flags & PDE64_PRESENT == 0 {
//             for page_index in 0..TABLE_SIZE {
//                 let set_value = u8_slice_to_u64_slice(
//                     &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
//                 )[page_index];
//                 assert_eq!(
//                     set_value, 0,
//                     "at index {} for address: {}",
//                     page_index, page_base_address
//                 );
//             }
//         } else {
//             let old_address = handler_page_address as usize;
//             for page_index in 0..TABLE_SIZE {
//                 let set_value = u8_slice_to_u64_slice(
//                     &mut guest_mem[page_base_address..page_base_address + PAGE_SIZE],
//                 )[page_index];
//                 let expected =
//                     u8_slice_to_u64_slice(&mut guest_mem[old_address..old_address + PAGE_SIZE])
//                         [page_index];
//                 assert_eq!(set_value, expected, "at index {}", page_index);
//             }
//         }
//     };
// }

fn setup_long_mode(sregs: &mut kvm_sregs, xregs: &mut kvm_xcrs) {
    sregs.cr4 =
        CR4_PAE | CR4_PCIDE | CR4_PGE | CR4_OSFXSR | CR4_OSXMMEXCPT | CR4_FSGSBASE | CR4_OSXSAVE;
    sregs.cr0 = CR0_PE | CR0_MP | CR0_ET | CR0_NE | CR0_WP | CR0_AM | CR0_PG;
    sregs.efer = EFER_LME | EFER_LMA;
    if cfg!(target_feature = "avx512f") {
        xregs.xcrs[0].value = XCR0_X87 | XCR0_SSE | XCR0_AVX | XCR0_AVX512;
    } else {
        xregs.xcrs[0].value = XCR0_X87 | XCR0_SSE | XCR0_AVX;
    }

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
    {
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
        let fpu_state = vcpu.get_fpu().unwrap();
        trace!("fpr: {:?}", fpu_state.fpr);
        trace!(
            "fcw:\t{:?}, fsw:\t{:?}, ftwx:\t{:?}",
            fpu_state.fcw,
            fpu_state.fsw,
            fpu_state.ftwx
        );
        trace!(
            "last_opcode: {:?}, last_ip: {:?}, last_dp:{:?}",
            fpu_state.last_opcode,
            fpu_state.last_ip,
            fpu_state.last_dp
        );
        trace!("xmm: {:?}", fpu_state.xmm);
        trace!("mxcsr: {:?}", fpu_state.mxcsr);
    }
}
