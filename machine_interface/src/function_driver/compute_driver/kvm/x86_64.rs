use kvm_bindings::{kvm_fpu, kvm_regs, kvm_segment, kvm_sregs};
use kvm_ioctls::{VcpuFd, VmFd};

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
/// Page size
const PDE64_PS: u64 = 1 << 7;

/// PML4 address (guest virtual)
const P4_ADDR: usize = 0x1000;
/// First PDP address (guest virtual)
const P3_ADDR: usize = 0x2000;

fn u8_slice_to_u64_slice(input: &mut [u8]) -> &mut [u64] {
    assert!(
        input.len() % 8 == 0,
        "Input slice length must be a multiple of 8"
    );
    assert!(
        input.as_ptr() as usize % 8 == 0,
        "Input slice must be 8-byte aligned"
    );
    let u64_len = input.len() / 8;
    unsafe { std::slice::from_raw_parts_mut(input.as_mut_ptr() as *mut u64, u64_len) }
}

pub struct ResetState {
    sregs: kvm_sregs,
}

impl ResetState {
    pub fn new(_vm: &VmFd, vcpu: &VcpuFd) -> Self {
        let mut sregs = vcpu.get_sregs().unwrap();
        setup_long_mode(&mut sregs);
        return Self { sregs };
    }

    pub fn init_vcpu(&self, vcpu: &VcpuFd, entry_point: u64, stack_pointer: u64) {
        // TODO: set up exception handler(s) for invalid instructions / memory accesses
        vcpu.set_sregs(&self.sregs).unwrap();
        vcpu.set_regs(&kvm_regs {
            rip: entry_point,
            rsp: stack_pointer,
            rflags: 2,
            ..Default::default()
        })
        .unwrap();
        vcpu.set_fpu(&kvm_fpu::default()).unwrap();
    }

    pub fn set_page_table(&self, guest_mem: &mut [u8]) {
        // use identity mapping of 1GB huge page
        // TODO: support larger guest memory (context) size
        // TODO: store the page table in another guest memory slot (outside the context)
        assert!(guest_mem.len() <= 1 << 30);
        let p4 = u8_slice_to_u64_slice(&mut guest_mem[P4_ADDR..P4_ADDR + 0x1000]);
        p4[0] = PDE64_PRESENT | PDE64_RW | PDE64_USER | P3_ADDR as u64;
        let p3 = u8_slice_to_u64_slice(&mut guest_mem[P3_ADDR..P3_ADDR + 0x1000]);
        p3[0] = PDE64_PRESENT | PDE64_RW | PDE64_USER | PDE64_PS; // 1GB huge page
    }
}

fn setup_long_mode(sregs: &mut kvm_sregs) {
    sregs.cr3 = P4_ADDR as u64;
    sregs.cr4 = CR4_PAE | CR4_OSFXSR | CR4_OSXMMEXCPT | CR4_FSGSBASE;
    sregs.cr0 = CR0_PE | CR0_MP | CR0_ET | CR0_NE | CR0_WP | CR0_AM | CR0_PG;
    sregs.efer = EFER_LME | EFER_LMA;
    sregs.cs = kvm_segment {
        base: 0,
        limit: 0xffffffff,
        selector: 1 << 3,
        type_: 11,
        present: 1,
        dpl: 0,
        db: 0,
        s: 1,
        l: 1,
        g: 1,
        ..Default::default()
    };
}

pub fn dump_regs(vcpu: &VcpuFd) {
    let regs = vcpu.get_regs().unwrap();
    print!(
        "rip: {:<#10x}, rsp: {:<#10x}, rflags: {:<#10x}",
        regs.rip, regs.rsp, regs.rflags,
    );
    println!();
    print!("rax: {:<#10x}, ", regs.rax);
    print!("rbx: {:<#10x}, ", regs.rbx);
    print!("rcx: {:<#10x}, ", regs.rcx);
    print!("rdx: {:<#10x}, ", regs.rdx);
    println!("");
}
