use kvm_bindings::{
    kvm_vcpu_init, KVM_REG_ARM64, KVM_REG_ARM64_SYSREG, KVM_REG_ARM_CORE, KVM_REG_SIZE_U64,
};
use kvm_ioctls::{VcpuFd, VmFd};

// General Purpose Registers
const CORE_REG_BASE: u64 = KVM_REG_ARM64 | KVM_REG_SIZE_U64 | KVM_REG_ARM_CORE as u64;
const X0_ID: u64 = CORE_REG_BASE + 2 * 0;
const PC_ID: u64 = CORE_REG_BASE + 2 * 32;
const PSTATE_ID: u64 = CORE_REG_BASE + 2 * 33;
const SP_EL1_ID: u64 = CORE_REG_BASE + 2 * 34;

// System Registers at EL1
const SYS_REG_BASE: u64 = KVM_REG_ARM64 | KVM_REG_SIZE_U64 | KVM_REG_ARM64_SYSREG as u64;
/// Architectural Feature Access Control Register
const CPACR_EL1_ID: u64 = SYS_REG_BASE | 0b11_000_0001_0000_010;
/// Translation Table Base Register 0
const TTBR0_EL1_ID: u64 = SYS_REG_BASE | 0b11_000_0010_0000_000;
/// Memory Attribute Indirection Register
const MAIR_EL1_ID: u64 = SYS_REG_BASE | 0b11_000_1010_0010_000;
/// Translation Control Register
const TCR_EL1_ID: u64 = SYS_REG_BASE | 0b11_000_0010_0000_010;
/// System Control Register
const SCTLR_EL1_ID: u64 = SYS_REG_BASE | 0b11_000_0001_0000_000;

// block descriptor (translation table entry) attributes
const PDE_VALID: u64 = 1 << 0;
const PDE_AF: u64 = 1 << 10;

/// Level 1 Page Table Address
const L1_ADDR: usize = 0x1000;

pub fn u8_slice_to_u64_slice(input: &mut [u8]) -> &mut [u64] {
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

fn get_reg(vcpu: &VcpuFd, reg_id: u64) -> u64 {
    let mut data = [0u8; 8];
    let reg_size = vcpu.get_one_reg(reg_id, &mut data).unwrap();
    assert_eq!(reg_size, 8);
    return u64::from_le_bytes(data);
}

fn set_reg(vcpu: &VcpuFd, reg_id: u64, data: u64) {
    let reg_size = vcpu.set_one_reg(reg_id, &data.to_le_bytes()).unwrap();
    assert_eq!(reg_size, 8);
}

pub struct ResetState {
    kvi: kvm_vcpu_init,
}

impl ResetState {
    pub fn new(vm: &VmFd, _vcpu: &VcpuFd) -> Self {
        let mut kvi = kvm_vcpu_init::default();
        vm.get_preferred_target(&mut kvi).unwrap();
        kvi.features[0] |= 1 << kvm_bindings::KVM_ARM_VCPU_PSCI_0_2;
        return Self { kvi };
    }

    pub fn init_vcpu(&self, vcpu: &VcpuFd, entry_point: u64, stack_pointer: u64) {
        // TODO: set up exception handler(s) for invalid instructions / memory accesses
        vcpu.vcpu_init(&self.kvi).unwrap();
        set_reg(vcpu, PC_ID, entry_point);
        set_reg(vcpu, SP_EL1_ID, stack_pointer);
        // enable advanced SIMD support: https://developer.arm.com/documentation/ddi0409/i/programmers-model/about-this-programmers-model/enabling-advanced-simd-and-floating-point-support
        set_reg(vcpu, CPACR_EL1_ID, 0x300000);
        // configure MMU stage 1 translation
        set_reg(vcpu, TTBR0_EL1_ID, L1_ADDR as u64);
        set_reg(vcpu, MAIR_EL1_ID, 0xff);
        set_reg(vcpu, TCR_EL1_ID, 0x80803519);
        let sctlr = get_reg(vcpu, SCTLR_EL1_ID);
        set_reg(vcpu, SCTLR_EL1_ID, sctlr | 1);
    }

    // It's possible to disable the guest->host (stage 1) MMU translation
    // However, this will make all data accesses be Device type: https://developer.arm.com/documentation/102376/0200/Describing-memory-in-AArch64/MMU-disabled
    // And an unaligned access to a Device region will trigger an exception: https://developer.arm.com/documentation/102376/0100/Alignment-and-endianness
    pub fn set_page_table(&self, guest_mem: &mut [u8]) {
        // use identity mapping of 1GB huge page
        // 39-bit (512GB) address space is configured in TCR_EL1.T0SZ, so the starting level of translation is level 1:
        // https://developer.arm.com/documentation/101811/0103/Translation-granule/The-starting-level-of-address-translation
        // TODO: store the page table in another guest memory slot (outside the context)
        let mem_size_gb = (guest_mem.len() + (1 << 30) - 1) / (1 << 30);
        assert!(mem_size_gb <= 512); // number of PTE entries in a single PD
        let l1 = u8_slice_to_u64_slice(&mut guest_mem[L1_ADDR..L1_ADDR + 0x1000]);
        for i in 0..mem_size_gb {
            l1[i] = PDE_VALID | PDE_AF | (i as u64) << 30;
        }
    }
}

pub fn dump_regs(vcpu: &VcpuFd) {
    print!(
        "pc : {:<#10x}, sp : {:<#10x}, pstate: {:<#10x}",
        get_reg(vcpu, PC_ID),
        get_reg(vcpu, SP_EL1_ID),
        get_reg(vcpu, PSTATE_ID),
    );
    for i in 0..31 {
        if i % 8 == 0 {
            println!("");
        }
        print!("x{:<2}: {:<#10x}, ", i, get_reg(vcpu, X0_ID + 2 * i));
    }
    println!("");
}
