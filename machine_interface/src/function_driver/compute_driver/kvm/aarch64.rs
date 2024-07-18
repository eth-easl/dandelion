use kvm_bindings::kvm_vcpu_init;
use kvm_ioctls::{VcpuFd, VmFd};

const CORE_REG_BASE: u64 = 0x6030_0000_0010_0000;
const X0_ID: u64 = CORE_REG_BASE + 2 * 0;
const PC_ID: u64 = CORE_REG_BASE + 2 * 32;
const PSTATE_ID: u64 = CORE_REG_BASE + 2 * 33;
const SP_EL1_ID: u64 = CORE_REG_BASE + 2 * 34;

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
    }

    pub fn set_page_table(&self, _guest_mem: &mut [u8]) {
        // disable guest->host translation, so no need for the guest page table here
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
