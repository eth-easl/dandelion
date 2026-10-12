use super::x86_64::{
    LARGE_PAGE, PAGE_SIZE, PDE64_ALL_ALLOWED, PDE64_IS_PAGE, PDE64_PRESENT, PDE64_USER,
};
use super::{GLOBAL_SYSTEM_MEMORY_SIZE, ROOT_PCID, SYSTEM_VIRTUAL_BASE};
use core::arch::global_asm;

// Assembly function that uses rax and rbx to fetch a new page address into rax, uses label 1
macro_rules! FETCH_PAGE {
   () => {r#"
   mov rax, [0] # load the root of the free list
   out 33, eax
1: test rax, rax
   jz 0f # abort handling if head of free list is 0
   mov rbx, [rax] # load the next entry in the node
   lock cmpxchg [0], rbx
   jnz 1b # if 0, compare and exchange did not succeed, otherwise rax now contains the new page to be used
   out 37, eax
"#};
}

// TODO add version that use the 512 bit vector registers, if the feature is available
// Assembly function that uses rax and rbx to zero the page pointed to by rax, uses label 1
// #[not(cfg((target_feature(enable = "avx512f")))]
macro_rules! ZERO_PAGE {
    () => {r#"
   mov rbx, 0 # set page offset to 0
   vpxor ymm0, ymm0, ymm0 #set vector register to 0
1:
   vmovntdq [rax + rbx], ymm0
   vmovntdq [rax + rbx + 32], ymm0
   vmovntdq [rax + rbx + 64], ymm0
   vmovntdq [rax + rbx + 96], ymm0
   vmovntdq [rax + rbx + 128], ymm0
   vmovntdq [rax + rbx + 160], ymm0
   vmovntdq [rax + rbx + 192], ymm0
   vmovntdq [rax + rbx + 224], ymm0
   add rbx, 256
   cmp rbx, {PAGE_SIZE} 
   jl 1b
   out 38, eax
"#};
}

// Assembly function that copies page from rbx to rax using rcx and label 1
macro_rules! COPY_PAGE {
    () => {r#"
   mov rcx, 0
1: # copy from old page
   vmovntdqa ymm0, [rbx + rcx]
   vmovntdqa ymm1, [rbx + rcx + 32]
   vmovntdqa ymm2, [rbx + rcx + 64]
   vmovntdqa ymm3, [rbx + rcx + 96]
   vmovntdqa ymm4, [rbx + rcx + 128]
   vmovntdqa ymm5, [rbx + rcx + 160]
   vmovntdqa ymm6, [rbx + rcx + 192]
   vmovntdqa ymm7, [rbx + rcx + 224]
   vmovntdq [rax + rcx], ymm0
   vmovntdq [rax + rcx + 32], ymm1
   vmovntdq [rax + rcx + 64], ymm2
   vmovntdq [rax + rcx + 96], ymm3
   vmovntdq [rax + rcx + 128], ymm4
   vmovntdq [rax + rcx + 160], ymm5
   vmovntdq [rax + rcx + 192], ymm6
   vmovntdq [rax + rcx + 224], ymm7
   add rcx, 256
   cmp rcx, {PAGE_SIZE}
   jl 1b # finished hanlding p2 fault for copy on write      
"#};
}

global_asm!("
.global asm_start
.global default_handler
.global divide_error_exception_handler
.global debug_interrupt_handler
.global nmi_interrupt_handler
.global breakpoint_exception_handler
.global overflow_exception_handler
.global bound_range_excepion_handler
.global invalid_opcode_exception_handler
.global device_not_available_exception_handler
.global double_fault_exception_handler
.global coprocessor_segment_overrun_handler
.global invalid_tss_exception_handler
.global segment_not_present_handler
.global stack_fault_exception_handler
.global general_protection_exception_handler
.global page_fault_exception_handler
.global floating_point_error_handler
.global alignment_check_exception_handler
.global machine_check_exception_handler
.global simd_fp_exception_handler
.global virtualization_exception_handler
.global control_protection_exception
.global user_exit_handler
.global fault_handlers_end

// TODO: once attributes on asm stabilize,
// add in avx-512 versions
asm_start:
   mov rcx, 0x7FC00
   shl rcx, 20
   mov rcx, [73728]
   mov rax, 0
   mov [73728], rax
   // add rcx, 64
   // mov rbx, [rcx]
   // mov ax, 0x43
   // mov ss, ax
   // mov eax, [0]
   out 1, eax
default_handler:
   out 31, eax
   mov eax, [rsp]
divide_error_exception_handler:
   out 0, eax
   mov eax, [rsp]
debug_interrupt_handler:
   out 1, eax
   mov eax, [rsp]
nmi_interrupt_handler:
   out 2, eax
   mov eax, [rsp]
breakpoint_exception_handler:
   out 3, eax
   mov eax, [rsp]
overflow_exception_handler:
   out 4, eax
   mov eax, [rsp]
bound_range_excepion_handler:
   out 5, eax
   mov eax, [rsp]
invalid_opcode_exception_handler:
   out 6, eax
   mov eax, [rsp]
device_not_available_exception_handler:
   out 7, eax
   mov eax, [rsp]
double_fault_exception_handler:
   out 8, eax
   mov eax, [rsp]
coprocessor_segment_overrun_handler:
   out 9, eax
   mov eax, [rsp]
invalid_tss_exception_handler:
   out 10, eax
   mov eax, [rsp]
segment_not_present_handler:
   out 11, eax
   mov eax, [rsp]
stack_fault_exception_handler:
   out 12, eax
   mov eax, [rsp]
general_protection_exception_handler:
   out 13, eax
   mov rax, [rsp]
page_fault_exception_handler:
   # out 14, eax
   # ensure the reentry flag is set to 0 after returning
   mov [rsp - 8], rax
   mov rax, [rsp + 16]
   and rax, ~0x10000
   mov [rsp + 16], rax
   # preserve registers we use in handler
   mov [rsp - 16], rbx
   mov [rsp - 24], rcx
   mov [rsp - 32], rdx
   mov [rsp - 40], r8
   mov [rsp - 48], r9
   mov [rsp - 56], r10
   mov [rsp - 64], r11
   mov [rsp - 72], r12
   mov [rsp - 80], r13
   mov [rsp - 88], r14
   mov [rsp - 96], r15
   vmovdqa [rsp - 144], ymm0
   vmovdqa [rsp - 176], ymm1
   vmovdqa [rsp - 208], ymm2
   vmovdqa [rsp - 240], ymm3
   vmovdqa [rsp - 272], ymm4
   vmovdqa [rsp - 304], ymm5
   vmovdqa [rsp - 336], ymm6
   vmovdqa [rsp - 368], ymm7
   # Switch the page table to the root page table, keep old root page table pointer in rdx
   mov rdx, cr3
   mov rax, {ROOT_CR3_UPPER}
   shl rax, 32
   mov rbx, {ROOT_CR3_LOWER}
   add rax, rbx
   mov cr3, rax
   # this handler assumes 4 level paging
   # each linear address consists of the following
   # 47 .. 39 | 38 .. 30 | 29 .. 21 | 20 .. 12 | 11 .. 0
   # p4       | p3       | p2       | p1       | Offset
   # with the p4 (PML4) being the offset into the page table pointed to by the root pointer in CR3
   # and then the others being indexes into the tables recursively
   # The p3 (direcotry pointer table) entries are either 1GB pages or directory table pointers
   # p2 (directory table) entries are either 2MB pages or tables
   # p1 (table) entries are always 4KB pages   
   # cr2 holds the offending address, cr3 the root table address, [rsp] holds the error code 
   # compute the table entries in 10 - 13, a number in [0..512)
   # check if the access was to the NULL page, if so fail out of the function
   mov rax, cr2
   and rax, ~0xFFF
   test rax, rax
   jz 0f
   mov r10, cr2 
   mov r11, cr2 
   mov r12, cr2 
   mov r13, cr2 
   shr r10, 36 # p4 shift (-3 since we want it to be 8 byte address offset)
   shr r11, 27 # p3 
   shr r12, 18 # p2
   shr r13, 9 # p1
   # Remove anything before the entry number and round to 8 byte allign
   and r10, 0xFF8
   and r11, 0xFF8
   and r12, 0xFF8
   and r13, 0xFF8
   # find the entry addresses
   mov rax, rdx # load the old cr3
   and rax, ~0xFFF # filter out the PCID to get the p4 address
   add r10, rax # after add r10 holds the address of the p4 entry
   mov rbx, [r10]
   and rbx, ~0xFFF # remove flags to get p3 address
   add r11, rbx # after add r11 holds the address of the p3 entry
   mov rcx, [r11]
   and rcx, ~0xFFF # r14 now contains base address of p2 table
   add r12, rcx # after add r12 holds the address of the p2 entry
   mov rcx, [r12] # after rcx holds the p2 entry
   mov rax, [rsp] # load error code
   test rax, 0x1 # check if the page fault was caused by page not being present
   jnz 3f # if the last bit of the error code is not zero, the page was already present, handle CoW
   test rcx, {PDE64_PRESENT} # check if present  
   and rcx, ~0xFFF # rcx now contains the base address of the p1 table (if it was present
   jnz 2f # p2 entry is present, check go to check for p1 entry using the table in rcx
   # p1 table is not present, fault in a zeroed p1 table",
   FETCH_PAGE!(),
   ZERO_PAGE!(),
   "mov rcx, rax # ensure rcx holds the pointer to the p1 table
   or rax, {PDE64_ALL_ALLOWED}
   mov [r12], rax # insert p1 table into p2 entry
2: # rcx contains valid p1 table pointer, need to get a new page and insert into the table",
   FETCH_PAGE!(), // uses rax and rbx to fetch new page address into rax
   ZERO_PAGE!(), // uses rax and rbx to zero new page
   "# need to set the page address in rax into the p1 entry
   add r13, rcx
   or rax, {PDE64_ALL_ALLOWED}
   out 36, eax
   mov [r13], rax
   out 39, eax
   jmp 9f
3: # fix CoW page, first find old physical address, rcx holds the p2 entry
   # TODO: think if we need to do more checks here, (could check USER flag, to ensure is not root mapping)
   and rcx, ~0xFFF # remove flags to get p1 address
   add r13, rcx # after add r13 holds the address of the p1 entry",
   FETCH_PAGE!(), // rax holds the new page, rbx and label 1 were used in the process
   "mov rbx, [r13] # p1 entry
   and rbx, ~0xFFF # get page address",
   COPY_PAGE!(), // copy page from address in rbx to rax, using rcx and label 1
   "or rax, {PDE64_ALL_ALLOWED}
   mov [r13], rax # set the entry with the new page
   out 34, eax
9: 
   mov cr3, rdx # restore the user page table
   mov rax, cr2 # load the faulting address
   invlpg [rax] # invalided previous entry
   # restore the registers
   vmovdqa ymm7, [rsp - 368]
   vmovdqa ymm6, [rsp - 336]
   vmovdqa ymm5, [rsp - 304]
   vmovdqa ymm4, [rsp - 272]
   vmovdqa ymm3, [rsp - 240]
   vmovdqa ymm2, [rsp - 208]
   vmovdqa ymm1, [rsp - 176]
   vmovdqa ymm0, [rsp - 144]
   mov r15, [rsp - 96]
   mov r14, [rsp - 88]
   mov r13, [rsp - 80]
   mov r12, [rsp - 72]
   mov r11, [rsp - 64]
   mov r10, [rsp - 56]
   mov r9, [rsp - 48]
   mov r8, [rsp - 40]
   mov rdx, [rsp - 32]
   mov rcx, [rsp - 24]
   mov rbx, [rsp - 16]
   mov rax, [rsp - 8]
   add rsp, 8 # pop the interrupt handler argument (needs to be done manually, as not all handlers have one)
   rex64 iretq
0: # Abort handling and exit to show error, ran out of memory or there is a seg fault
   out 14, eax #
floating_point_error_handler:
   out 16, eax
   mov eax, [rsp]
alignment_check_exception_handler:
   out 17, eax
   mov eax, [rsp]
machine_check_exception_handler:
   out 18, eax
   mov eax, [rsp]
simd_fp_exception_handler:
   out 19, eax
   mov eax, [rsp]
virtualization_exception_handler:
   out 20, eax
   mov eax, [rsp]
control_protection_exception:
   out 21, eax
   mov rax, [rsp]
user_exit_handler:
   out 32, eax
fault_handlers_end:
   hlt", 
    PAGE_SIZE = const PAGE_SIZE,
   //  LARGE_PAGE = const LARGE_PAGE,
    PDE64_PRESENT = const PDE64_PRESENT,
   //  PDE64_USER = const PDE64_USER,
    PDE64_ALL_ALLOWED = const PDE64_ALL_ALLOWED,
   //  PDE64_IS_PAGE = const PDE64_IS_PAGE,
    ROOT_CR3_UPPER = const ((SYSTEM_VIRTUAL_BASE + GLOBAL_SYSTEM_MEMORY_SIZE) as u64 + ROOT_PCID) >> 32,
    ROOT_CR3_LOWER = const ((SYSTEM_VIRTUAL_BASE + GLOBAL_SYSTEM_MEMORY_SIZE) as u64 + ROOT_PCID) & 0xFFFF_FFFF,
);
