use super::x86_64::{
    LARGE_PAGE, PAGE_SIZE, PDE64_ALL_ALLOWED, PDE64_IS_PAGE, PDE64_PRESENT, PDE64_USER,
};
use core::arch::global_asm;

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
default_handler:
   mov eax, [rsp]
   out 31, eax
divide_error_exception_handler:
   mov eax, [rsp]
   out 0, eax
debug_interrupt_handler:
   mov eax, [rsp]
   out 1, eax
nmi_interrupt_handler:
   mov eax, [rsp]
   out 2, eax
breakpoint_exception_handler:
   mov eax, [rsp]
   out 3, eax
overflow_exception_handler:
   mov eax, [rsp]
   out 4, eax
bound_range_excepion_handler:
   mov eax, [rsp]
   out 5, eax
invalid_opcode_exception_handler:
   mov eax, [rsp]
   out 6, eax
device_not_available_exception_handler:
   mov eax, [rsp]
   out 7, eax
double_fault_exception_handler:
   mov eax, [rsp]
   out 8, eax
coprocessor_segment_overrun_handler:
   mov eax, [rsp]
   out 9, eax
invalid_tss_exception_handler:
   mov eax, [rsp]
   out 10, eax
segment_not_present_handler:
   mov eax, [rsp]
   out 11, eax
stack_fault_exception_handler:
   mov eax, [rsp]
   out 12, eax
general_protection_exception_handler:
   mov rax, [rsp]
   out 13, eax
page_fault_exception_handler:
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
   mov [rsp - 96], r15",
   // #[not(cfg((target_feature(enable = "avx512f")))]
   "vmovdqa [rsp - 144], ymm0",
   "vmovdqa [rsp - 176], ymm1",
   "vmovdqa [rsp - 208], ymm2",
   "vmovdqa [rsp - 240], ymm3",
   "vmovdqa [rsp - 272], ymm4",
   "vmovdqa [rsp - 304], ymm5",
   "vmovdqa [rsp - 336], ymm6",
   "vmovdqa [rsp - 368], ymm7",
   "# this handler assumes 4 level paging
   # each linear address consists of the following
   # 47 .. 39 | 38 .. 30 | 29 .. 21 | 20 .. 12 | 11 .. 0
   # p4       | p3       | p2       | p1       | Offset
   # with the p4 (PML4) being the offset into the page table pointed to by the root pointer in CR3
   # and then the others being indexes into the tables recursively
   # The p3 (direcotry pointer table) entries are either 1GB pages or directory table pointers
   # p2 (directory table entries) are either 2MB pages or tables
   # p1 (table) entries are always 4KB pages   
   # cr2 holds the offending address, cr3 the root table address, [rsp] holds the error code 
   # compute the table entries in 10 - 13, a number in [0..512)
   # mov r10, cr3 # r10 holds the address of the p4 entry, always expect offset to be zero, so is same as p4 table
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
   mov rax, cr3
   add r10, rax # r10 holds the address of the p4 entry
   mov rbx, [r10]
   and rbx, ~0xFFF # remove flags to get address
   add r11, rbx # r11 holds the address of the p3 entry
   mov r14, [r11]
   and r14, ~0xFFF # r14 now contains base address of p2 table
   # can compute the base address of p1 from this and p1 entry
   # by taking p2 entry, multiplying by table size and adding 1 page for the p2 page
   mov r15, r12
   imul r15, 512
   add r15, {PAGE_SIZE} 
   add r15, r14 # r15 now holds p1 base
   mov r8, [r14 + r12]
   mov r9, [r15 + r13]
   # now have the p2 entry in r8 and p1 entry in r9 (p1 entry may not be valid)
   test r8, {PDE64_PRESENT} # check if present  
   jz 2f # p2 entry is not present handle p2 demand pageing
   test r8, {PDE64_USER}
   jz 0f # if user is not set on present, abort handling
   test r8, {PDE64_IS_PAGE}
   jnz 4f # p2 is page, handle p2 zero copy 
   # check if p1 entry points to present page
   test r9, {PDE64_PRESENT}
   jz 3f # handle p1 demand pageing
   test r9, {PDE64_USER}
   jnz 5f # handle p1 zero copy
0: # if not user abort handling
   out 14, eax #
2: # handle p2 demand page, by zeroing p1 table and inserting mapping
   mov rcx, 0
   // set vector register to 0",
   "vpxor ymm0, ymm0, ymm0",
"1:",
   "vmovntdq [r15 + rcx], ymm0",
   "vmovntdq [r15 + rcx + 32], ymm0",
   "vmovntdq [r15 + rcx + 64], ymm0",
   "vmovntdq [r15 + rcx + 96], ymm0",
   "vmovntdq [r15 + rcx + 128], ymm0",
   "vmovntdq [r15 + rcx + 160], ymm0",
   "vmovntdq [r15 + rcx + 192], ymm0",
   "vmovntdq [r15 + rcx + 224], ymm0",
   // "mov qword ptr [r15 + rcx], 0",
   "add rcx, 256
   cmp rcx, {PAGE_SIZE} 
   jl 1b
   # all zeroed, need to set the p2 and p1 entry
   mov rax, r15
   or rax, {PDE64_ALL_ALLOWED}
   mov [r14 + r12], rax
3: # hanlde p1 demand page, set p1 entry and zeroing page
   mov rbx, cr2 
   and rbx, ~0xFFF
   or rbx, {PDE64_ALL_ALLOWED} 
   mov [r15 + r13], rbx
   # don't need to invalidate tlb entry, since there was no valid one before
   and rbx, ~0xFFF
   mov rcx, 0",
   "vpxor ymm0, ymm0, ymm0",
"1:",
   "vmovntdq [rbx + rcx], ymm0",
   "vmovntdq [rbx + rcx + 32], ymm0",
   "vmovntdq [rbx + rcx + 64], ymm0",
   "vmovntdq [rbx + rcx + 96], ymm0",
   "vmovntdq [rbx + rcx + 128], ymm0",
   "vmovntdq [rbx + rcx + 160], ymm0",
   "vmovntdq [rbx + rcx + 192], ymm0",
   "vmovntdq [rbx + rcx + 224], ymm0",
   // "mov qword ptr [rbx + rcx], 0",
   "add rcx, 256
   cmp rcx, {PAGE_SIZE}
   jl 1b
   jmp 9f # Finished hanlding demand pageing
4: # the p2 entry had the present flag set and is page, handle p2 copy on write
   # test r8, {PDE64_IS_PAGE} # check if page
   # jz 9f # if writable can go to hanlde p1 fault
   # if present and page, assume it is a copy on write fault (there should be no other option)
   # need to get current physical for copy on write
   mov rax, r8 
   and rax, (~0xFFF | {PDE64_PRESENT} | {PDE64_USER}) # reset old and add new premissions
   mov rbx, 0
1: # fill new p1 
   mov [r15 + rbx], rax
   add rax, {PAGE_SIZE} 
   add rbx, 8
   cmp rbx, {PAGE_SIZE} 
   jl 1b
   # set the one page with write permissions
   mov rax, r15 
   or rax, {PDE64_ALL_ALLOWED} 
   mov [r14 + r12], rax
5: # handle p1 copy on write
   mov rbx, cr2 
   and rbx, ~0xFFF
   or rbx, {PDE64_ALL_ALLOWED} 
   mov [r15 + r13], rbx
   invlpg [rbx] 
   mov rcx, r8
   test r8, {PDE64_IS_PAGE}
   cmovz rcx, r9
   and rcx, ~({LARGE_PAGE}-1)
   mov rax, r13 
   imul rax, 512
   add rax, rcx
   add rdx, r8 
   and rbx, ~0xFFF
   mov rcx, 0
1: # copy from old page",
   "vmovntdqa ymm0, [rax + rcx]",
   "vmovntdqa ymm1, [rax + rcx + 32]",
   "vmovntdqa ymm2, [rax + rcx + 64]",
   "vmovntdqa ymm3, [rax + rcx + 96]",
   "vmovntdqa ymm4, [rax + rcx + 128]",
   "vmovntdqa ymm5, [rax + rcx + 160]",
   "vmovntdqa ymm6, [rax + rcx + 192]",
   "vmovntdqa ymm7, [rax + rcx + 224]",
   "vmovntdq [rbx + rcx], ymm0",
   "vmovntdq [rbx + rcx + 32], ymm1",
   "vmovntdq [rbx + rcx + 64], ymm2",
   "vmovntdq [rbx + rcx + 96], ymm3",
   "vmovntdq [rbx + rcx + 128], ymm4",
   "vmovntdq [rbx + rcx + 160], ymm5",
   "vmovntdq [rbx + rcx + 192], ymm6",
   "vmovntdq [rbx + rcx + 224], ymm7",
   "add rcx, 256
   cmp rcx, {PAGE_SIZE} 
   jl 1b # finished hanlding p2 fault for copy on write
9: 
   # out 14, eax # !!! Uncomment for backend debug
   # restore the registers",
   // #[not(cfg((target_feature(enable = "avx512f")))]
   "vmovdqa ymm7, [rsp - 368]",
   "vmovdqa ymm6, [rsp - 336]",
   "vmovdqa ymm5, [rsp - 304]",
   "vmovdqa ymm4, [rsp - 272]",
   "vmovdqa ymm3, [rsp - 240]",
   "vmovdqa ymm2, [rsp - 208]",
   "vmovdqa ymm1, [rsp - 176]",
   "vmovdqa ymm0, [rsp - 144]",
   "mov r15, [rsp - 96]
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
   # add rsp, 0xd0
   add rsp, 8 # pop the interrupt handler argument (needs to be done manually, as not all handlers have one)
   rex64 iretq
floating_point_error_handler:
   mov eax, [rsp]
   out 16, eax
alignment_check_exception_handler:
   mov eax, [rsp]
   out 17, eax
machine_check_exception_handler:
   mov eax, [rsp]
   out 18, eax
simd_fp_exception_handler:
   mov eax, [rsp]
   out 19, eax
virtualization_exception_handler:
   mov eax, [rsp]
   out 20, eax
control_protection_exception:
   mov rax, [rsp]
   out 21, eax
user_exit_handler:
   out 32, eax
fault_handlers_end:
   hlt", 
    PAGE_SIZE = const PAGE_SIZE,
    LARGE_PAGE = const LARGE_PAGE,
    PDE64_PRESENT = const PDE64_PRESENT,
    PDE64_USER = const PDE64_USER,
    PDE64_ALL_ALLOWED = const PDE64_ALL_ALLOWED,
    PDE64_IS_PAGE = const PDE64_IS_PAGE
);
