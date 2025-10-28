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
.global alignment_check_exception_handler
.global machine_check_exception_handler
.global simd_fp_exception_handler
.global virtualization_exception_handler
.global control_protection_exception
.global user_exit_handler
.global fault_handlers_end

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
   # preserve registers we use in handler
   push rax
   # ensure the reentry flag is set to 0 after returning
   mov rax, [rsp + 24]
   and rax, ~0x10000
   mov [rsp + 24], rax
   # handle fault in host
   mov rax, [rsp + 8] # load the error code to rax for the host see
   out 14, eax
   mov rax, cr2  
   invlpg [rax] # invalidate tlb entry
   pop rax
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
   hlt