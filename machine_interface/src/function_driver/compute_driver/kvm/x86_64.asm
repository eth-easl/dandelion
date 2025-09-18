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
   #mov ax, 0x50
   #mov ds, ax 
   #ltr ax
   #mov ax, ss
   #mov ax, 0x43
   #mov ss, ax
   #mov ds, ax
   #int 49
   #mov rax, -1
   #mov rax, [rax] 
   mov rax, 2
   push rax
   mov rax, 3
   mov rax, 4
   int 32
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
   push rbp
   pushf
   mov rbp, rsp
   or word ptr[rbp], 0x100
   popf
   pop rbp
   mov rax, [rsp]
   mov rax, [rsp + 8]
   mov rax, [rsp + 16]
   mov rax, [rsp + 24]
   mov rax, [rsp + 32]
   mov rax, [rsp + 40]
   mov rax, [rsp]
   out 13, eax
page_fault_exception_handler:
   # for debugging, enable debug interrupts
   push rbp
   pushf
   mov rbp, rsp
   or word ptr[rbp], 0x100
   popf
   pop rbp
   #mov rax, [rsp]
   #mov rax, [rsp + 8]
   #mov rax, [rsp + 16]
   #mov QWORD PTR [rsp + 16], 51
   # ensure the reentry flag is set to 0 after returning
   mov rax, [rsp + 24]
   and rax, 0xFFFFFFFFFFFEFFFF
   mov [rsp + 24], rax
   # push registers we use in the handler
   mov [rsp - 8], rax
   # cr2 holds the offending address, cr3 the root table address, [rsp] holds the error code 
   # TODO: might want to do some check on the error code
   # find the offending entry in the root page table by looking at bits 47..39
   mov rax, cr2
   shr rax, 39
   and rax, 0xFF
   hlt
   #mov rax, [rsp + 32]
   #mov rax, [rsp + 40]
   #mov QWORD PTR [rsp + 40], 67
   #mov rax, [rsp + 32]
   # sub rsp, 16
   #mov rbx, [0x6000]
   #add rbx, 1
   #mov [0x6000], rbx
   #cmp rbx, 1
   #jne fault_handlers_end
   # restore the registers we use during handler
   mov [rsp - 8], rax
   # set the stack pointer to remove the error code and return from interrupt
   add rsp, 8 
   rex64 iretq
   #out 14, eax
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

# dont forget to clear page cache after changing the entry