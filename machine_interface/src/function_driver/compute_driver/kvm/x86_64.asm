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
   # this handler assumes 4 level paging
   # each linera address consists of the following
   # 47 .. 39  | 38 .. 30        | 29 .. 21  | 20 .. 12  | 11 .. 0
   # PML4      | Directory PTR   | Directory | Table     | Offset
   # with the PML4 being the offset into the page table pointed to by the root pointer in CR3
   # and then the others being indexes into the tables recursively
   # The direcotry pointer table entries are either 1GB pages or directory table pointers
   # Directory table entries are either 2MB pages or tables
   # Table entries are always 4KB pages
   # push registers we use in the handler
   mov [rsp - 8], rax
   mov [rsp - 16], rbx
   mov [rsp - 24], rcx
   mov [rsp - 32], rdx
   # for debugging, enable debug interrupts
   push rbp
   pushf
   mov rbp, rsp
   or word ptr[rbp], 0x100
   popf
   pop rbp 
   # ensure the reentry flag is set to 0 after returning
   mov rax, [rsp + 24]
   and rax, 0xFFFFFFFFFFFEFFFF
   mov [rsp + 24], rax
   # cr2 holds the offending address, cr3 the root table address, [rsp] holds the error code 
   # TODO: might want to do some check on the error code
   # find the entry in the PML4 table by looking at bits 47..39 of the offending pointer
   mov rax, cr3 # rax holds the table pointer
   mov rbx, cr2
   shr rbx, 39
   and rbx, 0x1FF
   add rbx, rax # rbx holds the entry pointer
   mov rax, [rbx] # load the PML4 entry
   # check there is an entry for the next level 
   test rax, 0x1
   jz 9f
   # zero out the 11:0 bits that are used for flags
   and rax, ~0xFFF # address of the page directory pointer table
   mov rbx, cr2
   shr rbx, 30
   and rbx, 0x1FF # index into the page directory pointer table
   add rbx, rax # page directory pointer table entry
   mov rax, [rbx]
   # check if it is a 1GB page or a direcotry table pointer
   test rax, 1 << 7
   jz 0f
   # handle 1GB page
   # TODO do copy handler instead 
   jmp 9f
0:
   # handle direcotry table pointer, zero out bits used for flags
   and rax, ~0xFFF # address of directory table
   mov rbx, cr2
   shr rbx, 18 # want bit 21 to be the new bit 3 (index into 8 byte datastructure, so last 3 bits are 0)
   and rbx, 0xFF8 # offset into directory table
   add rbx, rax # directory table entry
   mov rax, [rbx]
   # check if it is a 2MB page or a page table
   test rax, 1 << 7
   jz 0f
   # handle 2MB page 
   jmp 1f
0: 
   #handle page table
   jmp 9f
1: 
   # resovle fault: update page table, flush tlb, copy data
   # assumes:
   # - cr2 holds faulting address
   # - rax holds page base address 
   # - rbx holds the address of the table entry
   # - rcx holds the size of the page
   or rax, 0x2 # allow writing on the page
   mov [rbx], rax
   and rax, ~0xFFF
   invlpg [rax]
   # restore the registers we use during handler
   mov rax, [rsp - 8]
   mov rbx, [rsp - 16]
   mov rcx, [rsp - 24]
   mov rdx, [rsp - 32]
   # set the stack pointer to remove the error code and return from interrupt
   add rsp, 8 
   rex64 iretq
9: 
   # failure exit
   out 14, eax
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