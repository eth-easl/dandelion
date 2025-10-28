# backup assembly to implement the page fault handler
# inside the vm
# push registers we use in the handler 
push rbp
mov rbp, rsp
and rsp, ~0x1F # 32 bit allign the stack pointer
mov [rsp - 8], rax
mov [rsp - 16], rbx
mov [rsp - 24], rcx
mov [rsp - 32], rdx
vmovdqa [rsp - 64], ymm0
vmovdqa [rsp - 96], ymm1
vmovdqa [rsp - 128], ymm2
vmovdqa [rsp - 160], ymm3
vmovdqa [rsp - 192], ymm4
vmovdqa [rsp - 224], ymm5
vmovdqa [rsp - 256], ymm6
vmovdqa [rsp - 288], ymm7 
# for debugging, enable debug interrupts
# push rbp
# pushf
# mov rbp, rsp
# or word ptr[rbp], 0x10100
# popf
# pop rbp 
# this handler assumes 4 level paging
# each linear address consists of the following
# 47 .. 39  | 38 .. 30        | 29 .. 21  | 20 .. 12  | 11 .. 0
# PML4      | Directory PTR   | Directory | Table     | Offset
# with the PML4 being the offset into the page table pointed to by the root pointer in CR3
# and then the others being indexes into the tables recursively
# The direcotry pointer table entries are either 1GB pages or directory table pointers
# Directory table entries are either 2MB pages or tables
# Table entries are always 4KB pages   
# cr2 holds the offending address, cr3 the root table address, [rsp] holds the error code 
# TODO: might want to do some check on the error code
# TODO: Make sure it is a error because of write permissions, not because of user permissions
# find the entry in the PML4 table by looking at bits 47..39 of the offending pointer
mov rax, cr3 # rax holds the table pointer
mov rbx, cr2
shr rbx, 36 # shift right so that 39 bit becomes new bit 3, as we have 8 byte datastructure
and rbx, 0xFF8 # offset into table, 8 bit structure, so last 3 bits need to be zero, at most 512 entries
add rbx, rax # rbx holds the entry pointer
mov rax, [rbx] # load the PML4 entry
# check there is an entry for the next level 
test rax, 0x1
jz 9f
# zero out the 11:0 bits that are used for flags
and rax, ~0xFFF # address of the page directory pointer table
mov rbx, cr2
shr rbx, 27
and rbx, 0xFF8 # index into the page directory pointer table
add rbx, rax # page directory pointer table entry
mov rax, [rbx]
# check if it is a 1GB page or a direcotry table pointer
test rax, 1 << 7
# should not have 1GB cow pages so go to error exit
jnz 9f
# handle direcotry table, zero out bits used for flags
and rax, ~0xFFF # address of directory table
mov rbx, cr2
shr rbx, 18 # want bit 21 to be the new bit 3 (index into 8 byte datastructure, so last 3 bits are 0)
and rbx, 0xFF8 # offset into directory table
add rbx, rax # directory table entry
mov rax, [rbx]
# check if it is a 2MB page or a page table
test rax, 1 << 7
# should not have 2MB cow pages so go to error exit
jnz 9f
# handle page table
and rax, ~0xFFF # address of page table
mov rbx, cr2
shr rbx, 9 # want the 12 bit to be the new bit 3
and rbx, 0xFF8 # offset into directory table
add rbx, rax 
mov rax, [rbx] # load page table entry physical page address
test rax, 1 << 7
jnz 9f # if the page is not a real page, jump to error state
# page of the faulting address is destination for copy, current physical is source
# the current physical should also be mapped at the same virtual
# first update the entry to point to the copy destination and then copy
# assumed state:
# - cr2 holds faulting address
# - rax holds physical page base address and flags
# - rbx holds the address of the table entry
# first update the table entry, by creating entry from faulting address
mov rcx, cr2
and rcx, ~0xFFF # get page base address
# get flags from previous entry
and rax, 0xFFF
or rax, 0x2 # allow writing on the page
or rcx, rax # move flags over to new entry
mov rax, [rbx] # reload old address into rax
mov [rbx], rcx # store new address into table
invlpg [rcx] # invalidate tlb entry
# remove flags from both
and rax, ~0xFFF
and rcx, ~0xFFF
# start copying
vmovdqa ymm0, [rax] 
vmovdqa [rcx], ymm0
vmovdqa ymm1, [rax + 32] 
vmovdqa [rcx + 32], ymm1
vmovdqa ymm2, [rax + 64] 
vmovdqa [rcx + 64], ymm2
vmovdqa ymm3, [rax + 96] 
vmovdqa [rcx + 96], ymm3
vmovdqa ymm4, [rax + 128] 
vmovdqa [rcx + 128], ymm4
vmovdqa ymm5, [rax + 160] 
vmovdqa [rcx + 160], ymm5
vmovdqa ymm6, [rax + 192] 
vmovdqa [rcx + 192], ymm6
vmovdqa ymm7, [rax + 224] 
vmovdqa [rcx + 224], ymm7
vmovdqa ymm0, [rax + 256] 
vmovdqa [rcx + 256], ymm0
vmovdqa ymm1, [rax + 288] 
vmovdqa [rcx + 288], ymm1
vmovdqa ymm2, [rax + 320] 
vmovdqa [rcx + 320], ymm2
vmovdqa ymm3, [rax + 352] 
vmovdqa [rcx + 352], ymm3
vmovdqa ymm4, [rax + 384] 
vmovdqa [rcx + 384], ymm4
vmovdqa ymm5, [rax + 416] 
vmovdqa [rcx + 416], ymm5
vmovdqa ymm6, [rax + 448] 
vmovdqa [rcx + 448], ymm6
vmovdqa ymm7, [rax + 480] 
vmovdqa [rcx + 480], ymm7
# restore the registers we use during handler
mov rax, [rsp - 8]
mov rbx, [rsp - 16]
mov rcx, [rsp - 24]
mov rdx, [rsp - 32]
vmovdqa ymm0, [rsp - 64]
vmovdqa ymm1, [rsp - 96]
vmovdqa ymm2, [rsp - 128]
vmovdqa ymm3, [rsp - 160]
vmovdqa ymm4, [rsp - 192]
vmovdqa ymm5, [rsp - 224]
vmovdqa ymm6, [rsp - 256]
    vmovdqa ymm7, [rsp - 288]
    # set the stack pointer to remove the error code and return from interrupt
    mov rsp, rbp
    pop rbp
    add rsp, 8 # pop the interrupt handler argument (needs to be done manually, as not all handlers have one)
    rex64 iretq
9: 
    # failure exit
    out 14, eax