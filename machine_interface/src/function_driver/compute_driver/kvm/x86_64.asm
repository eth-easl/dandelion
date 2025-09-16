.global page_fault_handler
.global page_fault_handler_end
.global asm_entry

   rex64 ljmp [0x5100]
   mov eax, 1
   retfq
asm_entry:
   int 49
   mov BYTE PTR [-1], 0x1
   mov rax, 0x5028
   push rax
   mov ax, (50*16)-1
   push ax
   lidt [rsp]
   mov rbx, 0x5028
   add rbx, 49*16
   mov rax, 0x4000
   mov [rbx], ax
   mov WORD PTR [rbx + 2], 0x20
   mov WORD PTR [rbx + 4], 0x8e00
   shr rax, 16
   mov [rbx + 6], ax
   shr rax, 16
   mov [rbx + 8], rax
   mov [0x0], rax
   int 49
   push 0x8
   mov rax, [0x5100]
   push rax
   mov eax, 2
   mov eax, 3
   mov eax, 4
page_fault_handler:
   mov eax, 16
   out 0x1, eax
page_fault_handler_end:
   hlt

# dont forget to clear page cache after changing the entry