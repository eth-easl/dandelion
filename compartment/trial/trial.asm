.global trialFunction

.align 8
.text
  trialFunction:
    mov x0, 0xCAFE
    mov x1, 0x7
    stur x0, [SP, -8]
    stur x1, [SP, -16]
    mrs c0, DDC
    ldr c0, [c0]
    ldpbr c29, [c0]
