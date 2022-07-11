.global overwriteAll

# overwrite all normal and status registers
# can't overwrite ddc and register 30 as they are needed to return.
.align 8
.text
  overwriteAll:
    mov x0, 0xCA31
    mov CSP, c0
    mov x0, 0xAB32
    msr CID_EL0, c0
    mov x0, 0xAB33
    msr CTPIDR_EL0, c0
    mov x0, 0xAB34
    msr RCTPIDR_EL0, c0
    mov x0, 0xCA00
    mov x1, 0xCA01
    mov x2, 0xCA02
    mov x3, 0xCA03
    mov x4, 0xCA04
    mov x5, 0xCA05
    mov x6, 0xCA06
    mov x7, 0xCA07
    mov x8, 0xCA08
    mov x9, 0xCA09
    mov x10, 0xCA10
    mov x11, 0xCA11
    mov x12, 0xCA12
    mov x13, 0xCA13
    mov x14, 0xCA14
    mov x15, 0xCA15
    mov x16, 0xCA16
    mov x17, 0xCA17
    mov x18, 0xCA18
    mov x19, 0xCA19
    mov x20, 0xCA20
    mov x21, 0xCA21
    mov x22, 0xCA22
    mov x23, 0xCA23
    mov x24, 0xCA24
    mov x25, 0xCA25
    mov x26, 0xCA26
    mov x27, 0xCA27
    mov x28, 0xCA28
    mov x29, 0xCA29
    ret
