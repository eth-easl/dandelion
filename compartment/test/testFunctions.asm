.global overwriteAll
.global overwriteAllEnd
.global safeAll
.global safeAllEnd

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
overwriteAllEnd: ret

# safe all registers the function can read to the ddc with offset of 16 bytes,
# to not overwrite the return pair capability
.align 8
.text
  safeAll:
    str c0, [SP]
    str c1, [SP, 16]
    str c2, [SP, 32]
    str c3, [SP, 48]
    str c4, [SP, 64]
    str c5, [SP, 80]
    str c6, [SP, 96]
    str c7, [SP, 112]
    str c8, [SP, 128]
    str c9, [SP, 144]
    str c10, [SP, 160]
    str c11, [SP, 176]
    str c12, [SP, 192]
    str c13, [SP, 208]
    str c14, [SP, 224]
    str c15, [SP, 240]
    str c16, [SP, 256]
    str c17, [SP, 272]
    str c18, [SP, 288]
    str c19, [SP, 304]
    str c20, [SP, 320]
    str c21, [SP, 336]
    str c22, [SP, 352]
    str c23, [SP, 368]
    str c24, [SP, 384]
    str c25, [SP, 400]
    str c26, [SP, 416]
    str c27, [SP, 432]
    str c28, [SP, 448]
    str c29, [SP, 464]
    str c30, [SP, 480]
    cpy c0, CSP
    mrs c1, DDC
    mrs c2, CID_EL0
    mrs c3, CTPIDR_EL0
    mrs c4, RCTPIDR_EL0
    str c0, [SP, 496]
    str c1, [SP, 512]
    str c2, [SP, 528]
    str c3, [SP, 544]
    str c4, [SP, 560]
safeAllEnd:  ret
