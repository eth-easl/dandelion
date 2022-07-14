.global overwriteAll
.global overwriteAllEnd
.global safeAll
.global safeAllEnd
.global sandboxedCallWrapped

.extern sandboxedCall

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

# takes arguments for sanboxed call in regs c0,c1,c2 and pointer where to safe
# the state in c3, this can then be copied to r19 which is a callee-saved
# register and thus can be used to store it.
# Additionally the return register c30 needs to be safed, for which we use c20.
# The registers c19 and c20 are thus expected to hold the same values after
# return from the sandboxed call and are restored by the wrapper.
sandboxedCallWrapped:
  str c0, [x3]
  str c1, [x3, 16]
  str c2, [x3, 32]
  str c3, [x3, 48]
  str c4, [x3, 64]
  str c5, [x3, 80]
  str c6, [x3, 96]
  str c7, [x3, 112]
  str c8, [x3, 128]
  str c9, [x3, 144]
  str c10, [x3, 160]
  str c11, [x3, 176]
  str c12, [x3, 192]
  str c13, [x3, 208]
  str c14, [x3, 224]
  str c15, [x3, 240]
  str c16, [x3, 256]
  str c17, [x3, 272]
  str c18, [x3, 288]
  str c19, [x3, 304]
  str c20, [x3, 320]
  str c21, [x3, 336]
  str c22, [x3, 352]
  str c23, [x3, 368]
  str c24, [x3, 384]
  str c25, [x3, 400]
  str c26, [x3, 416]
  str c27, [x3, 432]
  str c28, [x3, 448]
  str c29, [x3, 464]
  str c30, [x3, 480]
  cpy c5, CSP
  str c5, [x3, 496]
  mrs c5, DDC
  str c5, [x3, 512]
  mrs c5, CID_EL0
  str c5, [x3, 528]
  mrs c5, CTPIDR_EL0
  str c5, [x3, 544]
  mrs c5, RCTPIDR_EL0
  str c5, [x3, 560]
  ldr c5, [x3, 80]
  cpy c19, c3
  cpy c20, c30
  bl sandboxedCall
  str c0, [x19, 576]
  str c1, [x19, 592]
  str c2, [x19, 608]
  str c3, [x19, 624]
  str c4, [x19, 640]
  str c5, [x19, 656]
  str c6, [x19, 672]
  str c7, [x19, 688]
  str c8, [x19, 704]
  str c9, [x19, 720]
  str c10, [x19, 736]
  str c11, [x19, 752]
  str c12, [x19, 768]
  str c13, [x19, 784]
  str c14, [x19, 800]
  str c15, [x19, 816]
  str c16, [x19, 832]
  str c17, [x19, 848]
  str c18, [x19, 864]
  str c19, [x19, 880]
  str c20, [x19, 896]
  str c21, [x19, 912]
  str c22, [x19, 928]
  str c23, [x19, 944]
  str c24, [x19, 960]
  str c25, [x19, 976]
  str c26, [x19, 992]
  str c27, [x19, 1008]
  str c28, [x19, 1024]
  str c29, [x19, 1040]
  str c30, [x19, 1056]
  cpy c0, CSP
  mrs c1, DDC
  mrs c2, CID_EL0
  mrs c3, CTPIDR_EL0
  mrs c4, RCTPIDR_EL0
  str c0, [x19, 1072]
  str c1, [x19, 1088]
  str c2, [x19, 1104]
  str c3, [x19, 1120]
  str c4, [x19, 1136]
  ldr c0, [x19, 576]
  ldr c1, [x19, 592]
  ldr c2, [x19, 608]
  ldr c3, [x19, 624]
  ldr c4, [x19, 640]
  cpy c30, c20
  ldr c20, [x19, 320]
  ldr c19, [x19, 304]
  ret
