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
    # load the ddc into a register so it can be used for a capability load
    mrs c0, DDC
    ldr c0, [c0]
    # jump using sealed pair at DDC[0]
overwriteAllEnd: ldpbr c29, [c0]


# safe all registers the function can read to the ddc with offset of 16 bytes,
# to not overwrite the return pair capability
.align 8
.text
  safeAll:
    clrtag c0, c0
    clrtag c1, c1
    clrtag c2, c2
    clrtag c3, c3
    clrtag c4, c4
    clrtag c5, c5
    clrtag c6, c6
    clrtag c7, c7
    clrtag c8, c8
    clrtag c9, c9
    clrtag c10, c10
    clrtag c11, c11
    clrtag c12, c12
    clrtag c13, c13
    clrtag c14, c14
    clrtag c15, c15
    clrtag c16, c16
    clrtag c17, c17
    clrtag c18, c18
    clrtag c19, c19
    clrtag c20, c20
    clrtag c21, c21
    clrtag c22, c22
    clrtag c23, c23
    clrtag c24, c24
    clrtag c25, c25
    clrtag c26, c26
    clrtag c27, c27
    clrtag c28, c28
    clrtag c29, c29
    clrtag c30, c30
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
    clrtag c0, c0
    clrtag c1, c1
    clrtag c2, c2
    clrtag c3, c3
    clrtag c4, c4
    str c0, [SP, 496]
    str c1, [SP, 512]
    str c2, [SP, 528]
    str c3, [SP, 544]
    str c4, [SP, 560]
    # load the ddc into a register so it can be used for a capability load
    mrs c0, DDC
    ldr c0, [c0]
    # jump using sealed pair at DDC[0]
safeAllEnd: ldpbr c29, [c0]

# takes arguments for sanboxed call in regs c0,c1,c2 and pointer where to safe
# the state in c3, this can then be copied to r19 which is a callee-saved
# register and thus can be used to store it.
# Additionally the return register c30 needs to be safed, for which we use c20.
# The registers c19 and c20 are thus expected to hold the same values after
# return from the sandboxed call and are restored by the wrapper.
sandboxedCallWrapped:
  str c0, [x7]
  str c1, [x7, 16]
  str c2, [x7, 32]
  str c3, [x7, 48]
  str c4, [x7, 64]
  str c5, [x7, 80]
  str c6, [x7, 96]
  str c7, [x7, 112]
  str c8, [x7, 128]
  str c9, [x7, 144]
  str c10, [x7, 160]
  str c11, [x7, 176]
  str c12, [x7, 192]
  str c13, [x7, 208]
  str c14, [x7, 224]
  str c15, [x7, 240]
  str c16, [x7, 256]
  str c17, [x7, 272]
  str c18, [x7, 288]
  str c19, [x7, 304]
  str c20, [x7, 320]
  str c21, [x7, 336]
  str c22, [x7, 352]
  str c23, [x7, 368]
  str c24, [x7, 384]
  str c25, [x7, 400]
  str c26, [x7, 416]
  str c27, [x7, 432]
  str c28, [x7, 448]
  str c29, [x7, 464]
  str c30, [x7, 480]
  cpy c5, CSP
  str c5, [x7, 496]
  mrs c5, DDC
  str c5, [x7, 512]
  mrs c5, CID_EL0
  str c5, [x7, 528]
  mrs c5, CTPIDR_EL0
  str c5, [x7, 544]
  mrs c5, RCTPIDR_EL0
  str c5, [x7, 560]
  ldr c5, [x7, 80]
  cpy c19, c7
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
