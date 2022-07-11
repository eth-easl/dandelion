.global wrapperCode

.align 8
.text
  wrapperCode:
    # branch to the user code which should be written over the return
    bl returnLabel
    # load the ddc into a register so it can be used for a capability load
    mrs c0, DDC
    ldr c0, [c0]
    # jump using sealed pair at DDC[0]
    ldpbr c29, [c0]
    # safety return in case no code was written over the end of the wrapper
  returnLabel:
    ret
