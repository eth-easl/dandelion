MEMORY
{
	pcc (rx)  : ORIGIN = 0x00001000, LENGTH = 1024M
	ddc (!rx) : ORIGIN = 0x00001000, LENGTH = 1024M
}

SECTIONS
{
    .text :
    {
        *(.text)
    } >pcc AT>pcc

    .rodata :
    {
        *(.rodata)
    } >ddc AT>pcc
    
    .eh_frame : 
    {
	*(.eh_frame)
    } >ddc AT>pcc

    .eh_frame_hdr :
    {
	*(.eh_frame_hdr)
    } >ddc AT>pcc

    .data :
    {
        *(.data)
    } >ddc AT>pcc

    .bss :
    {
        *(.bss)
    } >ddc AT>pcc
}
