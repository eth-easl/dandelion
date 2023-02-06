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
	*(.rodata.*)
    } >ddc AT>pcc
    
    .eh_frame : 
    {
	*(.eh_frame)
    } >ddc AT>pcc

    .eh_frame_hdr :
    {
	*(.eh_frame_hdr)
    } >ddc AT>pcc

    .note :
    {
	*(.note.*)
    } >ddc AT>pcc

    .tdata :
    {
	*(.tdata)
    } >ddc AT>pcc

    .init_array :
    {
	*(.init_array)
	*(.init_array.*)
    } >ddc AT>pcc

    .fini_array :
    {
	*(.fini_array)
	*(.fini_array.*)
    } >ddc AT>pcc

    .got :
    {
	*(.got)
    } >ddc AT>pcc

    .jcr :
    {
	*(.jcr)
    } >ddc AT>pcc

    .data :
    {
        *(.data)
    } >ddc AT>pcc

    .bss :
    {
        *(.bss)
    } >ddc AT>pcc

    .tbss :
    {
	*(.tbss)
    } >ddc AT>pcc

}
