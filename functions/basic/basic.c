
void noOperation(){
  return;
}

int returnSeven(){
  return 7;
}

int returnNumber(int number){
  return number;
}

int addOne(int number){
  return number+1;
}

void preWrapped(void){
  // return using the load pair at the bottom of the ddc
  // puts the contex capability in c0
  __asm__ volatile(
    "mrs c0, ddc \n"
    // load add 1 and store on the value after the return cap to check that this
    // code executed
    "ldr w1, [c0, #16] \n"
    "add w1, w1, #1 \n"
    "str w1, [c0, #16] \n"
    // return to cap at bottom of ddc
    "ldr c0, [c0] \n"
    "ldpbr c29, [c0] \n"
    : : : "c0", "w1"
  );
}
