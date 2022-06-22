
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
    "ldr c0, [c0] \n"
    "ldpbr c0, [c0] \n"
    : : : "c0"
  );
}
