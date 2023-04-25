
// struct to hold all register state
typedef struct {
  void* __capability c0;
  void* __capability c1;
  void* __capability c2;
  void* __capability c3;
  void* __capability c4;
  void* __capability c5;
  void* __capability c6;
  void* __capability c7;
  void* __capability c8;
  void* __capability c9;
  void* __capability c10;
  void* __capability c11;
  void* __capability c12;
  void* __capability c13;
  void* __capability c14;
  void* __capability c15;
  void* __capability c16;
  void* __capability c17;
  void* __capability c18;
  void* __capability c19;
  void* __capability c20;
  void* __capability c21;
  void* __capability c22;
  void* __capability c23;
  void* __capability c24;
  void* __capability c25;
  void* __capability c26;
  void* __capability c27;
  void* __capability c28;
  void* __capability c29;
  void* __capability c30;
  void* __capability csp;
  void* __capability ddc;
  void* __capability cid;
  void* __capability ctpidr;
  void* __capability rctpidr;
} RegisterState;

typedef struct {
  RegisterState expected;
  RegisterState actual;
} StatePair;
