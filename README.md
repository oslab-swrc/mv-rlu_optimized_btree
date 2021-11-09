# mv-rlu_optimized_btree

This repository provide the algorithm of btree to which MV-RLU is applied.

It is provided under the terms of Apache 2.0 License

## How to use

- Fetch the MV-RLU from github and build
```
$> git clone https://github.com/cosmoss-vt/mv-rlu.git
$> cd mv-rlu
$/mv-rlu> make ordo
$/mv-rlu> make
```
- Clone this repository and check the file for the btrr: 'BTreeTS.h'
```
$> git clone <this repo> 
```

- You should modify the include path of MV-RLU in the header file before compiling.
```
$> vi BTeeTS.h
#include "{PATH}/mvrlu.h"
```
- Put the files in your application, and including it before using it with MV-RLU
```
#include "BTreeTS.h"
```
- You can handle ART structure through following APIs
```
isFull()
insert()
split()
makeRoot()
yield()
lookup()
```
