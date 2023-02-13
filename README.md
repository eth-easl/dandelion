# dandelion

This project is aiming to build a workernode for serverless computing with
minimal, secure isolation.

## Dependencies:

For testing we are using unity which is included directly in the project.

## Cheri setup:

First clone the Cheribuild repository onto a local disk.
It is important that it is on a local disk, as it needs to lock some files which
does not work with remote file systems.

```
git clone https://github.com/CTSRD-CHERI/cheribuild.git
```

Then go into the cheribuild folder and copy the cheribuild.json into it.
It contains configurations that make it easier to manage the output produced by
the build process and also manages the ssh-ports for the different builds.
Then build the sources with:
```
./cheribuild.py -d cheribsd-morello-purecap
```
This will generate the sources to make an image and install all dependencies.
If there are any errors consult the cheribuild git repo.

Then add your ssh keys to the configured source folder.
```
cd <cheri-source>/extra-files
mkdir root && cd root
mkdir .ssh && cd .ssh
cp <public-key-path> authorized_keys
```

Then you can build the disk image in the cheribsd folder:
```
./cheribuild.py disk-image-morello-purecap
```

And start it with either QEMU or FVP.
To run it for the first time add a -d at the end.
This will rebuild all dependencies (including the steps we already did)
If the image is already built and QEMU or FVP are installed separately then it
it can be executed right away.
```
./cheribuild.py run-morello-purecap
./cheribuild.py run-fvp-morello-purecap
```

To manually set the ssh-port to another port that the one set by the config
(55555) use `--run/ssh-forwarding-port=<portno>` or
`--run-fvp/ssh-port=<portno>` before the run command.

## Build
Before cross compilation can be used you need to fill in the respective paths
in the toolchain file.

If the toolchain still needs to be built, set up the cheribuild repository as
described above and build it with `./cheribuild sdk-morello-purecap -d`

After specifying them you can make a build folder, change in to it, and start
building with the toolchain file:
```
mkdir build
cd build
cmake -DCMAKE_TOOLCHAIN_FILE=../morello-toolchain.txt ..
make
```
For native builds the toolchain specification can be omitted.

## Kernel module build
The kernel modules needed are in the kernelModule folder.
They can be built with a normal make command, but have the following requirements:
- the /usr/src folder contains the OS source

### Known issues:
- make: "/usr/src/sys/conf/kmod.mk" line 549: is ZFSTOP set?
    - solution `export ZFSTOP=/usr/src/sys/contrib/openzfs`
- currently syscall seems not to work, but loading does, with cpuset -l <core> a specific core can be setup. repeat for each core on machine to set up entire machine.