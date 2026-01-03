There is a catch while running from Windows terminal. 
The DataLoader code uses POSIX APIs (pread, unistd.h, fsync, etc.). Those compile/run great on Linux (and macOS), but not directly with Microsoft’s MSVC on native Windows unless you adapt it.
So there are two ways to run this:
Option A (recommended): Run it in WSL from Windows Terminal (no code changes)

This is the simplest and most “systems-dev” friendly.

1) Install WSL (one-time)

Open Windows Terminal (PowerShell) and run:

wsl --install

Reboot if prompted.

2) Open Ubuntu (WSL) in Windows Terminal

From Windows Terminal, open a new tab and choose Ubuntu, or run:

wsl

3) Install compiler

Inside WSL:

sudo apt update
sudo apt install -y g++

4) Compile

Assuming dataloader_prefetch.cpp is in your Windows folder (like C:\Users\You\projects\), in WSL it’s here:
/mnt/c/Users/You/projects/

Example:

cd /mnt/c/Users/You/projects
g++ -O2 -std=c++20 -pthread dataloader_prefetch.cpp -o dataloader

5) Run (generate dataset)

./dataloader --generate data.bin --num-records 2000000 --record-bytes 512

6) Run (load/benchmark)

./dataloader --file data.bin --record-bytes 512 --batch-records 1024 --prefetch 16 --readers 4

Option B: Build natively on Windows (requires small code edits)

If you really want it to run in native Windows Terminal without WSL, the easiest route is MSYS2/MinGW plus a small Windows compatibility shim, because Windows uses _open, _pread, _commit, etc.

Quick setup (MSYS2)

Install MSYS2

Open MSYS2 UCRT64 terminal

Install compiler:

pacman -S --needed mingw-w64-ucrt-x86_64-gcc

You’ll need to modify the code

At minimum, you’ll map:

open → _open

pread → _pread

fsync → _commit

close → _close

add binary flag: _O_BINARY

