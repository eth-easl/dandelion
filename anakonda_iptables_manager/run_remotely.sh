#!/bin/bash

cargo build && RUST_LOG=debug sudo ./target/debug/anakonda_iptables_manager