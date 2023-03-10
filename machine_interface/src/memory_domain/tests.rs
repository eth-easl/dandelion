use std::vec;

use super::{ControllerError, MemoryDomainController};
// produces binary pattern 0b0101_01010 or 0x55
const BYTEPATTERN: u8 = 85;

macro_rules! controllerReadWriteTests {
    ($controller:ident ; $init : expr) => {
        // TODO create make distinguishable
        use super::malloc::$controller;
        #[test]
        fn write_single_oob_offset() {
            let start_result = $controller::start_domain_controller($init);
            let controller = start_result.expect("controller should have been started");
            let mut domain = controller
                .alloc_memory_subdomain(1)
                .expect("Single byte domain should always be allocatable");
            assert_eq!(
                Err(ControllerError::InvalidWrite),
                domain.write(1, vec![BYTEPATTERN])
            );
        }
        #[test]
        fn write_single_oob_size() {
            let start_result = $controller::start_domain_controller($init);
            let controller = start_result.expect("controller should have been started");
            let mut domain = controller
                .alloc_memory_subdomain(1)
                .expect("Single byte domain should always be allocatable");
            assert_eq!(
                Err(ControllerError::InvalidWrite),
                domain.write(0, vec![BYTEPATTERN, BYTEPATTERN])
            );
        }
        #[test]
        fn read_single_success() {
            let start_result = $controller::start_domain_controller($init);
            let controller = start_result.expect("controller should have been started");
            let mut domain = controller
                .alloc_memory_subdomain(1)
                .expect("Single byte domain should always be allocatable");
            domain
                .write(0, vec![BYTEPATTERN])
                .expect("Writing should succeed");
            let return_val = domain
                .read(0, 1, false)
                .expect("Domain should return single value vector in range");
            assert_eq!(vec![BYTEPATTERN], return_val);
        }
        #[test]
        fn read_single_oob_offset() {
            let start_result = $controller::start_domain_controller($init);
            let controller = start_result.expect("controller should have been started");
            let mut domain = controller
                .alloc_memory_subdomain(1)
                .expect("Single byte domain should always be allocatable");
            domain
                .write(0, vec![BYTEPATTERN])
                .expect("Writing should succeed");
            assert_eq!(Err(ControllerError::InvalidRead), domain.read(1, 1, false));
        }
        #[test]
        fn read_single_oob_size() {
            let start_result = $controller::start_domain_controller($init);
            let controller = start_result.expect("controller should have been started");
            let mut domain = controller
                .alloc_memory_subdomain(1)
                .expect("Single byte domain should always be allocatable");
            domain
                .write(0, vec![BYTEPATTERN])
                .expect("Writing should succeed");
            assert_eq!(Err(ControllerError::InvalidRead), domain.read(0, 2, false));
        }
        #[test]
        fn read_single_sanitize() {
            let start_result = $controller::start_domain_controller($init);
            let controller = start_result.expect("controller should have been started");
            let mut domain = controller
                .alloc_memory_subdomain(1)
                .expect("Single byte domain should always be allocatable");
            domain
                .write(0, vec![BYTEPATTERN])
                .expect("Writing should succeed");
            let return_val = domain
                .read(0, 1, true)
                .expect("Domain should return single value vector in range");
            assert_eq!(vec![BYTEPATTERN], return_val);
            let sanitized_val = domain
                .read(0, 1, false)
                .expect("Domain should return single value vector in range");
            assert_eq!(vec![0], sanitized_val);
        }
        // TODO longer writes and reads, as well as partial writes and reads
    };
}

controllerReadWriteTests!(MallocMemoryController; Vec::new());
