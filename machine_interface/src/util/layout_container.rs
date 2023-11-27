extern crate alloc;

pub struct LayoutForBox {
    layout: alloc::alloc::Layout,
    n: usize,
}

impl LayoutForBox {
    pub fn new<T>() -> Self {
        LayoutForBox { layout: alloc::alloc::Layout::new::<T>(), n: 1 }
    }
    
    pub fn array<T>(n: usize) -> Result<Self, alloc::alloc::LayoutError> {
        Ok(LayoutForBox { layout: alloc::alloc::Layout::array::<T>(n)?, n })
    }
}

pub struct LayoutBox {
    layout: LayoutForBox,
    ptr: *mut u8,
}

impl LayoutBox {
    pub fn new(layout: LayoutForBox) -> Self {
        // SAFETY: this is safe because `LayoutForBox` is guaranteed to be a valid type layout
        unsafe {
            LayoutBox {
                layout,
                ptr: alloc::alloc::alloc(layout.layout),
            }
        }
    }
    
    pub fn borrow<T>(&self) -> &T {
        assert!(self.layout.n == 1 && core::mem::size_of::<T>() == self.layout.layout.size()
            && core::mem::align_of::<T>() == self.layout.layout.align());
        
        unsafe {
            &*(self.ptr as *const T)
        }
    }

    pub fn borrow_slice<T>(&self) -> &[T] {
        assert!(core::mem::size_of::<T>() == (self.layout.layout.size() / self.layout.n)
            && core::mem::align_of::<T>() == self.layout.layout.align());
        
        unsafe {
            core::slice::from_raw_parts(self.ptr as *const T, self.layout.n)
        }
    }

    pub fn borrow_mut<T>(&self) -> &mut T {
        assert!(self.layout.n == 1 && core::mem::size_of::<T>() == self.layout.layout.size()
            && core::mem::align_of::<T>() == self.layout.layout.align());
        
        unsafe {
            &mut *(self.ptr as *mut T)
        }
    }

    pub fn borrow_mut_slice<T>(&self) -> &mut [T] {
        assert!(core::mem::size_of::<T>() == (self.layout.layout.size() / self.layout.n)
            && core::mem::align_of::<T>() == self.layout.layout.align());
        
        unsafe {
            core::slice::from_raw_parts_mut(self.ptr as *mut T, self.layout.n)
        }
    }
}

impl Drop for LayoutBox {
    fn drop(&mut self) {
        // SAFETY: this is safe because `self.layout.layout` is guaranteed to be a valid type layout
        unsafe {
            alloc::alloc::dealloc(self.ptr, self.layout.layout)
        }
    }
}
