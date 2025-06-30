// Minimal crunchy implementation for nexus-cli
// This is a stub implementation to satisfy the dependency requirement

#[macro_export]
macro_rules! unroll {
    (for $v:ident in 0..$n:tt $body:block) => {
        { let $v: usize = 0; $body }
        { let $v: usize = 1 % $n; if 1 < $n $body }
        { let $v: usize = 2 % $n; if 2 < $n $body }
        { let $v: usize = 3 % $n; if 3 < $n $body }
        { let $v: usize = 4 % $n; if 4 < $n $body }
        { let $v: usize = 5 % $n; if 5 < $n $body }
        { let $v: usize = 6 % $n; if 6 < $n $body }
        { let $v: usize = 7 % $n; if 7 < $n $body }
        { let $v: usize = 8 % $n; if 8 < $n $body }
        { let $v: usize = 9 % $n; if 9 < $n $body }
        { let $v: usize = 10 % $n; if 10 < $n $body }
        { let $v: usize = 11 % $n; if 11 < $n $body }
        { let $v: usize = 12 % $n; if 12 < $n $body }
        { let $v: usize = 13 % $n; if 13 < $n $body }
        { let $v: usize = 14 % $n; if 14 < $n $body }
        { let $v: usize = 15 % $n; if 15 < $n $body }
        { let $v: usize = 16 % $n; if 16 < $n $body }
        { let $v: usize = 17 % $n; if 17 < $n $body }
        { let $v: usize = 18 % $n; if 18 < $n $body }
        { let $v: usize = 19 % $n; if 19 < $n $body }
        { let $v: usize = 20 % $n; if 20 < $n $body }
        { let $v: usize = 21 % $n; if 21 < $n $body }
        { let $v: usize = 22 % $n; if 22 < $n $body }
        { let $v: usize = 23 % $n; if 23 < $n $body }
        { let $v: usize = 24 % $n; if 24 < $n $body }
        { let $v: usize = 25 % $n; if 25 < $n $body }
        { let $v: usize = 26 % $n; if 26 < $n $body }
        { let $v: usize = 27 % $n; if 27 < $n $body }
        { let $v: usize = 28 % $n; if 28 < $n $body }
        { let $v: usize = 29 % $n; if 29 < $n $body }
        { let $v: usize = 30 % $n; if 30 < $n $body }
        { let $v: usize = 31 % $n; if 31 < $n $body }
    };
} 