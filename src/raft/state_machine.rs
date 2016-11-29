#[macro_export]
macro_rules! state_machine {
//TODO: Use higher order macro to merge with rpc service! macro when possible to do this in Rust.
//Current major problem is repeated macro will be recognized as outer macro which breaks expand
    (
        $(
            $(#[$attr:meta])*
            $fnt: ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) $(-> $out:ty)* $(| $error:ty)*;
        )*
    ) => {
        state_machine! {{
            $(
                $(#[$attr])*
                $fnt $fn_name( $( $arg : $in_ ),* ) $(-> $out)* $(| $error)*;
            )*
        }}
    };
    (
        {
            $(#[$attr:meta])*
            $fnt: ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ); // No return, no error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            $fnt $fn_name( $( $arg : $in_ ),* ) -> () | ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            $fnt: ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty; //return, no error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            $fnt $fn_name( $( $arg : $in_ ),* ) -> $out | ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            $fnt: ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) | $error:ty; //no return, error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            $fnt $fn_name( $( $arg : $in_ ),* ) -> () | $error;
        }
    };
    (
        {
            $(#[$attr:meta])*
            $fnt: ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty; //return, error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            $fnt $fn_name( $( $arg : $in_ ),* ) -> $out | $error;
        }
    };
    (
        {} // all expanded
        $(
            $(#[$attr:meta])*
            $fnt: ident $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty;
        )*
    ) => {
        state_machine! {
            uncatgorized {
                $(
                    $(#[$attr])*
                    $fnt $fn_name( $( $arg : $in_ ),* ) $(-> $out)* $(| $error)*;
                )*
            }
            commands {

            }
            queries {

            }
        }
    };
    ( //expand command
        uncatgorized {
            $(#[$attr:meta])*
            command $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty;
            $( $unexpanded:tt )*
        }
        commands {
            $( $expanded_cmds:tt )*
        }
        queries {
            $( $expanded_queries:tt )*
        }
    ) => {
        state_machine! {
            uncatgorized {
                $( $unexpanded )*
            }
            commands {
                $( $expanded_cmds )*
                $(#[$attr])*
                command $fn_name ( $( $arg : $in_ ),* ) -> $out | $error;
            }
            queries {
                $( $expanded_queries )*
            }
        }
    };
    ( //expand query
        uncatgorized {
            $(#[$attr:meta])*
            query $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty;
            $( $unexpanded:tt )*
        }
        commands {
            $( $expanded_cmds:tt )*
        }
        queries {
            $( $expanded_queries:tt )*
        }
    ) => {
        state_machine! {
            uncatgorized {
                $( $unexpanded )*
            }
            commands {
                $( $expanded_cmds )*
            }
            queries {
                $( $expanded_queries )*
                $(#[$attr])*
                $fn_name ( $( $arg : $in_ ),* ) -> $out | $error;
            }
        }
    };
    (
        uncatgorized {} // all types expanded
        commands {
            $( $expanded_cmds:tt )*
        }
        queries {
            $( $expanded_queries:tt )*
        }
    ) => {
        state_machine! {
            command {
                $( $expanded_cmds )*
            }
        }
        state_machine! {
            queries {
                $( $expanded_queries )*
            }
        }
    };
    (
        commands {
            $( $expanded_cmds:tt )*
        }
    ) => {

    };
}